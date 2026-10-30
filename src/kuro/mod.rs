//! Kuro Games (Wuthering Waves).
//!
//! A Kuro release is one JSON index: a list of `{dest, md5, size}` files served from a base URL.
//! There are no chunks to reuse, so the unit of work is the file, and the rule is the same one the
//! Sophon engine follows for chunks: look at what is on disk, keep what matches, fetch the rest.
//!
//! * install, update, resume and repair are one operation. Repair only differs in trusting
//!   nothing (every file is hashed);
//! * files are downloaded into `downloading/<dest>` (resumable `.part` + MD5 check, see
//!   [`crate::download`]) and moved into place only once complete, so a file in the game folder is
//!   never half written and an interrupted update leaves the old, working file alone;
//! * `.berry-kuro/index.json` records what the last successful run installed: unchanged files are
//!   accepted without hashing (size must still match), and only files listed there can ever be
//!   pruned, so saves and settings that no index mentions are never touched;
//! * `downloading/journal-kuro` remembers files verified during an interrupted run, so resuming a
//!   130 GB update does not hash everything again;
//! * any file that cannot be fetched makes the operation fail (`Incomplete`); nothing is ever
//!   reported as finished with holes;
//! * big files (the multi-gigabyte paks) are fetched in parallel segments over several connections
//!   and streamed to disk, see [`segments`]; small files travel one connection each.

mod segments;

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use md5::{Digest, Md5};
use serde::Deserialize;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::download::{DownloadSpec, download_file};
use crate::error::{InstallError, IoContext, Result};
use crate::fsutil::atomic_write;
use crate::hash::{Checksum, hex};
use crate::http::Http;
use crate::progress::{CancelToken, JobContext, Phase, Progress, SpeedMeter};
use crate::sophon::engine::Journal;
use crate::sophon::manifest::safe_relative;

pub const STORED_DIR: &str = ".berry-kuro";
const STORED_INDEX: &str = "index.json";
const WORK_DIR: &str = "downloading";
const JOURNAL_SLOT: &str = "kuro";
/// Files handled at once. Big ones fan out into segments (see [`segments`]); small ones are
/// latency bound, so more of them are kept in flight than there are big transfers.
const FILES_IN_FLIGHT: usize = 12;
const MAX_FILE_FAILURES: usize = 4;
const MAX_INDEX_BYTES: u64 = 256 * 1024 * 1024;

/// Where a release lives: `<base_url>/<dest>` is a file's URL.
#[derive(Debug, Clone)]
pub struct KuroJob {
    pub index_url: String,
    pub base_url: String,
}

#[derive(Debug, Deserialize)]
struct RawIndex {
    #[serde(default)]
    resource: Vec<RawResource>,
    #[serde(default, alias = "deleteFiles")]
    delete_files: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawResource {
    dest: String,
    md5: String,
    size: u64,
    #[serde(default, rename = "chunkInfos")]
    chunk_infos: Option<Vec<RawChunk>>,
}

#[derive(Debug, Deserialize)]
struct RawChunk {
    start: u64,
    end: u64,
    md5: String,
}

/// A byte range of a file with the md5 of exactly those bytes.
#[derive(Debug, Clone)]
struct Region {
    start: u64,
    len: u64,
    md5: String,
}

/// The largest region read into memory at once; bigger ones are not worth the risk.
const MAX_REGION: u64 = 128 * 1024 * 1024;
/// Range requests in flight across all files.
const REGIONS_IN_FLIGHT: usize = 12;
const REGION_ATTEMPTS: u32 = 4;
/// Whole-file md5 passes allowed at once. The multi-gigabyte paks finish downloading at about
/// the same time; hashing a dozen of them in parallel only makes the disk seek (on a hard drive
/// it is many times slower than doing them a few at a time) and leaves the UI with nothing to
/// show for minutes.
const HASHES_IN_FLIGHT: usize = 3;

/// Turn the index's `chunkInfos` into regions, or nothing when they cannot be trusted.
///
/// The index does not say whether `end` is inclusive. The two readings tile a file differently
/// (`next.start == end` vs `next.start == end + 1`), so the data itself decides; anything that
/// does not tile the whole file exactly under one of them is ignored and the file is handled as
/// a unit. Whatever is chosen, every region is still verified by md5 before it is trusted.
fn tile_regions(size: u64, chunks: &[RawChunk]) -> Vec<Region> {
    if chunks.len() < 2 || size == 0 {
        return Vec::new();
    }
    let mut sorted: Vec<&RawChunk> = chunks.iter().collect();
    sorted.sort_by_key(|c| c.start);
    for inclusive in [false, true] {
        let extra = u64::from(inclusive);
        let mut regions = Vec::with_capacity(sorted.len());
        let mut next = 0u64;
        let mut ok = true;
        for chunk in &sorted {
            let len = chunk.end.saturating_sub(chunk.start).saturating_add(extra);
            if chunk.start != next || len == 0 || len > MAX_REGION {
                ok = false;
                break;
            }
            let Ok(Checksum::Md5(md5)) = Checksum::md5(&chunk.md5) else {
                ok = false;
                break;
            };
            regions.push(Region {
                start: chunk.start,
                len,
                md5,
            });
            next = chunk.start + len;
        }
        if ok && next == size {
            return regions;
        }
    }
    Vec::new()
}

#[derive(Debug, Clone)]
struct Entry {
    name: String,
    path: PathBuf,
    md5: String,
    size: u64,
    /// Empty when the index gives no usable ranges.
    regions: Vec<Region>,
}

#[derive(Debug)]
struct Index {
    /// md5 of the index text: names the release in the journal.
    id: String,
    files: Vec<Entry>,
    delete: Vec<PathBuf>,
    raw: Vec<u8>,
}

impl Index {
    fn parse(raw: Vec<u8>) -> Result<Self> {
        let parsed: RawIndex = serde_json::from_slice(&raw)
            .map_err(|e| InstallError::InvalidData(format!("Kuro index is not valid JSON: {e}")))?;
        if parsed.resource.is_empty() {
            return Err(InstallError::InvalidData(
                "Kuro index lists no files".into(),
            ));
        }
        let mut files = Vec::with_capacity(parsed.resource.len());
        for resource in parsed.resource {
            let path = safe_relative(&resource.dest)?;
            // The name ends up in a URL: no query or fragment tricks.
            if resource.dest.contains(['?', '#', '\\']) {
                return Err(InstallError::InvalidData(format!(
                    "unsafe file name in Kuro index: {:?}",
                    resource.dest
                )));
            }
            let Checksum::Md5(md5) = Checksum::md5(&resource.md5)? else {
                unreachable!("Checksum::md5 builds an Md5")
            };
            let regions = resource
                .chunk_infos
                .as_deref()
                .map(|chunks| tile_regions(resource.size, chunks))
                .unwrap_or_default();
            files.push(Entry {
                name: resource.dest,
                path,
                md5,
                size: resource.size,
                regions,
            });
        }
        let delete = parsed
            .delete_files
            .iter()
            .filter_map(|name| safe_relative(name).ok())
            .collect();
        Ok(Self {
            id: hex(&Md5::digest(&raw)),
            files,
            delete,
            raw,
        })
    }

    fn total(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    fn load_stored(game_dir: &Path) -> Option<Self> {
        Self::parse(fs::read(game_dir.join(STORED_DIR).join(STORED_INDEX)).ok()?).ok()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Install,
    Repair,
    Plan,
}

impl Action {
    fn writes_game(self) -> bool {
        self != Self::Plan
    }

    fn name(self) -> &'static str {
        match self {
            Self::Install => "install",
            Self::Repair => "repair",
            Self::Plan => "plan",
        }
    }
}

/// What an operation found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Outcome {
    pub files: u64,
    /// Accepted without hashing (identical to the stored index, or verified earlier this run).
    pub files_trusted: u64,
    /// Already correct on disk (hashed and matched).
    pub files_verified: u64,
    pub files_fetched: u64,
    /// Files brought up to date by fetching only the byte ranges that differed.
    pub files_patched: u64,
    pub regions_reused: u64,
    pub regions_fetched: u64,
    pub bytes_total: u64,
    /// Size of the files that are missing or different.
    pub bytes_missing: u64,
    pub bytes_fetched: u64,
    pub files_pruned: u64,
}

#[derive(Debug, Clone)]
pub struct KuroInstaller {
    http: Http,
}

impl KuroInstaller {
    pub fn new(http: Http) -> Self {
        Self { http }
    }

    pub async fn install(
        &self,
        game_dir: &Path,
        job: &KuroJob,
        ctx: &JobContext,
    ) -> Result<Outcome> {
        self.run(Action::Install, game_dir, job, ctx).await
    }

    /// Like `install` but trusts nothing: every file is hashed.
    pub async fn repair(
        &self,
        game_dir: &Path,
        job: &KuroJob,
        ctx: &JobContext,
    ) -> Result<Outcome> {
        self.run(Action::Repair, game_dir, job, ctx).await
    }

    /// Read-only: what would an install download? (Hashes files, writes nothing.)
    pub async fn plan(&self, game_dir: &Path, job: &KuroJob, ctx: &JobContext) -> Result<Outcome> {
        self.run(Action::Plan, game_dir, job, ctx).await
    }

    async fn run(
        &self,
        action: Action,
        game_dir: &Path,
        job: &KuroJob,
        ctx: &JobContext,
    ) -> Result<Outcome> {
        ctx.cancel.check()?;
        ctx.reporter
            .report_now(Progress::new(Phase::Resolving, 0, 0));
        let index = fetch_index(&self.http, &job.index_url, ctx).await?;
        tracing::info!(
            "[kuro] {} game: {} files, {} to describe, index {}",
            action.name(),
            index.files.len(),
            crate::error::format_bytes(index.total()),
            index.id
        );
        let run = Arc::new(Run::new(
            action,
            self.http.clone(),
            game_dir,
            job,
            index,
            ctx.clone(),
        )?);
        run.execute().await
    }
}

async fn fetch_index(http: &Http, url: &str, ctx: &JobContext) -> Result<Index> {
    let mut last = None;
    for _ in 0..2 {
        let raw = http.get_bytes(url, MAX_INDEX_BYTES, &ctx.cancel).await?;
        match Index::parse(raw) {
            Ok(index) => return Ok(index),
            Err(error) => last = Some(error),
        }
    }
    Err(last.expect("two attempts were made"))
}

struct Run {
    action: Action,
    http: Http,
    game_dir: PathBuf,
    base_url: String,
    index: Index,
    /// name -> (md5, size) of the previous successful run.
    stored: Option<HashMap<String, (String, u64)>>,
    journal: Option<Journal>,
    ctx: JobContext,

    /// Bytes accounted for: right on disk, or downloaded.
    done: AtomicU64,
    net: AtomicU64,
    fetching: AtomicUsize,
    stop: AtomicBool,
    failures: AtomicUsize,
    first_failure: Mutex<Option<String>>,
    fatal: Mutex<Option<InstallError>>,

    files_trusted: AtomicU64,
    files_verified: AtomicU64,
    files_fetched: AtomicU64,
    files_patched: AtomicU64,
    regions_reused: AtomicU64,
    regions_fetched: AtomicU64,
    /// Cleared when the server turns out to ignore `Range`: later files go straight to whole-file.
    ranges_ok: AtomicBool,
    region_sem: Arc<Semaphore>,
    /// Connections used by segmented downloads of big files, across all files.
    segment_sem: Arc<Semaphore>,
    bytes_missing: AtomicU64,
    bytes_fetched: AtomicU64,
    /// Bytes of whole files queued for (or past) an md5 pass, and how many of them were hashed.
    /// They give the closing stretch of an install (everything is downloaded, big files are being
    /// verified) a progress of its own instead of a bar frozen at 100 % and 0 B/s.
    to_hash: AtomicU64,
    hashed: AtomicU64,
    hash_sem: Arc<Semaphore>,
}

impl Run {
    fn new(
        action: Action,
        http: Http,
        game_dir: &Path,
        job: &KuroJob,
        index: Index,
        ctx: JobContext,
    ) -> Result<Self> {
        let journal = if action.writes_game() {
            fs::create_dir_all(game_dir).context(|| format!("creating {}", game_dir.display()))?;
            let header = format!("{}:{}", action.name(), index.id);
            Some(Journal::open(
                &game_dir.join(WORK_DIR),
                JOURNAL_SLOT,
                &header,
            )?)
        } else {
            None
        };
        // Repair must not take anyone's word for what is on disk.
        let stored = (action == Action::Install)
            .then(|| Index::load_stored(game_dir))
            .flatten()
            .map(|old| {
                old.files
                    .into_iter()
                    .map(|f| (f.name, (f.md5, f.size)))
                    .collect()
            });
        Ok(Self {
            action,
            http,
            game_dir: game_dir.to_owned(),
            base_url: job.base_url.trim_end_matches('/').to_owned(),
            index,
            stored,
            journal,
            ctx,
            done: AtomicU64::new(0),
            net: AtomicU64::new(0),
            fetching: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
            failures: AtomicUsize::new(0),
            first_failure: Mutex::new(None),
            fatal: Mutex::new(None),
            files_trusted: AtomicU64::new(0),
            files_verified: AtomicU64::new(0),
            files_fetched: AtomicU64::new(0),
            files_patched: AtomicU64::new(0),
            regions_reused: AtomicU64::new(0),
            regions_fetched: AtomicU64::new(0),
            ranges_ok: AtomicBool::new(true),
            region_sem: Arc::new(Semaphore::new(REGIONS_IN_FLIGHT)),
            segment_sem: Arc::new(Semaphore::new(segments::SEGMENTS_IN_FLIGHT)),
            bytes_missing: AtomicU64::new(0),
            bytes_fetched: AtomicU64::new(0),
            to_hash: AtomicU64::new(0),
            hashed: AtomicU64::new(0),
            hash_sem: Arc::new(Semaphore::new(HASHES_IN_FLIGHT)),
        })
    }

    /// md5 of a whole file on the blocking pool, a few files at a time, counted in
    /// `to_hash` / `hashed` so the ticker can show the verification as progress.
    async fn hash_whole_file(self: &Arc<Self>, path: &Path, size: u64) -> Result<String> {
        self.to_hash.fetch_add(size, Ordering::Relaxed);
        let _permit = self
            .ctx
            .cancel
            .guard(self.hash_sem.clone().acquire_owned())
            .await?
            .map_err(|_| InstallError::Cancelled)?;
        let (path, cancel, run) = (path.to_owned(), self.ctx.cancel.clone(), self.clone());
        tokio::task::spawn_blocking(move || md5_file_counted(&path, &cancel, &run.hashed))
            .await
            .map_err(|e| InstallError::Archive(format!("hash task failed: {e}")))?
    }

    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed) || self.ctx.cancel.is_cancelled()
    }

    async fn execute(self: Arc<Self>) -> Result<Outcome> {
        let total = self.index.total();
        let ticker = (self.action != Action::Plan).then(|| tokio::spawn(self.clone().tick(total)));
        let result = self.clone().process_all().await;
        if let Some(ticker) = ticker {
            ticker.abort();
        }
        result?;
        self.ctx.cancel.check()?;
        self.finish(total)
    }

    async fn tick(self: Arc<Self>, total: u64) {
        let mut net_meter = SpeedMeter::new();
        let mut hash_meter = SpeedMeter::new();
        let mut last_net = 0u64;
        let mut last_hashed = 0u64;
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let net = self.net.load(Ordering::Relaxed);
            let net_speed = net_meter.add(net.saturating_sub(last_net));
            last_net = net;
            let hashed = self.hashed.load(Ordering::Relaxed);
            let hash_speed = hash_meter.add(hashed.saturating_sub(last_hashed));
            last_hashed = hashed;
            let to_hash = self.to_hash.load(Ordering::Relaxed);

            let fetching = self.fetching.load(Ordering::Relaxed) > 0;
            let progress = if !fetching && hashed < to_hash {
                // Nothing is being downloaded and files are being verified: show that work, with
                // the hashing rate in place of the (zero) network speed.
                Progress {
                    phase: Phase::Verifying,
                    current: hashed,
                    total: to_hash,
                    speed: hash_speed,
                }
            } else {
                Progress {
                    phase: if fetching {
                        Phase::Downloading
                    } else {
                        Phase::Verifying
                    },
                    current: self.done.load(Ordering::Relaxed).min(total),
                    total,
                    speed: net_speed,
                }
            };
            self.ctx.reporter.report(progress);
        }
    }

    async fn process_all(self: Arc<Self>) -> Result<()> {
        let sem = Arc::new(Semaphore::new(FILES_IN_FLIGHT));
        let mut order: Vec<usize> = (0..self.index.files.len()).collect();
        // Big files first: the long pole starts early and small ones fill the gaps.
        order.sort_by_key(|&i| std::cmp::Reverse(self.index.files[i].size));

        let mut set: JoinSet<()> = JoinSet::new();
        for i in order {
            if self.stopped() {
                break;
            }
            let Ok(permit) = self.ctx.cancel.guard(sem.clone().acquire_owned()).await else {
                break;
            };
            let Ok(permit) = permit else { break };
            let run = self.clone();
            set.spawn(async move {
                let _permit = permit;
                if let Err(error) = run.process_file(i).await {
                    run.record(error);
                }
            });
        }
        if self.stopped() {
            set.abort_all();
        }
        while let Some(joined) = set.join_next().await {
            // A task that panicked never recorded its file: without this the run would report
            // success with that file missing.
            if let Err(error) = joined
                && !error.is_cancelled()
            {
                self.record(InstallError::Archive(format!(
                    "background task failed: {error}"
                )));
            }
        }

        if self.ctx.cancel.is_cancelled() {
            return Err(InstallError::Cancelled);
        }
        if let Some(fatal) = self.fatal.lock().unwrap_or_else(|p| p.into_inner()).take() {
            return Err(fatal);
        }
        let failed = self.failures.load(Ordering::Relaxed);
        if failed > 0 {
            let first = self
                .first_failure
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
                .unwrap_or_default();
            return Err(InstallError::Incomplete(format!(
                "{failed} file(s) could not be downloaded; first error: {first}. Running the \
                 operation again fetches only those."
            )));
        }
        Ok(())
    }

    fn record(&self, error: InstallError) {
        match error {
            InstallError::Cancelled => {}
            InstallError::Io { .. }
            | InstallError::NoSpace { .. }
            | InstallError::UnsupportedPlatform(_) => {
                let mut fatal = self.fatal.lock().unwrap_or_else(|p| p.into_inner());
                fatal.get_or_insert(error);
                self.stop.store(true, Ordering::SeqCst);
            }
            other => {
                tracing::warn!("[kuro] {other}");
                self.first_failure
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get_or_insert_with(|| other.to_string());
                if self.failures.fetch_add(1, Ordering::SeqCst) + 1 >= MAX_FILE_FAILURES {
                    self.stop.store(true, Ordering::SeqCst);
                }
            }
        }
    }

    fn trusted(&self, entry: &Entry) -> bool {
        if self
            .journal
            .as_ref()
            .is_some_and(|j| j.done.contains(&entry.name))
        {
            return true;
        }
        self.stored
            .as_ref()
            .and_then(|s| s.get(&entry.name))
            .is_some_and(|(md5, size)| *md5 == entry.md5 && *size == entry.size)
    }

    async fn process_file(self: &Arc<Self>, i: usize) -> Result<()> {
        let entry = &self.index.files[i];
        let path = self.game_dir.join(&entry.path);
        let on_disk = fs::metadata(&path).ok().filter(|m| m.is_file());
        let right_size = on_disk.as_ref().is_some_and(|m| m.len() == entry.size);

        if right_size && self.trusted(entry) {
            self.done.fetch_add(entry.size, Ordering::Relaxed);
            self.files_trusted.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Big files that the index describes in ranges: keep what matches, fetch the rest.
        if !entry.regions.is_empty()
            && on_disk.is_some()
            && self.ranges_ok.load(Ordering::Relaxed)
            && self.patch_file(i, &path).await?
        {
            return Ok(());
        }

        if right_size {
            let actual = self.hash_whole_file(&path, entry.size).await?;
            if actual == entry.md5 {
                self.done.fetch_add(entry.size, Ordering::Relaxed);
                self.files_verified.fetch_add(1, Ordering::Relaxed);
                if let Some(journal) = &self.journal {
                    journal.record(&entry.name);
                }
                return Ok(());
            }
        }

        self.bytes_missing.fetch_add(entry.size, Ordering::Relaxed);
        if self.action == Action::Plan {
            return Ok(());
        }
        if entry.size >= segments::SEGMENT_MIN_FILE
            && self.ranges_ok.load(Ordering::Relaxed)
            && self.fetch_segmented(entry, &path).await?
        {
            return Ok(());
        }
        self.fetch(entry, &path).await
    }

    /// Update an existing file in place from its ranges. `Ok(true)`: the file is now right (or, for
    /// a plan, the numbers were recorded). `Ok(false)`: not worth it or not possible, use the whole
    /// file path (nothing is left half-counted).
    async fn patch_file(self: &Arc<Self>, i: usize, path: &Path) -> Result<bool> {
        let entry = &self.index.files[i];
        let matched = {
            let (run, path, cancel) = (self.clone(), path.to_owned(), self.ctx.cancel.clone());
            tokio::task::spawn_blocking(move || {
                let entry = &run.index.files[i];
                inspect_regions(&path, &entry.regions, &cancel, |k, ok| {
                    if ok {
                        run.done.fetch_add(entry.regions[k].len, Ordering::Relaxed);
                    }
                })
            })
            .await
            .map_err(|e| InstallError::Archive(format!("hash task failed: {e}")))??
        };
        let reused_bytes: u64 = entry
            .regions
            .iter()
            .zip(&matched)
            .filter(|(_, ok)| **ok)
            .map(|(r, _)| r.len)
            .sum();
        let missing: Vec<usize> = (0..matched.len()).filter(|&k| !matched[k]).collect();
        let undo = |run: &Run| {
            run.done.fetch_sub(
                reused_bytes.min(run.done.load(Ordering::Relaxed)),
                Ordering::Relaxed,
            );
        };

        // Nothing in common: a plain download is simpler and keeps the old file until it is done.
        if reused_bytes == 0 {
            return Ok(false);
        }
        let missing_bytes: u64 = missing.iter().map(|&k| entry.regions[k].len).sum();
        self.regions_reused
            .fetch_add((matched.len() - missing.len()) as u64, Ordering::Relaxed);
        self.bytes_missing
            .fetch_add(missing_bytes, Ordering::Relaxed);
        if self.action == Action::Plan {
            return Ok(true);
        }
        if missing.is_empty() && fs::metadata(path).map(|m| m.len()).unwrap_or(0) == entry.size {
            self.files_verified.fetch_add(1, Ordering::Relaxed);
            if let Some(journal) = &self.journal {
                journal.record(&entry.name);
            }
            return Ok(true);
        }

        let file = {
            let (p, size) = (path.to_owned(), entry.size);
            tokio::task::spawn_blocking(move || crate::sophon::local::prepare_file(&p, size))
                .await
                .map_err(|e| InstallError::Archive(format!("background task failed: {e}")))??
        };
        let file = Arc::new(file);
        let url = format!("{}/{}", self.base_url, entry.name);

        let mut set: JoinSet<Result<u64>> = JoinSet::new();
        for &k in &missing {
            if self.stopped() {
                break;
            }
            let Ok(permit) = self
                .ctx
                .cancel
                .guard(self.region_sem.clone().acquire_owned())
                .await
            else {
                break;
            };
            let Ok(permit) = permit else { break };
            let (run, file, url, path) = (self.clone(), file.clone(), url.clone(), path.to_owned());
            set.spawn(async move {
                let _permit = permit;
                run.fetch_region_streamed(&url, &run.index.files[i].regions[k], &file, &path)
                    .await
            });
        }
        if self.stopped() {
            set.abort_all();
        }
        let mut failure: Option<InstallError> = None;
        let mut fetched_sum = 0u64;
        while let Some(finished) = set.join_next().await {
            match finished {
                Ok(Ok(len)) => fetched_sum += len,
                Ok(Err(error)) => {
                    failure.get_or_insert(error);
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    failure.get_or_insert(InstallError::Archive(format!(
                        "background task failed: {error}"
                    )));
                }
            }
        }
        self.ctx.cancel.check()?;
        match failure {
            None if !self.stopped() => {
                self.files_patched.fetch_add(1, Ordering::Relaxed);
                if let Some(journal) = &self.journal {
                    journal.record(&entry.name);
                }
                Ok(true)
            }
            None => Ok(true),
            Some(error @ (InstallError::Io { .. } | InstallError::NoSpace { .. })) => Err(error),
            Some(error) => {
                tracing::warn!(
                    "[kuro] {}: range update failed ({error}); downloading the whole file instead",
                    entry.name
                );
                if matches!(&error, InstallError::InvalidData(m) if m.contains("ignores Range")) {
                    self.ranges_ok.store(false, Ordering::Relaxed);
                }
                // Forget what this attempt added, the whole-file path counts the file again.
                undo(self);
                self.done.fetch_sub(
                    fetched_sum.min(self.done.load(Ordering::Relaxed)),
                    Ordering::Relaxed,
                );
                self.bytes_missing
                    .fetch_sub(missing_bytes, Ordering::Relaxed);
                self.regions_reused
                    .fetch_sub((matched.len() - missing.len()) as u64, Ordering::Relaxed);
                Ok(false)
            }
        }
    }

    async fn fetch(self: &Arc<Self>, entry: &Entry, final_path: &Path) -> Result<()> {
        let staged = self.game_dir.join(WORK_DIR).join(&entry.path);
        let url = format!("{}/{}", self.base_url, entry.name);
        let spec = DownloadSpec::new(url, &staged).with_checksum(Checksum::Md5(entry.md5.clone()));

        // download_file reports per-file numbers; fold them into the shared byte counters.
        let seen = Arc::new(AtomicU64::new(0));
        let first = Arc::new(AtomicBool::new(true));
        let (run, seen_cb, first_cb) = (self.clone(), seen.clone(), first.clone());
        let file_ctx = JobContext::new()
            .with_cancel(self.ctx.cancel.clone())
            .with_progress(move |p| {
                if p.phase != Phase::Downloading {
                    return;
                }
                let last = seen_cb.swap(p.current, Ordering::Relaxed);
                if p.current >= last {
                    let delta = p.current - last;
                    run.done.fetch_add(delta, Ordering::Relaxed);
                    // The first report is the offset a resumed `.part` already had.
                    if !first_cb.swap(false, Ordering::Relaxed) {
                        run.net.fetch_add(delta, Ordering::Relaxed);
                    }
                } else {
                    // The transfer started over.
                    run.done
                        .fetch_sub((last - p.current).min(last), Ordering::Relaxed);
                }
            });

        self.fetching.fetch_add(1, Ordering::Relaxed);
        let result = download_file(&self.http, &spec, &file_ctx).await;
        self.fetching.fetch_sub(1, Ordering::Relaxed);

        // Reconcile the shared counter with what this file really is (success) or contributed.
        let counted = seen.load(Ordering::Relaxed);
        match &result {
            Ok(_) => {
                self.done
                    .fetch_add(entry.size.saturating_sub(counted), Ordering::Relaxed);
            }
            Err(_) => {
                // Failed or cancelled: the next run re-counts the `.part` it resumes from.
                self.done.fetch_sub(
                    counted.min(self.done.load(Ordering::Relaxed)),
                    Ordering::Relaxed,
                );
            }
        }
        result?;

        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).context(|| format!("creating {}", parent.display()))?;
        }
        fs::rename(&staged, final_path)
            .context(|| format!("moving {} into place", final_path.display()))?;
        if let Some(journal) = &self.journal {
            journal.record(&entry.name);
        }
        self.files_fetched.fetch_add(1, Ordering::Relaxed);
        self.bytes_fetched.fetch_add(entry.size, Ordering::Relaxed);
        Ok(())
    }

    fn finish(&self, total: u64) -> Result<Outcome> {
        self.ctx.reporter.report_now(Progress {
            phase: Phase::Finalizing,
            current: total,
            total,
            speed: 0,
        });
        let mut pruned = 0u64;
        if self.action.writes_game() {
            pruned = self.prune_stale();
            let dir = self.game_dir.join(STORED_DIR);
            fs::create_dir_all(&dir).context(|| format!("creating {}", dir.display()))?;
            atomic_write(&dir.join(STORED_INDEX), &self.index.raw)?;
            // Everything left in the work folder is garbage once the release is complete; its
            // presence would keep the game marked as unfinished.
            let work = self.game_dir.join(WORK_DIR);
            if work.exists() {
                fs::remove_dir_all(&work).context(|| format!("removing {}", work.display()))?;
            }
        }
        self.ctx.reporter.report_now(Progress {
            phase: Phase::Finalizing,
            current: total,
            total,
            speed: 0,
        });
        Ok(Outcome {
            files: self.index.files.len() as u64,
            files_trusted: self.files_trusted.load(Ordering::Relaxed),
            files_verified: self.files_verified.load(Ordering::Relaxed),
            files_fetched: self.files_fetched.load(Ordering::Relaxed),
            files_patched: self.files_patched.load(Ordering::Relaxed),
            regions_reused: self.regions_reused.load(Ordering::Relaxed),
            regions_fetched: self.regions_fetched.load(Ordering::Relaxed),
            bytes_total: total,
            bytes_missing: self.bytes_missing.load(Ordering::Relaxed),
            bytes_fetched: self.bytes_fetched.load(Ordering::Relaxed),
            files_pruned: pruned,
        })
    }

    /// Remove what the index says to delete, and files the previous release listed that this one
    /// no longer does. Anything no index ever mentioned (saves, settings, mods) stays.
    fn prune_stale(&self) -> u64 {
        let mut removed = 0u64;
        let current: std::collections::HashSet<&str> =
            self.index.files.iter().map(|f| f.name.as_str()).collect();
        let mut doomed: Vec<PathBuf> = self.index.delete.clone();
        if let Some(old) = Index::load_stored(&self.game_dir) {
            doomed.extend(
                old.files
                    .into_iter()
                    .filter(|f| !current.contains(f.name.as_str()))
                    .map(|f| f.path),
            );
        }
        for relative in doomed {
            let name = relative.to_string_lossy().replace('\\', "/");
            if current.contains(name.as_str()) {
                continue;
            }
            if fs::remove_file(self.game_dir.join(&relative)).is_ok() {
                removed += 1;
            }
        }
        removed
    }
}

/// md5 of a whole file, polling `cancel` between reads and adding every byte read to `counter`.
/// Blocking. (Unlike [`crate::hash::md5_file`] it does not report through a [`JobContext`]: a
/// per-file report would overwrite the operation-wide numbers the UI shows.)
fn md5_file_counted(path: &Path, cancel: &CancelToken, counter: &AtomicU64) -> Result<String> {
    let mut file = fs::File::open(path).context(|| format!("opening {}", path.display()))?;
    let mut hasher = Md5::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        cancel.check()?;
        let read = file
            .read(&mut buffer)
            .context(|| format!("reading {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        counter.fetch_add(read as u64, Ordering::Relaxed);
    }
    Ok(hex(&hasher.finalize()))
}

/// For every region: does the file already hold exactly that data? Short or missing files answer
/// `false` for what they lack. Blocking.
fn inspect_regions(
    path: &Path,
    regions: &[Region],
    cancel: &crate::progress::CancelToken,
    mut on_region: impl FnMut(usize, bool),
) -> Result<Vec<bool>> {
    use std::os::unix::fs::FileExt;
    let file = fs::File::open(path).ok();
    let file_len = file
        .as_ref()
        .and_then(|f| f.metadata().ok())
        .map(|m| m.len())
        .unwrap_or(0);
    let mut buffer = Vec::new();
    let mut answers = Vec::with_capacity(regions.len());
    for (index, region) in regions.iter().enumerate() {
        cancel.check()?;
        let ok = match &file {
            Some(file) if region.start.saturating_add(region.len) <= file_len => {
                buffer.resize(region.len as usize, 0);
                file.read_exact_at(&mut buffer, region.start).is_ok()
                    && hex(&Md5::digest(&buffer)) == region.md5
            }
            _ => false,
        };
        on_region(index, ok);
        answers.push(ok);
    }
    Ok(answers)
}
