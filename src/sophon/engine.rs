//! The Sophon engine: install, update, repair and pre-download of HoYoverse games.
//!
//! # One idea
//!
//! A Sophon manifest describes every file as a list of *chunks*: fixed regions of the final file,
//! each stored on the CDN as a separate zstd blob and each with the md5 of its decompressed data.
//! So "what do I still need?" is a question the manifest can answer by itself:
//!
//! > hash every region of the file on disk; the regions that already match need nothing, the rest
//! > are downloaded and written **in place** at their offsets.
//!
//! That one rule gives every operation for free:
//!
//! * **fresh install**: nothing matches, everything is downloaded;
//! * **update**: unchanged files and unchanged regions of changed files are kept, only different
//!   chunks cross the network (the job `hdiffpatch` used to do, by reuse instead of by patching);
//! * **resume** after a pause or crash: chunks written before the stop match their md5 and are
//!   skipped, no bookkeeping to corrupt;
//! * **repair**: the same pass with no trust shortcuts, fetching only damaged regions;
//! * **pre-download**: the same pass, but the missing chunks go to a cache folder instead of the
//!   game, and the later update finds them there.
//!
//! Shortcuts that only skip *hashing*, never correctness checks of downloaded data:
//! a file whose manifest entry is identical to the one stored by the last successful install is
//! trusted (updates stay fast), and a file already verified earlier in an interrupted run is
//! trusted through a small journal. Repair never trusts the stored manifest.
//!
//! Every downloaded chunk is checked three times (compressed length + md5, decompressed length +
//! md5) before it touches the game, and a chunk that cannot be fetched makes the operation fail
//! instead of "completing with failures".

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::local;
use super::manifest::{self, Chunk, FileEntry, Manifest, safe_slot};
use crate::error::{InstallError, IoContext, Result};
use crate::fsutil::{ensure_free_space, remove_dir_if_exists};
use crate::http::{Http, sleep_unless_cancelled};
use crate::progress::{JobContext, Phase, Progress, SpeedMeter};

/// Files inspected or written at the same time (bounds open files and hashing threads).
const FILES_IN_FLIGHT: usize = 4;
/// Chunk downloads in flight across all files.
const CHUNKS_IN_FLIGHT: usize = 24;
/// Attempts per chunk on top of the HTTP layer's own retries (bad bytes, CDN hiccups).
const CHUNK_ATTEMPTS: u32 = 3;
/// After this many chunks failed for good the operation gives up instead of grinding through
/// thousands more (typically the network is simply down).
const MAX_CHUNK_FAILURES: usize = 8;
const DISK_MARGIN: u64 = 64 * 1024 * 1024;

pub const WORK_DIR: &str = "downloading";
pub const PRELOAD_DIR: &str = ".berry-preload";
const PRELOAD_READY: &str = "READY";

/// One downloadable piece of a game: the game data, or one voice pack.
#[derive(Debug, Clone)]
pub struct SophonJob {
    /// Stable name (`game`, `en-us`): keys the stored manifest, journal and pre-download cache.
    pub slot: String,
    pub manifest_url: String,
    /// `<chunk_base>/<chunk_name>` is a chunk's URL.
    pub chunk_base: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Install,
    Repair,
    Preload,
    Plan,
}

impl Action {
    fn writes_game(self) -> bool {
        matches!(self, Self::Install | Self::Repair)
    }

    fn name(self) -> &'static str {
        match self {
            Self::Install => "install",
            Self::Repair => "repair",
            Self::Preload => "preload",
            Self::Plan => "plan",
        }
    }
}

/// What an operation did (or, for [`SophonInstaller::plan`], would have to do).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Outcome {
    pub files: u64,
    /// Files accepted without hashing (identical to the stored manifest, or verified earlier).
    pub files_trusted: u64,
    pub chunks: u64,
    /// Chunks whose region on disk already matched.
    pub chunks_reused: u64,
    /// Chunks that had to come from somewhere else (pre-download cache or network).
    pub chunks_missing: u64,
    pub chunks_from_cache: u64,
    pub chunks_fetched: u64,
    /// Compressed size of the whole manifest.
    pub bytes_total: u64,
    /// Compressed size of the chunks that are missing locally.
    pub bytes_missing: u64,
    /// Bytes that really crossed the network.
    pub bytes_fetched: u64,
    pub files_pruned: u64,
}

#[derive(Debug, Clone)]
pub struct SophonInstaller {
    http: Http,
}

impl SophonInstaller {
    pub fn new(http: Http) -> Self {
        Self { http }
    }

    /// Install a new game or bring an installed one to the manifest's version (also resumes).
    pub async fn install(
        &self,
        game_dir: &Path,
        job: &SophonJob,
        ctx: &JobContext,
    ) -> Result<Outcome> {
        self.run(Action::Install, game_dir, job, ctx).await
    }

    /// Verify every byte against the manifest and fix what differs.
    pub async fn repair(
        &self,
        game_dir: &Path,
        job: &SophonJob,
        ctx: &JobContext,
    ) -> Result<Outcome> {
        self.run(Action::Repair, game_dir, job, ctx).await
    }

    /// Download the chunks an update will need into a cache next to the game, without touching the
    /// game itself. The later [`install`](Self::install) uses them instead of the network.
    pub async fn preload(
        &self,
        game_dir: &Path,
        job: &SophonJob,
        ctx: &JobContext,
    ) -> Result<Outcome> {
        self.run(Action::Preload, game_dir, job, ctx).await
    }

    /// Read-only: what would [`install`](Self::install) have to download?
    pub async fn plan(
        &self,
        game_dir: &Path,
        job: &SophonJob,
        ctx: &JobContext,
    ) -> Result<Outcome> {
        self.run(Action::Plan, game_dir, job, ctx).await
    }

    /// Manifest id of a completed pre-download for `slot`, if there is one.
    pub fn preload_ready(game_dir: &Path, slot: &str) -> Option<String> {
        let id = fs::read_to_string(preload_dir(game_dir, slot).join(PRELOAD_READY)).ok()?;
        let id = id.trim();
        (!id.is_empty()).then(|| id.to_owned())
    }

    async fn run(
        &self,
        action: Action,
        game_dir: &Path,
        job: &SophonJob,
        ctx: &JobContext,
    ) -> Result<Outcome> {
        ctx.cancel.check()?;
        let manifest = manifest::fetch(&self.http, &job.manifest_url, ctx).await?;
        tracing::info!(
            "[sophon] {} {}: {} files, {} to describe, manifest {}",
            action.name(),
            job.slot,
            manifest.files.len(),
            crate::error::format_bytes(manifest.compressed_total()),
            manifest.id
        );
        let run = Arc::new(Run::new(
            action,
            self.http.clone(),
            game_dir,
            job,
            manifest,
            ctx.clone(),
        )?);
        run.execute().await
    }
}

fn preload_dir(game_dir: &Path, slot: &str) -> PathBuf {
    game_dir.join(PRELOAD_DIR).join(safe_slot(slot))
}

/// Append-only list of files verified during this (possibly interrupted) operation.
pub(crate) struct Journal {
    pub(crate) done: HashSet<String>,
    file: Mutex<File>,
    pub(crate) path: PathBuf,
}

impl Journal {
    pub(crate) fn open(dir: &Path, slot: &str, header: &str) -> Result<Self> {
        fs::create_dir_all(dir).context(|| format!("creating {}", dir.display()))?;
        let path = dir.join(format!("journal-{}", safe_slot(slot)));
        let mut done = HashSet::new();
        let mut valid = false;
        if let Ok(text) = fs::read_to_string(&path) {
            let mut lines = text.lines();
            if lines.next() == Some(header) {
                valid = true;
                done.extend(lines.map(str::to_owned));
            }
        }
        let mut options = OpenOptions::new();
        options.create(true);
        let mut file = if valid {
            options.append(true)
        } else {
            options.write(true).truncate(true)
        }
        .open(&path)
        .context(|| format!("opening {}", path.display()))?;
        if !valid {
            writeln!(file, "{header}").context(|| format!("writing {}", path.display()))?;
        }
        Ok(Self {
            done,
            file: Mutex::new(file),
            path,
        })
    }

    pub(crate) fn record(&self, name: &str) {
        let mut file = self.file.lock().unwrap_or_else(|p| p.into_inner());
        // Best effort: losing a line only means hashing that file again next time.
        let _ = writeln!(file, "{name}");
    }
}

struct Run {
    action: Action,
    http: Http,
    game_dir: PathBuf,
    job: SophonJob,
    manifest: Manifest,
    /// Manifest of what the last successful operation left on disk.
    stored: Option<(Manifest, HashMap<String, usize>)>,
    journal: Option<Journal>,
    ctx: JobContext,

    chunk_sem: Arc<Semaphore>,
    /// Compressed bytes accounted for (already right, or fetched): the progress bar's numerator.
    done: AtomicU64,
    net: AtomicU64,
    /// Smoothed network speed, refreshed by the ticker.
    speed: AtomicU64,
    fetching: AtomicUsize,
    stop: AtomicBool,
    failures: AtomicUsize,
    first_failure: Mutex<Option<String>>,
    fatal: Mutex<Option<InstallError>>,

    files_trusted: AtomicU64,
    chunks_reused: AtomicU64,
    chunks_missing: AtomicU64,
    chunks_from_cache: AtomicU64,
    chunks_fetched: AtomicU64,
    bytes_missing: AtomicU64,
}

impl Run {
    fn new(
        action: Action,
        http: Http,
        game_dir: &Path,
        job: &SophonJob,
        manifest: Manifest,
        ctx: JobContext,
    ) -> Result<Self> {
        if action.writes_game() {
            fs::create_dir_all(game_dir).context(|| format!("creating {}", game_dir.display()))?;
        }
        // Repair must not take anyone's word for what is on disk.
        let stored = (action != Action::Repair)
            .then(|| Manifest::load_stored(game_dir, &job.slot))
            .flatten()
            .map(|m| {
                let index = m
                    .files
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.name.clone(), i))
                    .collect();
                (m, index)
            });
        let journal = if action.writes_game() {
            let header = format!("{}:{}", action.name(), manifest.id);
            Some(Journal::open(&game_dir.join(WORK_DIR), &job.slot, &header)?)
        } else {
            None
        };
        Ok(Self {
            action,
            http,
            game_dir: game_dir.to_owned(),
            job: job.clone(),
            manifest,
            stored,
            journal,
            ctx,
            chunk_sem: Arc::new(Semaphore::new(CHUNKS_IN_FLIGHT)),
            done: AtomicU64::new(0),
            net: AtomicU64::new(0),
            speed: AtomicU64::new(0),
            fetching: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
            failures: AtomicUsize::new(0),
            first_failure: Mutex::new(None),
            fatal: Mutex::new(None),
            files_trusted: AtomicU64::new(0),
            chunks_reused: AtomicU64::new(0),
            chunks_missing: AtomicU64::new(0),
            chunks_from_cache: AtomicU64::new(0),
            chunks_fetched: AtomicU64::new(0),
            bytes_missing: AtomicU64::new(0),
        })
    }

    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed) || self.ctx.cancel.is_cancelled()
    }

    fn cache_dir(&self) -> PathBuf {
        preload_dir(&self.game_dir, &self.job.slot)
    }

    fn chunk_url(&self, chunk: &Chunk) -> String {
        format!(
            "{}/{}",
            self.job.chunk_base.trim_end_matches('/'),
            chunk.name
        )
    }

    async fn execute(self: Arc<Self>) -> Result<Outcome> {
        let total = self.manifest.compressed_total();
        if self.action.writes_game() {
            self.check_disk_space()?;
            for dir in &self.manifest.directories {
                let path = self.game_dir.join(dir);
                fs::create_dir_all(&path).context(|| format!("creating {}", path.display()))?;
            }
        }
        if self.action == Action::Preload {
            let cache = self.cache_dir();
            fs::create_dir_all(&cache).context(|| format!("creating {}", cache.display()))?;
            // A cache that was complete for another manifest is not complete for this one.
            let _ = fs::remove_file(cache.join(PRELOAD_READY));
        }

        let ticker = (self.action != Action::Plan).then(|| tokio::spawn(self.clone().tick(total)));
        let result = self.clone().process_all().await;
        if let Some(ticker) = ticker {
            ticker.abort();
        }

        result?;
        self.ctx.cancel.check()?;
        self.finish(total).await
    }

    /// Reports progress about four times a second until aborted.
    async fn tick(self: Arc<Self>, total: u64) {
        let mut meter = SpeedMeter::new();
        let mut last_net = 0u64;
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let net = self.net.load(Ordering::Relaxed);
            let speed = meter.add(net.saturating_sub(last_net));
            last_net = net;
            self.speed.store(speed, Ordering::Relaxed);
            let phase = if self.fetching.load(Ordering::Relaxed) > 0 {
                Phase::Downloading
            } else {
                Phase::Verifying
            };
            let current = self.done.load(Ordering::Relaxed).min(total);
            self.ctx.reporter.report(Progress {
                phase,
                current,
                total,
                speed,
            });
        }
    }

    fn check_disk_space(&self) -> Result<()> {
        let mut needed = 0u64;
        for file in &self.manifest.files {
            let have = fs::metadata(self.game_dir.join(&file.path))
                .map(|m| m.len())
                .unwrap_or(0);
            needed += file.size.saturating_sub(have);
        }
        ensure_free_space(&self.game_dir, needed + DISK_MARGIN)
    }

    async fn process_all(self: Arc<Self>) -> Result<()> {
        let file_sem = Arc::new(Semaphore::new(FILES_IN_FLIGHT));
        let mut order: Vec<usize> = (0..self.manifest.files.len()).collect();
        // Big files first: the long pole starts early and small ones fill the gaps.
        order.sort_by_key(|&i| std::cmp::Reverse(self.manifest.files[i].size));

        let mut set: JoinSet<()> = JoinSet::new();
        for index in order {
            if self.stopped() {
                break;
            }
            let Ok(permit) = self
                .ctx
                .cancel
                .guard(file_sem.clone().acquire_owned())
                .await
            else {
                break;
            };
            let Ok(permit) = permit else { break };
            let run = self.clone();
            set.spawn(async move {
                let _permit = permit;
                if let Err(error) = run.process_file(index).await {
                    run.record(error);
                }
            });
        }
        if self.stopped() {
            set.abort_all();
        }
        while set.join_next().await.is_some() {}

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
            return Err(InstallError::ChunksFailed {
                failed: failed as u64,
                first,
            });
        }
        Ok(())
    }

    /// File-level errors end up here: chunk problems are counted, disk problems are fatal.
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
                tracing::warn!("[sophon] {}: {other}", self.job.slot);
                self.first_failure
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get_or_insert_with(|| other.to_string());
                if self.failures.fetch_add(1, Ordering::SeqCst) + 1 >= MAX_CHUNK_FAILURES {
                    self.stop.store(true, Ordering::SeqCst);
                }
            }
        }
    }

    fn trusted(&self, entry: &FileEntry) -> bool {
        if self
            .journal
            .as_ref()
            .is_some_and(|j| j.done.contains(&entry.name))
        {
            return true;
        }
        self.stored
            .as_ref()
            .and_then(|(m, index)| index.get(&entry.name).map(|&i| &m.files[i]))
            .is_some_and(|old| old.same_content(entry))
    }

    async fn process_file(self: &Arc<Self>, index: usize) -> Result<()> {
        let entry = &self.manifest.files[index];
        let path = self.game_dir.join(&entry.path);
        let compressed = entry.compressed_size();

        if self.trusted(entry)
            && fs::metadata(&path)
                .map(|m| m.len() == entry.size)
                .unwrap_or(false)
        {
            self.done.fetch_add(compressed, Ordering::Relaxed);
            self.files_trusted.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Write modes open the file at its final size first, so the inspection below sees what
        // an interrupted run left behind and extends or cuts the rest.
        let handle: Option<Arc<File>> = if self.action.writes_game() {
            let (p, size) = (path.clone(), entry.size);
            let file = tokio::task::spawn_blocking(move || local::prepare_file(&p, size))
                .await
                .map_err(join_error)??;
            Some(Arc::new(file))
        } else {
            None
        };

        let known_ok = {
            let (run, path, cancel) = (self.clone(), path.clone(), self.ctx.cancel.clone());
            tokio::task::spawn_blocking(move || {
                let entry = &run.manifest.files[index];
                local::inspect_chunks(&path, entry, &cancel, |i, ok| {
                    if ok {
                        run.done
                            .fetch_add(entry.chunks[i].compressed_len, Ordering::Relaxed);
                    }
                })
            })
            .await
            .map_err(join_error)??
        };

        let missing: Vec<usize> = known_ok
            .iter()
            .enumerate()
            .filter(|(_, ok)| !**ok)
            .map(|(i, _)| i)
            .collect();
        self.chunks_reused
            .fetch_add((known_ok.len() - missing.len()) as u64, Ordering::Relaxed);
        self.chunks_missing
            .fetch_add(missing.len() as u64, Ordering::Relaxed);
        self.bytes_missing.fetch_add(
            missing
                .iter()
                .map(|&i| entry.chunks[i].compressed_len)
                .sum(),
            Ordering::Relaxed,
        );

        if self.action == Action::Plan {
            return Ok(());
        }

        let mut chunk_set: JoinSet<Result<()>> = JoinSet::new();
        let mut failed = false;
        for chunk_index in missing {
            if self.stopped() {
                break;
            }
            let Ok(permit) = self
                .ctx
                .cancel
                .guard(self.chunk_sem.clone().acquire_owned())
                .await
            else {
                break;
            };
            let Ok(permit) = permit else { break };
            let (run, handle, path) = (self.clone(), handle.clone(), path.clone());
            chunk_set.spawn(async move {
                let _permit = permit;
                let chunk = &run.manifest.files[index].chunks[chunk_index];
                run.fetch_chunk(chunk, handle.as_deref(), &path).await
            });
            while let Some(finished) = chunk_set.try_join_next() {
                failed |= self.settle(finished);
            }
        }
        if self.stopped() {
            chunk_set.abort_all();
        }
        while let Some(finished) = chunk_set.join_next().await {
            failed |= self.settle(finished);
        }
        if failed || self.stopped() {
            return Ok(());
        }

        if self.action.writes_game() && !entry.covered && !entry.md5.is_empty() {
            let (p, cancel) = (path.clone(), self.ctx.cancel.clone());
            let actual = tokio::task::spawn_blocking(move || local::file_md5(&p, &cancel))
                .await
                .map_err(join_error)??;
            if actual != entry.md5 {
                return Err(InstallError::ChecksumMismatch {
                    file: entry.name.clone(),
                    expected: entry.md5.clone(),
                    actual,
                });
            }
        }
        if let Some(journal) = &self.journal {
            journal.record(&entry.name);
        }
        Ok(())
    }

    /// Account for one finished chunk task. `true` when it failed.
    fn settle(&self, finished: std::result::Result<Result<()>, tokio::task::JoinError>) -> bool {
        match finished {
            Ok(Ok(())) => false,
            Ok(Err(error)) => {
                self.record(error);
                true
            }
            Err(error) if error.is_cancelled() => true,
            Err(error) => {
                self.record(join_error(error));
                true
            }
        }
    }

    /// Get one chunk (cache or network), verify it fully and put it where it belongs.
    async fn fetch_chunk(&self, chunk: &Chunk, target: Option<&File>, path: &Path) -> Result<()> {
        let mut last = None;
        for attempt in 1..=CHUNK_ATTEMPTS {
            self.ctx.cancel.check()?;
            match self.try_chunk(chunk, target, path).await {
                Ok(()) => return Ok(()),
                Err(error) if error.is_cancelled() => return Err(error),
                Err(error) if is_permanent(&error) => return Err(error),
                Err(error) => {
                    tracing::debug!(
                        "[sophon] chunk {} attempt {attempt}/{CHUNK_ATTEMPTS}: {error}",
                        chunk.name
                    );
                    last = Some(error);
                    sleep_unless_cancelled(
                        Duration::from_millis(400 * u64::from(attempt)),
                        &self.ctx.cancel,
                    )
                    .await?;
                }
            }
        }
        Err(last.expect("at least one attempt ran"))
    }

    async fn try_chunk(&self, chunk: &Chunk, target: Option<&File>, path: &Path) -> Result<()> {
        let cache_file = self.cache_dir().join(&chunk.name);
        let (compressed, cached) = match self.read_cache(&cache_file, chunk).await {
            Some(bytes) => (bytes, true),
            None => {
                let max_len = if chunk.compressed_len > 0 {
                    chunk.compressed_len
                } else {
                    64 * 1024 * 1024
                };
                self.fetching.fetch_add(1, Ordering::Relaxed);
                let bytes = self
                    .http
                    .get_bytes(&self.chunk_url(chunk), max_len, &self.ctx.cancel)
                    .await;
                self.fetching.fetch_sub(1, Ordering::Relaxed);
                let bytes = bytes?;
                local::verify_compressed(&bytes, chunk)?;
                self.net.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                (bytes, false)
            }
        };

        match target {
            None => {
                // Pre-download: keep the verified compressed chunk for later.
                if !cached {
                    self.write_cache(&cache_file, &compressed).await?;
                }
            }
            Some(file) => {
                // The handle outlives the blocking call through a duplicated descriptor.
                let file = file
                    .try_clone()
                    .context(|| format!("duplicating the handle of {}", path.display()))?;
                let (chunk_owned, path_owned) = (chunk.clone(), path.to_owned());
                tokio::task::spawn_blocking(move || {
                    let data = local::decode_chunk(&compressed, &chunk_owned)?;
                    local::write_at(&file, chunk_owned.offset, &data, &path_owned)
                })
                .await
                .map_err(join_error)??;
                if cached {
                    let _ = tokio::fs::remove_file(&cache_file).await;
                }
            }
        }

        let done =
            self.done.fetch_add(chunk.compressed_len, Ordering::Relaxed) + chunk.compressed_len;
        // Between ticker beats: chunk completions keep the bar moving (throttled by the reporter).
        let update = Progress {
            phase: Phase::Downloading,
            current: done.min(self.manifest.compressed_total()),
            total: self.manifest.compressed_total(),
            speed: self.speed.load(Ordering::Relaxed),
        };
        if !cached && self.chunks_fetched.load(Ordering::Relaxed) == 0 {
            // First network chunk: the phase change is always delivered.
            self.ctx.reporter.report_now(update);
        } else {
            self.ctx.reporter.report(update);
        }
        if cached {
            self.chunks_from_cache.fetch_add(1, Ordering::Relaxed);
        } else {
            self.chunks_fetched.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn read_cache(&self, path: &Path, chunk: &Chunk) -> Option<Vec<u8>> {
        let bytes = tokio::fs::read(path).await.ok()?;
        if local::verify_compressed(&bytes, chunk).is_ok() {
            Some(bytes)
        } else {
            let _ = tokio::fs::remove_file(path).await;
            None
        }
    }

    async fn write_cache(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        let part = crate::fsutil::sibling(path, ".part");
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .context(|| format!("creating {}", parent.display()))?;
        }
        tokio::fs::write(&part, bytes)
            .await
            .context(|| format!("writing {}", part.display()))?;
        tokio::fs::rename(&part, path)
            .await
            .context(|| format!("moving {} into place", part.display()))
    }

    async fn finish(self: Arc<Self>, total: u64) -> Result<Outcome> {
        self.ctx.reporter.report_now(Progress {
            phase: Phase::Finalizing,
            current: total,
            total,
            speed: 0,
        });
        let mut pruned = 0u64;
        match self.action {
            Action::Install | Action::Repair => {
                pruned = self.prune_stale();
                self.manifest.store(&self.game_dir, &self.job.slot)?;
                if let Some(journal) = &self.journal {
                    let _ = fs::remove_file(&journal.path);
                }
                let work = self.game_dir.join(WORK_DIR);
                // Leftovers (older launchers, stray `.part` files) are garbage once this slot is
                // complete; only another slot's journal is kept. The directory itself goes away
                // when nothing remains, because its presence marks the game as unfinished.
                if let Ok(entries) = fs::read_dir(&work) {
                    for entry in entries.flatten() {
                        if entry.file_name().to_string_lossy().starts_with("journal-") {
                            continue;
                        }
                        let path = entry.path();
                        let _ = if path.is_dir() {
                            fs::remove_dir_all(&path)
                        } else {
                            fs::remove_file(&path)
                        };
                    }
                }
                let _ = fs::remove_dir(&work);
                let cache = self.cache_dir();
                remove_dir_if_exists(&cache)?;
                let _ = fs::remove_dir(self.game_dir.join(PRELOAD_DIR));
            }
            Action::Preload => {
                crate::fsutil::atomic_write(
                    &self.cache_dir().join(PRELOAD_READY),
                    self.manifest.id.as_bytes(),
                )?;
            }
            Action::Plan => {}
        }
        self.ctx.reporter.report_now(Progress {
            phase: Phase::Finalizing,
            current: total,
            total,
            speed: 0,
        });
        Ok(Outcome {
            files: self.manifest.files.len() as u64,
            files_trusted: self.files_trusted.load(Ordering::Relaxed),
            chunks: self
                .manifest
                .files
                .iter()
                .map(|f| f.chunks.len() as u64)
                .sum(),
            chunks_reused: self.chunks_reused.load(Ordering::Relaxed),
            chunks_missing: self.chunks_missing.load(Ordering::Relaxed),
            chunks_from_cache: self.chunks_from_cache.load(Ordering::Relaxed),
            chunks_fetched: self.chunks_fetched.load(Ordering::Relaxed),
            bytes_total: total,
            bytes_missing: self.bytes_missing.load(Ordering::Relaxed),
            bytes_fetched: self.net.load(Ordering::Relaxed),
            files_pruned: pruned,
        })
    }

    /// Delete files the previous version listed and this one does not. Only files a manifest ever
    /// named: screenshots, logs, mods and the game's own downloaded data are never touched.
    fn prune_stale(&self) -> u64 {
        let Some((old, _)) = &self.stored else {
            return 0;
        };
        let keep: HashSet<&str> = self
            .manifest
            .files
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        let mut removed = 0;
        for file in old.files.iter().filter(|f| !keep.contains(f.name.as_str())) {
            if fs::remove_file(self.game_dir.join(&file.path)).is_ok() {
                removed += 1;
            }
        }
        removed
    }
}

/// Errors a second attempt cannot fix: our own disk or platform, or the CDN saying "no" (a 4xx
/// other than the retryable 408/429).
fn is_permanent(error: &InstallError) -> bool {
    match error {
        InstallError::Io { .. }
        | InstallError::NoSpace { .. }
        | InstallError::UnsupportedPlatform(_) => true,
        InstallError::HttpStatus { status, .. } => {
            (400..500).contains(status) && !error.is_transient()
        }
        _ => false,
    }
}

fn join_error(error: tokio::task::JoinError) -> InstallError {
    InstallError::Archive(format!("background task failed: {error}"))
}
