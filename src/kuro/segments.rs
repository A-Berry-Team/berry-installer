//! Segmented, streaming transfers for the Kuro engine.
//!
//! Wuthering Waves ships a handful of multi-gigabyte `.pak` files next to thousands of small ones.
//! One HTTP connection per file leaves most of a fast line idle for the whole install (the big
//! files are the long pole), so large files are cut into fixed segments that are fetched over
//! several connections at once and written straight to their offset in a pre-sized `.part` file.
//!
//! * a sidecar ledger (`<part>.segs`) lists the segments already on disk, so an interrupted
//!   transfer resumes where it stopped, segment by segment, across launcher restarts;
//! * a segment that breaks halfway resumes from the byte it reached, not from its start;
//! * bodies are streamed to disk in 1 MiB writes: memory use no longer depends on region or
//!   segment size;
//! * the whole file is still checked against the index md5 before it is moved into place, and a
//!   mismatch (a torn write after a crash, a bad CDN node) restarts that file from scratch once;
//! * a server that ignores `Range` hands the file back to the plain sequential path.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use md5::{Digest, Md5};
use tokio::task::JoinSet;

use super::{Entry, Region, Run, WORK_DIR};
use crate::error::{InstallError, IoContext, Result};
use crate::fsutil::{ensure_free_space, sibling};
use crate::hash::hex;

/// Files at least this big are fetched in segments; smaller ones are not worth the extra requests.
pub(super) const SEGMENT_MIN_FILE: u64 = 32 * 1024 * 1024;
/// Size of one segment. Large enough to amortise a request, small enough to balance connections
/// and to lose little when one breaks.
const SEGMENT_SIZE: u64 = 8 * 1024 * 1024;
/// Connections used for segments across all files.
pub(super) const SEGMENTS_IN_FLIGHT: usize = 16;
const SEGMENT_ATTEMPTS: u32 = 6;
/// Bytes buffered before they are written (and hashed, for regions).
const FLUSH_BYTES: usize = 1024 * 1024;
/// Space demanded on top of the transfer itself.
const SPACE_MARGIN: u64 = 64 * 1024 * 1024;

/// The set of segments of one file that are already on disk, backed by a small append-only file.
///
/// Format: one header line naming the release file, then little-endian `u32` segment numbers.
/// Losing the tail (a crash while writing) only means fetching those segments again.
struct Ledger {
    done: Mutex<Vec<bool>>,
    file: Mutex<File>,
}

impl Ledger {
    fn open(path: &Path, header: &str, count: usize) -> Result<Self> {
        let mut done = vec![false; count];
        let mut valid = false;
        if let Ok(bytes) = fs::read(path)
            && let Some(newline) = bytes.iter().position(|&b| b == b'\n')
            && &bytes[..newline] == header.as_bytes()
        {
            valid = true;
            for record in bytes[newline + 1..].as_chunks::<4>().0 {
                let index = u32::from_le_bytes([record[0], record[1], record[2], record[3]]);
                if let Some(slot) = done.get_mut(index as usize) {
                    *slot = true;
                }
            }
        }
        let mut options = OpenOptions::new();
        options.create(true);
        let mut file = if valid {
            options.append(true)
        } else {
            options.write(true).truncate(true)
        }
        .open(path)
        .context(|| format!("opening {}", path.display()))?;
        if !valid {
            writeln!(file, "{header}").context(|| format!("writing {}", path.display()))?;
        }
        Ok(Self {
            done: Mutex::new(done),
            file: Mutex::new(file),
        })
    }

    fn is_done(&self, index: usize) -> bool {
        self.done.lock().unwrap_or_else(|p| p.into_inner())[index]
    }

    fn record(&self, index: usize) {
        self.done.lock().unwrap_or_else(|p| p.into_inner())[index] = true;
        let mut file = self.file.lock().unwrap_or_else(|p| p.into_inner());
        // Best effort: a lost record costs one segment of re-download.
        let _ = file.write_all(&(index as u32).to_le_bytes());
    }
}

fn segment_bounds(size: u64, index: usize) -> (u64, u64) {
    let start = index as u64 * SEGMENT_SIZE;
    (start, SEGMENT_SIZE.min(size - start))
}

impl Run {
    /// Download `entry` in parallel segments and move it into place.
    ///
    /// `Ok(false)`: the segmented path is not usable here (an older sequential `.part` exists, or
    /// the server ignores `Range`); the caller falls back to [`Run::fetch`] and nothing is left
    /// counted.
    pub(super) async fn fetch_segmented(
        self: &Arc<Self>,
        entry: &Entry,
        final_path: &Path,
    ) -> Result<bool> {
        let staged = self.game_dir.join(WORK_DIR).join(&entry.path);
        let part = sibling(&staged, ".part");
        let ledger_path = sibling(&staged, ".segs");

        // A `.part` without a ledger comes from the sequential downloader: it holds a usable
        // prefix, so let that path resume it.
        let has_ledger = ledger_path.exists();
        if !has_ledger && fs::metadata(&part).map(|m| m.len() > 0).unwrap_or(false) {
            return Ok(false);
        }

        let url = format!("{}/{}", self.base_url, entry.name);
        let count = entry.size.div_ceil(SEGMENT_SIZE) as usize;
        let header = format!(
            "kuro-segments v1 {} {} {}",
            entry.md5, entry.size, SEGMENT_SIZE
        );

        for fresh_attempt in [false, true] {
            self.ctx.cancel.check()?;
            if let Some(parent) = part.parent() {
                fs::create_dir_all(parent).context(|| format!("creating {}", parent.display()))?;
            }
            let (file, ledger, resumed) = {
                let (part, ledger_path, header, size) = (
                    part.clone(),
                    ledger_path.clone(),
                    header.clone(),
                    entry.size,
                );
                tokio::task::spawn_blocking(move || -> Result<_> {
                    let ledger = Ledger::open(&ledger_path, &header, count)?;
                    let resumed: u64 = (0..count)
                        .filter(|&k| ledger.is_done(k))
                        .map(|k| segment_bounds(size, k).1)
                        .sum();
                    let file = crate::sophon::local::prepare_file(&part, size)?;
                    Ok((file, ledger, resumed))
                })
                .await
                .map_err(|e| InstallError::Archive(format!("background task failed: {e}")))??
            };
            ensure_free_space(&part, entry.size - resumed + SPACE_MARGIN)?;
            let (file, ledger) = (Arc::new(file), Arc::new(ledger));

            // Bytes this file added to the shared counter, so a failure can take them back.
            let counted = Arc::new(AtomicU64::new(resumed));
            self.done.fetch_add(resumed, Ordering::Relaxed);

            let outcome = self
                .run_segments(entry, &url, &file, &part, &ledger, &counted, count)
                .await;
            match outcome {
                Ok(()) => {}
                Err(error) => {
                    self.rollback(&counted);
                    if matches!(&error, InstallError::InvalidData(m) if m.contains("ignores Range"))
                    {
                        tracing::info!(
                            "[kuro] {}: the server ignores Range; downloading sequentially",
                            entry.name
                        );
                        self.ranges_ok.store(false, Ordering::Relaxed);
                        let _ = fs::remove_file(&part);
                        let _ = fs::remove_file(&ledger_path);
                        return Ok(false);
                    }
                    return Err(error);
                }
            }

            // Everything is on disk: the md5 of the whole file decides. A few files are hashed at
            // a time and their progress is shown (see `Run::hash_whole_file`).
            let actual = self.hash_whole_file(&part, entry.size).await?;
            if actual == entry.md5 {
                drop(file);
                if let Some(parent) = final_path.parent() {
                    fs::create_dir_all(parent)
                        .context(|| format!("creating {}", parent.display()))?;
                }
                fs::rename(&part, final_path)
                    .context(|| format!("moving {} into place", final_path.display()))?;
                let _ = fs::remove_file(&ledger_path);
                if let Some(journal) = &self.journal {
                    journal.record(&entry.name);
                }
                self.files_fetched.fetch_add(1, Ordering::Relaxed);
                self.bytes_fetched.fetch_add(entry.size, Ordering::Relaxed);
                return Ok(true);
            }

            tracing::warn!(
                "[kuro] {}: md5 is {actual}, expected {}; {}",
                entry.name,
                entry.md5,
                if fresh_attempt {
                    "giving up"
                } else {
                    "downloading it again from scratch"
                }
            );
            self.rollback(&counted);
            drop(file);
            let _ = fs::remove_file(&part);
            let _ = fs::remove_file(&ledger_path);
            if fresh_attempt {
                return Err(InstallError::ChecksumMismatch {
                    file: entry.name.clone(),
                    expected: entry.md5.clone(),
                    actual,
                });
            }
        }
        unreachable!("the loop returns on its second pass")
    }

    fn rollback(&self, counted: &AtomicU64) {
        let amount = counted.swap(0, Ordering::Relaxed);
        self.done.fetch_sub(
            amount.min(self.done.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_segments(
        self: &Arc<Self>,
        entry: &Entry,
        url: &str,
        file: &Arc<File>,
        part: &Path,
        ledger: &Arc<Ledger>,
        counted: &Arc<AtomicU64>,
        count: usize,
    ) -> Result<()> {
        let mut set: JoinSet<Result<()>> = JoinSet::new();
        for k in 0..count {
            if ledger.is_done(k) {
                continue;
            }
            if self.stopped() {
                break;
            }
            let Ok(permit) = self
                .ctx
                .cancel
                .guard(self.segment_sem.clone().acquire_owned())
                .await
            else {
                break;
            };
            let Ok(permit) = permit else { break };
            let (run, url, file, part, ledger, counted) = (
                self.clone(),
                url.to_owned(),
                file.clone(),
                part.to_owned(),
                ledger.clone(),
                counted.clone(),
            );
            let size = entry.size;
            set.spawn(async move {
                let _permit = permit;
                run.fetch_segment(&url, size, k, &file, &part, &ledger, &counted)
                    .await
            });
            // A failed segment stops the file: reap finished tasks so the error surfaces early.
            while let Some(done) = set.try_join_next() {
                if let Some(error) = join_error(done) {
                    set.abort_all();
                    while set.join_next().await.is_some() {}
                    return Err(error);
                }
            }
        }
        if self.stopped() {
            set.abort_all();
        }
        let mut failure: Option<InstallError> = None;
        while let Some(done) = set.join_next().await {
            if let Some(error) = join_error(done)
                && failure.is_none()
            {
                set.abort_all();
                failure = Some(error);
            }
        }
        self.ctx.cancel.check()?;
        if let Some(error) = failure {
            return Err(error);
        }
        if self.stopped() {
            // Another file hit a fatal error: this one is not finished, do not verify it.
            return Err(InstallError::Cancelled);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_segment(
        &self,
        url: &str,
        size: u64,
        index: usize,
        file: &Arc<File>,
        part: &Path,
        ledger: &Ledger,
        counted: &AtomicU64,
    ) -> Result<()> {
        let (start, len) = segment_bounds(size, index);
        let sink = |n: u64| {
            counted.fetch_add(n, Ordering::Relaxed);
            self.done.fetch_add(n, Ordering::Relaxed);
            self.net.fetch_add(n, Ordering::Relaxed);
        };
        // What this segment already has on disk survives retries: each attempt asks only for the rest.
        let mut persisted = 0u64;
        let mut last = None;
        for attempt in 1..=SEGMENT_ATTEMPTS {
            self.ctx.cancel.check()?;
            self.fetching.fetch_add(1, Ordering::Relaxed);
            let result = self
                .stream_to_file(
                    url,
                    start,
                    len,
                    file,
                    part,
                    &mut persisted,
                    &mut None,
                    &sink,
                )
                .await;
            self.fetching.fetch_sub(1, Ordering::Relaxed);
            match result {
                Ok(()) => {
                    ledger.record(index);
                    return Ok(());
                }
                Err(error) if error.is_cancelled() => return Err(error),
                Err(error) => {
                    let retry = error.is_transient();
                    last = Some(error);
                    if !retry {
                        break;
                    }
                }
            }
            if attempt < SEGMENT_ATTEMPTS {
                let pause = Duration::from_millis((400 * u64::from(attempt)).min(3000));
                crate::http::sleep_unless_cancelled(pause, &self.ctx.cancel).await?;
            }
        }
        Err(last.expect("at least one attempt ran"))
    }

    /// Stream `len` bytes at `start` into `file` at the same offset, in [`FLUSH_BYTES`] writes.
    ///
    /// `persisted` counts the bytes already written for this range: a call with a non-zero value
    /// resumes after them, and it is kept up to date even when the transfer fails. `hasher`, when
    /// set, sees exactly the persisted bytes in order, so its state always matches `persisted`.
    /// `sink` is told about every flushed byte.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn stream_to_file(
        &self,
        url: &str,
        start: u64,
        len: u64,
        file: &Arc<File>,
        path: &Path,
        persisted: &mut u64,
        hasher: &mut Option<Md5>,
        sink: &(dyn Fn(u64) + Sync),
    ) -> Result<()> {
        if *persisted >= len {
            return Ok(());
        }
        let mut response = self
            .http
            .open_range(url, start + *persisted, len - *persisted, &self.ctx.cancel)
            .await?;
        let mut buffer: Vec<u8> = Vec::with_capacity(FLUSH_BYTES + 64 * 1024);
        let mut received = *persisted;

        let streamed: Result<()> = async {
            loop {
                let chunk = self.ctx.cancel.guard(response.chunk()).await??;
                let Some(chunk) = chunk else { return Ok(()) };
                received += chunk.len() as u64;
                if received > len {
                    return Err(InstallError::Network(format!(
                        "{url}: the server sent more than the {len} bytes asked for"
                    )));
                }
                buffer.extend_from_slice(&chunk);
                if buffer.len() >= FLUSH_BYTES {
                    flush(file, path, start, persisted, &mut buffer, hasher, sink).await?;
                }
            }
        }
        .await;

        // Keep what arrived even when the stream broke: the next attempt resumes after it.
        let flushed = flush(file, path, start, persisted, &mut buffer, hasher, sink).await;
        streamed?;
        flushed?;
        if *persisted != len {
            return Err(InstallError::Network(format!(
                "{url}: connection closed after {} of {len} bytes",
                *persisted
            )));
        }
        Ok(())
    }

    /// Bring one range of an existing file up to date, streaming it to disk and checking its md5.
    pub(super) async fn fetch_region_streamed(
        &self,
        url: &str,
        region: &Region,
        file: &Arc<File>,
        path: &Path,
    ) -> Result<u64> {
        let mut last = None;
        for attempt in 1..=super::REGION_ATTEMPTS {
            self.ctx.cancel.check()?;
            let mut persisted = 0u64;
            let mut hasher = Some(Md5::new());
            let sink = |n: u64| {
                self.done.fetch_add(n, Ordering::Relaxed);
                self.net.fetch_add(n, Ordering::Relaxed);
            };
            self.fetching.fetch_add(1, Ordering::Relaxed);
            let result = self
                .stream_to_file(
                    url,
                    region.start,
                    region.len,
                    file,
                    path,
                    &mut persisted,
                    &mut hasher,
                    &sink,
                )
                .await;
            self.fetching.fetch_sub(1, Ordering::Relaxed);

            let error = match result {
                Ok(()) => {
                    let actual = hex(&hasher.take().map(Md5::finalize).unwrap_or_default());
                    if actual == region.md5 {
                        self.bytes_fetched.fetch_add(region.len, Ordering::Relaxed);
                        self.regions_fetched.fetch_add(1, Ordering::Relaxed);
                        return Ok(region.len);
                    }
                    InstallError::ChecksumMismatch {
                        file: format!("{url} [{}..{}]", region.start, region.start + region.len),
                        expected: region.md5.clone(),
                        actual,
                    }
                }
                Err(error) => error,
            };
            // This attempt starts over from the first byte: take back what it counted.
            self.done.fetch_sub(
                persisted.min(self.done.load(Ordering::Relaxed)),
                Ordering::Relaxed,
            );
            if error.is_cancelled() {
                return Err(error);
            }
            let permanent = matches!(
                error,
                InstallError::InvalidData(_)
                    | InstallError::Io { .. }
                    | InstallError::NoSpace { .. }
            );
            last = Some(error);
            if permanent {
                break;
            }
            if attempt < super::REGION_ATTEMPTS {
                crate::http::sleep_unless_cancelled(
                    Duration::from_millis(400 * u64::from(attempt)),
                    &self.ctx.cancel,
                )
                .await?;
            }
        }
        Err(last.expect("at least one attempt ran"))
    }
}

/// Write the buffered bytes at their offset (hashing them first when asked) and account for them.
async fn flush(
    file: &Arc<File>,
    path: &Path,
    start: u64,
    persisted: &mut u64,
    buffer: &mut Vec<u8>,
    hasher: &mut Option<Md5>,
    sink: &(dyn Fn(u64) + Sync),
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let data = std::mem::take(buffer);
    let offset = start + *persisted;
    let (file, path, taken) = (file.clone(), path.to_owned(), hasher.take());
    let (mut data, taken, written) = tokio::task::spawn_blocking(move || {
        let mut taken = taken;
        if let Some(h) = taken.as_mut() {
            h.update(&data);
        }
        let written = crate::sophon::local::write_at(&file, offset, &data, &path);
        (data, taken, written)
    })
    .await
    .map_err(|e| InstallError::Archive(format!("background task failed: {e}")))?;
    *hasher = taken;
    written?;
    *persisted += data.len() as u64;
    sink(data.len() as u64);
    data.clear();
    *buffer = data;
    Ok(())
}

/// The error of a finished segment task, if it has one. Cancellation is not a failure to report.
fn join_error(
    done: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Option<InstallError> {
    match done {
        Ok(Ok(())) => None,
        Ok(Err(error)) if error.is_cancelled() => None,
        Ok(Err(error)) => Some(error),
        Err(error) if error.is_cancelled() => None,
        Err(error) => Some(InstallError::Archive(format!(
            "background task failed: {error}"
        ))),
    }
}
