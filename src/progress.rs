//! Cancellation and progress reporting shared by every operation.
//!
//! An operation receives a [`JobContext`]: a [`CancelToken`] it must poll and a [`Reporter`] it
//! calls with [`Progress`] snapshots. The UI side never has to know which backend is running.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::{InstallError, Result};

/// What the operation is doing right now. Ordered the way they normally happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Phase {
    /// Asking servers which version / files exist.
    Resolving,
    Downloading,
    /// Hashing downloaded data.
    Verifying,
    Extracting,
    /// Applying downloaded HDiff / LDiff patches over the installed files. Local disk work, not a
    /// transfer: it cannot be paused and stopping it half way leaves files of two versions side
    /// by side. [`Progress`] counts patched bytes here and [`Reporter::patch`] announces every
    /// single patch.
    Patching,
    /// Moving the finished result into place. Stopping here would leave things half applied, so
    /// callers should treat it as the point of no return (see [`Phase::is_critical`]).
    Finalizing,
}

impl Phase {
    pub fn is_critical(self) -> bool {
        matches!(self, Phase::Patching | Phase::Finalizing)
    }
}

/// A snapshot of an operation. `current` / `total` are bytes of the *current phase*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    pub phase: Phase,
    pub current: u64,
    /// `0` when unknown.
    pub total: u64,
    /// Smoothed network speed in bytes per second (only meaningful while downloading).
    pub speed: u64,
}

impl Progress {
    pub fn new(phase: Phase, current: u64, total: u64) -> Self {
        Self {
            phase,
            current,
            total,
            speed: 0,
        }
    }

    /// `0.0..=1.0`; `0.0` when the total is unknown.
    pub fn fraction(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.current as f64 / self.total as f64).min(1.0)
        }
    }
}

/// One patch of a [`Phase::Patching`] run, announced right before it is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PatchStep {
    /// 1-based position of the patch among the ones to apply.
    pub index: u32,
    pub total: u32,
    /// Size in bytes of the file this patch produces; what the patch weighs in the run.
    pub bytes: u64,
    /// Bytes of the patches announced before this one.
    pub done_bytes: u64,
    /// Bytes of every patch of the run.
    pub total_bytes: u64,
}

/// Cheap, cloneable stop flag. Backed by an `Arc<AtomicBool>` so the launcher can keep using the
/// flag it already owns ([`CancelToken::from_flag`]).
#[derive(Debug, Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_flag(flag: Arc<AtomicBool>) -> Self {
        Self(flag)
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// Completes once cancelled (polls every 50 ms). Race it against slow I/O with `select!`, or
    /// use [`CancelToken::guard`].
    pub async fn cancelled(&self) {
        while !self.is_cancelled() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Run `future`, giving up with [`InstallError::Cancelled`] as soon as the token is cancelled.
    /// Dropping the future closes sockets and aborts whatever it was waiting for.
    pub async fn guard<F: std::future::Future>(&self, future: F) -> Result<F::Output> {
        self.check()?;
        tokio::select! {
            output = future => Ok(output),
            () = self.cancelled() => Err(InstallError::Cancelled),
        }
    }

    /// `Err(Cancelled)` once cancelled; use with `?` between steps.
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(InstallError::Cancelled)
        } else {
            Ok(())
        }
    }
}

type Sink = dyn Fn(Progress) + Send + Sync;
type PatchSink = dyn Fn(PatchStep) + Send + Sync;

/// Delivers progress to a callback, at most ~10 times per second unless forced.
#[derive(Clone, Default)]
pub struct Reporter {
    sink: Option<Arc<Sink>>,
    patch_sink: Option<Arc<PatchSink>>,
    last: Arc<Mutex<Option<Instant>>>,
}

const MIN_INTERVAL: Duration = Duration::from_millis(100);

impl Reporter {
    pub fn new(sink: impl Fn(Progress) + Send + Sync + 'static) -> Self {
        Self {
            sink: Some(Arc::new(sink)),
            patch_sink: None,
            last: Arc::default(),
        }
    }

    /// Also deliver [`PatchStep`]s to `sink`.
    pub fn with_patch_sink(mut self, sink: impl Fn(PatchStep) + Send + Sync + 'static) -> Self {
        self.patch_sink = Some(Arc::new(sink));
        self
    }

    /// Announce the patch that is about to be applied. Never throttled: there is one per file.
    pub fn patch(&self, step: PatchStep) {
        if let Some(sink) = &self.patch_sink {
            sink(step);
        }
    }

    /// Throttled update, for hot loops.
    pub fn report(&self, progress: Progress) {
        let Some(sink) = &self.sink else { return };
        {
            let mut last = self.last.lock().unwrap_or_else(|p| p.into_inner());
            let now = Instant::now();
            if last.is_some_and(|at| now.duration_since(at) < MIN_INTERVAL) {
                return;
            }
            *last = Some(now);
        }
        sink(progress);
    }

    /// Always delivered: phase changes and final values.
    pub fn report_now(&self, progress: Progress) {
        let Some(sink) = &self.sink else { return };
        *self.last.lock().unwrap_or_else(|p| p.into_inner()) = Some(Instant::now());
        sink(progress);
    }
}

impl std::fmt::Debug for Reporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reporter")
            .field("attached", &self.sink.is_some())
            .finish()
    }
}

/// Everything an operation needs from its caller.
#[derive(Debug, Clone, Default)]
pub struct JobContext {
    pub cancel: CancelToken,
    pub reporter: Reporter,
}

impl JobContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cancel(mut self, cancel: CancelToken) -> Self {
        self.cancel = cancel;
        self
    }

    pub fn with_progress(mut self, sink: impl Fn(Progress) + Send + Sync + 'static) -> Self {
        // Keep a patch sink attached earlier: the order of the builders must not matter.
        let patch_sink = self.reporter.patch_sink.take();
        self.reporter = Reporter::new(sink);
        self.reporter.patch_sink = patch_sink;
        self
    }

    /// Receive one [`PatchStep`] per patch while patches are applied.
    pub fn with_patch_progress(mut self, sink: impl Fn(PatchStep) + Send + Sync + 'static) -> Self {
        self.reporter.patch_sink = Some(Arc::new(sink));
        self
    }
}

/// Smoothed bytes/second (exponential moving average over >= 250 ms windows).
#[derive(Debug)]
pub(crate) struct SpeedMeter {
    window_start: Instant,
    window_bytes: u64,
    speed: f64,
}

impl SpeedMeter {
    pub(crate) fn new() -> Self {
        Self {
            window_start: Instant::now(),
            window_bytes: 0,
            speed: 0.0,
        }
    }

    pub(crate) fn add(&mut self, bytes: u64) -> u64 {
        self.window_bytes += bytes;
        let elapsed = self.window_start.elapsed();
        if elapsed >= Duration::from_millis(250) {
            let instant = self.window_bytes as f64 / elapsed.as_secs_f64();
            self.speed = if self.speed == 0.0 {
                instant
            } else {
                0.3 * instant + 0.7 * self.speed
            };
            self.window_start = Instant::now();
            self.window_bytes = 0;
        }
        self.speed as u64
    }
}
