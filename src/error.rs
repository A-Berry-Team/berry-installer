//! One error type for the whole engine.
//!
//! Callers (berry-downloads, berry-runners, ...) mostly care about three questions: was it a user
//! cancel, is it worth retrying, and what do I show the user. The variants answer those without
//! string matching.

use std::io;
use std::path::PathBuf;

pub type Result<T, E = InstallError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum InstallError {
    /// The user (or the launcher) asked to stop. Never retried, never shown as a failure.
    #[error("operation cancelled")]
    Cancelled,

    /// Connection-level failure: DNS, TLS, reset, timeout, truncated body...
    #[error("network error: {0}")]
    Network(String),

    #[error("{url} answered HTTP {status}")]
    HttpStatus { url: String, status: u16 },

    #[error("checksum mismatch for {file}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        file: String,
        expected: String,
        actual: String,
    },

    #[error("not enough free space in {}: {} needed, {} available", path.display(), format_bytes(*needed), format_bytes(*available))]
    NoSpace {
        path: PathBuf,
        needed: u64,
        available: u64,
    },

    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: io::Error,
    },

    /// Some chunks could not be fetched or verified even after retrying. Nothing is marked
    /// complete; running the operation again resumes and only fetches what is still missing.
    #[error("{failed} chunk(s) could not be downloaded; first error: {first}")]
    ChunksFailed { failed: u64, first: String },

    #[error("unsupported archive: {0}")]
    UnsupportedArchive(String),

    #[error("could not read archive: {0}")]
    Archive(String),

    /// A server answered something we cannot use (empty version file, malformed checksum list...).
    #[error("unexpected data from the server: {0}")]
    InvalidData(String),

    /// An operation finished all it could but some pieces failed. Running it again retries only
    /// those: everything already verified on disk is kept.
    #[error("{0}")]
    Incomplete(String),

    #[error("{0} is not supported on this platform")]
    UnsupportedPlatform(String),
}

impl InstallError {
    pub fn io(context: impl Into<String>, source: io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    /// Worth trying again with the same request: network trouble, 5xx, 429 or 408.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Network(_) => true,
            Self::HttpStatus { status, .. } => *status >= 500 || *status == 429 || *status == 408,
            _ => false,
        }
    }
}

impl From<reqwest::Error> for InstallError {
    fn from(error: reqwest::Error) -> Self {
        // `reqwest` hides the real cause ("connection reset", "stream error"...) behind a generic
        // message such as "error decoding response body"; keep the whole `source()` chain.
        let mut text = error.to_string();
        let mut source = std::error::Error::source(&error);
        while let Some(inner) = source {
            text.push_str(": ");
            text.push_str(&inner.to_string());
            source = inner.source();
        }
        Self::Network(text)
    }
}

/// `result.context("creating X")` for `io::Result`, producing [`InstallError::Io`].
pub trait IoContext<T> {
    fn context(self, what: impl FnOnce() -> String) -> Result<T>;
}

impl<T> IoContext<T> for io::Result<T> {
    fn context(self, what: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|source| InstallError::io(what(), source))
    }
}

pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}
