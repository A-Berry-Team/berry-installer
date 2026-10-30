//! Resumable, verified single-file download.
//!
//! Behaviour (the best of fischl-rs' `AsyncDownloader` and AGL's `Downloader`):
//!
//! * data goes to `<dest>.part` and is renamed to `<dest>` only after the checksum matched, so a
//!   file at `dest` is always complete;
//! * an existing `.part` is resumed with a `Range` request (also across launcher restarts); a
//!   server that ignores `Range` just makes the transfer start over;
//! * transient failures (reset, timeout, 5xx, 429, truncated body) are retried in place, keeping
//!   what was already written;
//! * a checksum mismatch discards the partial data and downloads once more from scratch;
//! * cancellation is polled on every chunk; the `.part` stays so the next run resumes.

use std::path::{Path, PathBuf};

use reqwest::StatusCode;
use reqwest::header::{CONTENT_RANGE, RANGE};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::error::{InstallError, Result};
use crate::fsutil::{ensure_free_space, sibling};
use crate::hash::{Checksum, verify_file_async};
use crate::http::{Http, retry};
use crate::progress::{JobContext, Phase, Progress, SpeedMeter};

/// Extra bytes of room demanded on top of the transfer itself (marker files, temp files).
const SPACE_MARGIN: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct DownloadSpec {
    pub url: String,
    pub dest: PathBuf,
    /// When set the file is verified, and an already present `dest` that matches is reused.
    pub checksum: Option<Checksum>,
}

impl DownloadSpec {
    pub fn new(url: impl Into<String>, dest: impl Into<PathBuf>) -> Self {
        Self {
            url: url.into(),
            dest: dest.into(),
            checksum: None,
        }
    }

    pub fn with_checksum(mut self, checksum: Checksum) -> Self {
        self.checksum = Some(checksum);
        self
    }
}

/// Download `spec`, returning the size of the final file.
pub async fn download_file(http: &Http, spec: &DownloadSpec, ctx: &JobContext) -> Result<u64> {
    ctx.cancel.check()?;
    if let Some(parent) = spec.dest.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| InstallError::io(format!("creating {}", parent.display()), e))?;
    }

    // Already downloaded and still good?
    if let Some(checksum) = &spec.checksum
        && fs::try_exists(&spec.dest).await.unwrap_or(false)
    {
        match verify_file_async(&spec.dest, checksum, ctx).await {
            Ok(()) => return file_len(&spec.dest).await,
            Err(InstallError::ChecksumMismatch { .. }) => {
                tracing::warn!(
                    "{} is present but corrupt; downloading it again",
                    spec.dest.display()
                );
                let _ = fs::remove_file(&spec.dest).await;
            }
            Err(other) => return Err(other),
        }
    }

    let part = sibling(&spec.dest, ".part");
    let mut fresh_retry_used = false;
    loop {
        // Transient network errors are retried inside; keep the partial data between attempts.
        retry(&ctx.cancel, || transfer(http, &spec.url, &part, ctx)).await?;

        let Some(checksum) = &spec.checksum else {
            break;
        };
        match verify_file_async(&part, checksum, ctx).await {
            Ok(()) => break,
            Err(error @ InstallError::ChecksumMismatch { .. }) => {
                let _ = fs::remove_file(&part).await;
                if fresh_retry_used {
                    return Err(error);
                }
                fresh_retry_used = true;
                tracing::warn!("{error}; downloading {} again from scratch", spec.url);
            }
            Err(other) => return Err(other),
        }
    }

    ctx.cancel.check()?;
    fs::rename(&part, &spec.dest)
        .await
        .map_err(|e| InstallError::io(format!("moving {} into place", part.display()), e))?;
    file_len(&spec.dest).await
}

async fn file_len(path: &Path) -> Result<u64> {
    fs::metadata(path)
        .await
        .map(|m| m.len())
        .map_err(|e| InstallError::io(format!("reading {}", path.display()), e))
}

/// One attempt: (re)open the `.part`, request the missing range, stream it to disk.
async fn transfer(http: &Http, url: &str, part: &Path, ctx: &JobContext) -> Result<()> {
    ctx.cancel.check()?;
    let mut offset = match fs::metadata(part).await {
        Ok(meta) => meta.len(),
        Err(_) => 0,
    };

    let mut request = http.client().get(url);
    if offset > 0 {
        request = request.header(RANGE, format!("bytes={offset}-"));
    }
    let response = request.send().await?;
    let status = response.status();

    let total: u64 = match status {
        StatusCode::PARTIAL_CONTENT => {
            let (start, size) = parse_content_range(
                response
                    .headers()
                    .get(CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok()),
            );
            if start != Some(offset) {
                // The server resumed somewhere else; starting over is the only safe move.
                tracing::warn!("{url}: server resumed at {start:?}, expected {offset}; restarting");
                let _ = fs::remove_file(part).await;
                return Err(InstallError::Network("resume position mismatch".into()));
            }
            size.unwrap_or(offset + response.content_length().unwrap_or(0))
        }
        StatusCode::OK => {
            if offset > 0 {
                tracing::info!("{url}: server ignored the Range header; starting over");
                offset = 0;
            }
            response.content_length().unwrap_or(0)
        }
        StatusCode::RANGE_NOT_SATISFIABLE if offset > 0 => {
            // Our partial data is as long as (or longer than) the file. Hand it to the checksum
            // step: complete files pass, oversized garbage fails and is discarded.
            return Ok(());
        }
        other => {
            return Err(InstallError::HttpStatus {
                url: url.to_owned(),
                status: other.as_u16(),
            });
        }
    };

    let remaining = total.saturating_sub(offset);
    if remaining > 0 {
        ensure_free_space(part, remaining + SPACE_MARGIN)?;
    }

    let file = if offset == 0 {
        File::create(part).await
    } else {
        OpenOptions::new().append(true).open(part).await
    }
    .map_err(|e| InstallError::io(format!("opening {}", part.display()), e))?;
    let mut out = BufWriter::with_capacity(256 * 1024, file);

    let mut response = response;
    let mut written = offset;
    ctx.reporter.report_now(Progress {
        phase: Phase::Downloading,
        current: written,
        total,
        speed: 0,
    });

    let streamed = stream_body(&mut response, &mut out, part, &mut written, total, ctx).await;
    // Flush even when the stream failed or was cancelled: tokio's BufWriter does not do it on
    // drop, and the bytes already received are exactly what the next attempt resumes from.
    let flushed = out
        .flush()
        .await
        .map_err(|e| InstallError::io(format!("writing {}", part.display()), e));
    streamed?;
    flushed?;

    if total > 0 && written != total {
        return Err(InstallError::Network(format!(
            "connection closed after {written} of {total} bytes"
        )));
    }
    ctx.reporter.report_now(Progress {
        phase: Phase::Downloading,
        current: written,
        total: total.max(written),
        speed: 0,
    });
    Ok(())
}

async fn stream_body(
    response: &mut reqwest::Response,
    out: &mut BufWriter<File>,
    part: &Path,
    written: &mut u64,
    total: u64,
    ctx: &JobContext,
) -> Result<()> {
    let mut meter = SpeedMeter::new();
    while let Some(chunk) = response.chunk().await? {
        ctx.cancel.check()?;
        out.write_all(&chunk)
            .await
            .map_err(|e| InstallError::io(format!("writing {}", part.display()), e))?;
        *written += chunk.len() as u64;
        let speed = meter.add(chunk.len() as u64);
        ctx.reporter.report(Progress {
            phase: Phase::Downloading,
            current: *written,
            total,
            speed,
        });
    }
    Ok(())
}

/// `Content-Range: bytes 100-199/1000` -> `(Some(100), Some(1000))`. `*` total -> `None`.
pub(crate) fn parse_content_range(value: Option<&str>) -> (Option<u64>, Option<u64>) {
    let Some(rest) = value.and_then(|v| v.trim().strip_prefix("bytes ")) else {
        return (None, None);
    };
    let Some((range, size)) = rest.split_once('/') else {
        return (None, None);
    };
    let start = range
        .split_once('-')
        .and_then(|(s, _)| s.trim().parse().ok());
    (start, size.trim().parse().ok())
}
