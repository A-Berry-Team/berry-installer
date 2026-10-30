//! Fetching one chunk: a single GET, then verify, decompress, verify again.
//!
//! Chunks are small (about a megabyte), so they are held in memory: no per-chunk temp files, no
//! `HEAD` round trips and nothing to clean up when the user pauses -- dropping the future is the
//! whole cancellation story.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use md5::{Digest, Md5};

use super::manifest::Chunk;
use crate::error::{InstallError, Result};
use crate::hash::hex;
use crate::http::{Http, sleep_unless_cancelled};
use crate::progress::CancelToken;

/// Attempts per chunk (network trouble, bad status or corrupt data each cost one).
const ATTEMPTS: u32 = 5;

/// Download, verify and decompress `chunk`. `received` counts every byte that arrives (retries
/// included) so the caller can show a real network speed.
pub(crate) async fn fetch_chunk(
    http: &Http,
    base: &str,
    chunk: &Chunk,
    cancel: &CancelToken,
    received: &Arc<AtomicU64>,
) -> Result<Vec<u8>> {
    let url = format!("{base}/{}", chunk.name);
    let mut attempt = 1;
    loop {
        cancel.check()?;
        let outcome = tokio::select! {
            result = attempt_once(http, &url, chunk, received) => result,
            () = cancel.cancelled() => return Err(InstallError::Cancelled),
        };
        match outcome {
            Ok(data) => return Ok(data),
            Err(error) if attempt < ATTEMPTS && retryable(&error) => {
                tracing::debug!("[sophon] chunk {} attempt {attempt}/{ATTEMPTS}: {error}", chunk.name);
                let pause = Duration::from_millis(400 * u64::from(attempt) + jitter_ms(&chunk.name, attempt));
                sleep_unless_cancelled(pause, cancel).await?;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

fn retryable(error: &InstallError) -> bool {
    error.is_transient() || matches!(error, InstallError::ChecksumMismatch { .. })
}

/// Spread retries of simultaneously failing chunks so they do not hit the CDN in lockstep.
fn jitter_ms(name: &str, attempt: u32) -> u64 {
    let seed = name.bytes().fold(u64::from(attempt), |acc, b| acc.wrapping_mul(31).wrapping_add(u64::from(b)));
    seed % 300
}

async fn attempt_once(http: &Http, url: &str, chunk: &Chunk, received: &Arc<AtomicU64>) -> Result<Vec<u8>> {
    let mut response = http.client().get(url).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(InstallError::HttpStatus { url: url.to_owned(), status: status.as_u16() });
    }
    let mut body = Vec::with_capacity(chunk.compressed_size as usize);
    // A server cannot make us buffer more than the manifest promised (plus slack for headers-less
    // framing differences); anything beyond is corrupt by definition.
    let limit = chunk.compressed_size.saturating_mul(2).saturating_add(4096) as usize;
    while let Some(piece) = response.chunk().await? {
        received.fetch_add(piece.len() as u64, Ordering::Relaxed);
        body.extend_from_slice(&piece);
        if body.len() > limit {
            return Err(InstallError::ChecksumMismatch {
                file: chunk.name.clone(),
                expected: format!("{} bytes", chunk.compressed_size),
                actual: format!("more than {} bytes", limit),
            });
        }
    }
    let chunk = chunk.clone();
    tokio::task::spawn_blocking(move || decode(&body, &chunk))
        .await
        .map_err(|e| InstallError::Archive(format!("decode task failed: {e}")))?
}

/// Verify the compressed bytes, decompress, verify the result.
pub(crate) fn decode(compressed: &[u8], chunk: &Chunk) -> Result<Vec<u8>> {
    let actual = hex(&Md5::digest(compressed));
    if actual != chunk.compressed_md5 {
        return Err(InstallError::ChecksumMismatch { file: chunk.name.clone(), expected: chunk.compressed_md5.clone(), actual });
    }
    let data = zstd::bulk::decompress(compressed, chunk.size as usize)
        .map_err(|e| InstallError::InvalidData(format!("chunk {} does not decompress: {e}", chunk.name)))?;
    let actual = hex(&Md5::digest(&data));
    if data.len() as u64 != chunk.size || actual != chunk.md5 {
        return Err(InstallError::ChecksumMismatch { file: chunk.name.clone(), expected: chunk.md5.clone(), actual });
    }
    Ok(data)
}
