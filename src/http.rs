//! Shared HTTP client and small helpers (retrying GET of text documents).

use std::future::Future;
use std::time::Duration;

use crate::error::{InstallError, Result};
use crate::progress::CancelToken;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// A body that stops flowing for this long is considered dead (reset and retried).
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const TEXT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ATTEMPTS: u32 = 4;

/// Cloneable handle around one `reqwest::Client` (connection pool shared by every operation).
#[derive(Debug, Clone)]
pub struct Http {
    client: reqwest::Client,
}

impl Http {
    /// `user_agent` identifies the launcher to the servers (e.g. `BerryLauncher/0.2.0`).
    pub fn new(user_agent: impl AsRef<str>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(user_agent.as_ref())
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            // Big downloads over a shared HTTP/2 connection suffer from stream resets on several
            // CDNs; separate HTTP/1.1 connections are slower to open but dependable.
            .http1_only()
            // Segmented downloads keep a few dozen sockets busy against one CDN host: let the pool
            // keep them warm between requests and keep idle ones alive through NAT / firewalls.
            .pool_max_idle_per_host(32)
            .pool_idle_timeout(Duration::from_secs(60))
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .map_err(|e| InstallError::Network(format!("could not build the HTTP client: {e}")))?;
        Ok(Self { client })
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// GET a whole body into memory (manifests, chunks), retrying transient failures. Cancellation
    /// drops the request mid-flight. `max_len` rejects oversized answers instead of buffering them.
    pub async fn get_bytes(
        &self,
        url: &str,
        max_len: u64,
        cancel: &CancelToken,
    ) -> Result<Vec<u8>> {
        retry(cancel, || async {
            cancel
                .guard(async {
                    let response = self.client.get(url).send().await?;
                    let status = response.status();
                    if !status.is_success() {
                        return Err(InstallError::HttpStatus {
                            url: url.to_owned(),
                            status: status.as_u16(),
                        });
                    }
                    if response.content_length().is_some_and(|len| len > max_len) {
                        return Err(InstallError::InvalidData(format!(
                            "{url} is larger than {max_len} bytes"
                        )));
                    }
                    let body = response.bytes().await?;
                    if body.len() as u64 > max_len {
                        return Err(InstallError::InvalidData(format!(
                            "{url} is larger than {max_len} bytes"
                        )));
                    }
                    Ok(body.to_vec())
                })
                .await?
        })
        .await
    }

    /// GET exactly `len` bytes starting at `start` (HTTP `Range`). A server that ignores `Range`
    /// (answers 200 with the whole body) is an error, never silently buffered.
    pub async fn get_range(
        &self,
        url: &str,
        start: u64,
        len: u64,
        cancel: &CancelToken,
    ) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let last = start.saturating_add(len - 1);
        retry(cancel, || async {
            cancel
                .guard(async {
                    let response = self
                        .client
                        .get(url)
                        .header(reqwest::header::RANGE, format!("bytes={start}-{last}"))
                        .send()
                        .await?;
                    let status = response.status();
                    if status == reqwest::StatusCode::OK {
                        return Err(InstallError::InvalidData(format!(
                            "{url}: the server ignores Range requests"
                        )));
                    }
                    if status != reqwest::StatusCode::PARTIAL_CONTENT {
                        return Err(InstallError::HttpStatus {
                            url: url.to_owned(),
                            status: status.as_u16(),
                        });
                    }
                    let (got_start, _) = crate::download::parse_content_range(
                        response
                            .headers()
                            .get(reqwest::header::CONTENT_RANGE)
                            .and_then(|v| v.to_str().ok()),
                    );
                    if got_start != Some(start) {
                        return Err(InstallError::InvalidData(format!(
                            "{url}: asked for bytes from {start}, the server answered from {got_start:?}"
                        )));
                    }
                    if response.content_length().is_some_and(|l| l != len) {
                        return Err(InstallError::Network(format!("{url}: unexpected range length")));
                    }
                    let body = response.bytes().await?;
                    if body.len() as u64 != len {
                        return Err(InstallError::Network(format!(
                            "{url}: received {} of {len} bytes",
                            body.len()
                        )));
                    }
                    Ok(body.to_vec())
                })
                .await?
        })
        .await
    }

    /// Open a `Range` request for `len` bytes at `start` and hand back the response with its body
    /// still unread, so the caller can stream it to disk instead of buffering it. The status and
    /// `Content-Range` are validated exactly like [`Http::get_range`] does; a server that ignores
    /// `Range` is an error. No retry happens here: the caller decides how to resume.
    pub(crate) async fn open_range(
        &self,
        url: &str,
        start: u64,
        len: u64,
        cancel: &CancelToken,
    ) -> Result<reqwest::Response> {
        if len == 0 {
            return Err(InstallError::InvalidData("empty range requested".into()));
        }
        let last = start.saturating_add(len - 1);
        cancel
            .guard(async {
                let response = self
                    .client
                    .get(url)
                    .header(reqwest::header::RANGE, format!("bytes={start}-{last}"))
                    .send()
                    .await?;
                let status = response.status();
                if status == reqwest::StatusCode::OK {
                    return Err(InstallError::InvalidData(format!(
                        "{url}: the server ignores Range requests"
                    )));
                }
                if status != reqwest::StatusCode::PARTIAL_CONTENT {
                    return Err(InstallError::HttpStatus {
                        url: url.to_owned(),
                        status: status.as_u16(),
                    });
                }
                let (got_start, _) = crate::download::parse_content_range(
                    response
                        .headers()
                        .get(reqwest::header::CONTENT_RANGE)
                        .and_then(|v| v.to_str().ok()),
                );
                if got_start != Some(start) {
                    return Err(InstallError::InvalidData(format!(
                        "{url}: asked for bytes from {start}, the server answered from {got_start:?}"
                    )));
                }
                if response.content_length().is_some_and(|l| l != len) {
                    return Err(InstallError::Network(format!(
                        "{url}: unexpected range length"
                    )));
                }
                Ok(response)
            })
            .await?
    }

    /// GET a small text document, retrying transient failures.
    pub async fn get_text(&self, url: &str, cancel: &CancelToken) -> Result<String> {
        retry(cancel, || async {
            let response = self.client.get(url).timeout(TEXT_TIMEOUT).send().await?;
            let status = response.status();
            if !status.is_success() {
                return Err(InstallError::HttpStatus {
                    url: url.to_owned(),
                    status: status.as_u16(),
                });
            }
            Ok(response.text().await?)
        })
        .await
    }
}

/// Run `operation` up to [`MAX_ATTEMPTS`] times while it fails with a transient error, with an
/// increasing pause in between. Cancellation interrupts both the pause and further attempts.
pub(crate) async fn retry<T, F, Fut>(cancel: &CancelToken, mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempt = 1;
    loop {
        cancel.check()?;
        match operation().await {
            Err(error) if error.is_transient() && attempt < MAX_ATTEMPTS => {
                tracing::warn!("attempt {attempt}/{MAX_ATTEMPTS} failed ({error}); retrying");
                sleep_unless_cancelled(Duration::from_millis(500 * u64::from(attempt)), cancel)
                    .await?;
                attempt += 1;
            }
            other => return other,
        }
    }
}

pub(crate) async fn sleep_unless_cancelled(total: Duration, cancel: &CancelToken) -> Result<()> {
    let step = Duration::from_millis(50);
    let mut waited = Duration::ZERO;
    while waited < total {
        cancel.check()?;
        tokio::time::sleep(step).await;
        waited += step;
    }
    Ok(())
}
