//! Checksums: SHA-256 (Steam runtimes, extras) and MD5 (Sophon chunks, Kuro files).

use std::fs::File;
use std::io::Read;
use std::path::Path;

use md5::Md5;
use sha2::{Digest, Sha256};

use crate::error::{InstallError, IoContext, Result};
use crate::progress::{JobContext, Phase, Progress};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Checksum {
    /// Lower-case hex, 64 characters.
    Sha256(String),
    /// Lower-case hex, 32 characters. Only for servers that publish nothing stronger.
    Md5(String),
}

impl Checksum {
    pub fn sha256(hex: impl AsRef<str>) -> Result<Self> {
        let hex = hex.as_ref().trim().to_ascii_lowercase();
        if hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            Ok(Self::Sha256(hex))
        } else {
            Err(InstallError::InvalidData(format!(
                "not a SHA-256 digest: {hex:?}"
            )))
        }
    }

    pub fn md5(hex: impl AsRef<str>) -> Result<Self> {
        let hex = hex.as_ref().trim().to_ascii_lowercase();
        if hex.len() == 32 && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            Ok(Self::Md5(hex))
        } else {
            Err(InstallError::InvalidData(format!(
                "not an MD5 digest: {hex:?}"
            )))
        }
    }

    pub fn expected(&self) -> &str {
        match self {
            Self::Sha256(hex) | Self::Md5(hex) => hex,
        }
    }
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Hash a whole file, polling `cancel` and reporting `Verifying` progress. Blocking.
pub fn sha256_file(path: &Path, ctx: &JobContext) -> Result<String> {
    digest_file::<Sha256>(path, ctx)
}

/// MD5 counterpart of [`sha256_file`].
pub fn md5_file(path: &Path, ctx: &JobContext) -> Result<String> {
    digest_file::<Md5>(path, ctx)
}

fn digest_file<D: Digest>(path: &Path, ctx: &JobContext) -> Result<String> {
    let mut file = File::open(path).context(|| format!("opening {}", path.display()))?;
    let total = file.metadata().map(|m| m.len()).unwrap_or(0);
    let mut hasher = D::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut done = 0u64;
    loop {
        ctx.cancel.check()?;
        let read = file
            .read(&mut buffer)
            .context(|| format!("reading {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        done += read as u64;
        ctx.reporter
            .report(Progress::new(Phase::Verifying, done, total));
    }
    Ok(hex(&hasher.finalize()))
}

/// `Ok(())` when the file matches, [`InstallError::ChecksumMismatch`] otherwise. Blocking.
pub fn verify_file(path: &Path, checksum: &Checksum, ctx: &JobContext) -> Result<()> {
    let actual = match checksum {
        Checksum::Sha256(_) => sha256_file(path, ctx)?,
        Checksum::Md5(_) => md5_file(path, ctx)?,
    };
    if actual == checksum.expected() {
        Ok(())
    } else {
        Err(InstallError::ChecksumMismatch {
            file: path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
            expected: checksum.expected().to_owned(),
            actual,
        })
    }
}

/// Async wrapper that keeps hashing off the runtime threads.
pub async fn verify_file_async(path: &Path, checksum: &Checksum, ctx: &JobContext) -> Result<()> {
    ctx.reporter
        .report_now(Progress::new(Phase::Verifying, 0, 0));
    let (path, checksum, ctx) = (path.to_owned(), checksum.clone(), ctx.clone());
    tokio::task::spawn_blocking(move || verify_file(&path, &checksum, &ctx))
        .await
        .map_err(|e| InstallError::Archive(format!("hash task failed: {e}")))?
}

/// Parse a `sha256sum` style list (`<hex> *<file>` or `<hex>  <file>`) and return the digest of
/// `file_name`.
pub fn find_in_sums(sums: &str, file_name: &str) -> Option<Checksum> {
    sums.lines().find_map(|line| {
        let line = line.trim();
        let (digest, rest) = line.split_once(char::is_whitespace)?;
        let name = rest.trim_start().trim_start_matches('*');
        if name == file_name {
            Checksum::sha256(digest).ok()
        } else {
            None
        }
    })
}
