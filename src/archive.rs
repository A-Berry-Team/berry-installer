//! Archive extraction (tar.xz / tar.gz / tar.zst / tar / zip) in pure Rust: no `tar`, `unzip` or
//! `7z` binaries are needed, which matters inside the Flatpak sandbox.
//!
//! * Extraction is path-safe: entries that would land outside `dest` are skipped.
//! * Progress is reported as compressed bytes consumed (tar) or uncompressed bytes written (zip).
//! * Cancellation is polled on every read, so even a single huge entry stops promptly.

use std::fs::{self, File};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{InstallError, IoContext, Result};
use crate::fsutil::flatten_single_root;
use crate::progress::{CancelToken, JobContext, Phase, Progress, Reporter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveKind {
    Tar,
    TarGz,
    TarXz,
    TarZst,
    Zip,
}

impl ArchiveKind {
    /// Guess from the file name (`.tar.xz`, `.txz`, `.zip`...).
    pub fn from_name(name: &str) -> Option<Self> {
        let name = name.to_ascii_lowercase();
        let ends = |suffixes: &[&str]| suffixes.iter().any(|s| name.ends_with(s));
        if ends(&[".tar.xz", ".txz"]) {
            Some(Self::TarXz)
        } else if ends(&[".tar.gz", ".tgz"]) {
            Some(Self::TarGz)
        } else if ends(&[".tar.zst", ".tar.zstd", ".tzst"]) {
            Some(Self::TarZst)
        } else if ends(&[".tar"]) {
            Some(Self::Tar)
        } else if ends(&[".zip"]) {
            Some(Self::Zip)
        } else {
            None
        }
    }

    /// Guess from the first bytes, for files with a misleading name.
    pub fn sniff(path: &Path) -> Option<Self> {
        let mut head = [0u8; 6];
        File::open(path).ok()?.read_exact(&mut head).ok()?;
        match head {
            [0xFD, b'7', b'z', b'X', b'Z', 0] => Some(Self::TarXz),
            [0x1F, 0x8B, ..] => Some(Self::TarGz),
            [0x28, 0xB5, 0x2F, 0xFD, ..] => Some(Self::TarZst),
            [b'P', b'K', 3, 4, ..] => Some(Self::Zip),
            _ => None,
        }
    }

    pub fn detect(path: &Path) -> Option<Self> {
        path.file_name()
            .and_then(|n| n.to_str())
            .and_then(Self::from_name)
            .or_else(|| Self::sniff(path))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ExtractOptions {
    /// Archive wraps everything in one top-level folder: drop that folder. `dest` must be empty.
    pub strip_root: bool,
}

/// Extract `archive` into `dest` (created if needed). Blocking; use [`extract`] from async code.
pub fn extract_blocking(
    archive: &Path,
    dest: &Path,
    options: ExtractOptions,
    ctx: &JobContext,
) -> Result<()> {
    let kind = ArchiveKind::detect(archive)
        .ok_or_else(|| InstallError::UnsupportedArchive(archive.display().to_string()))?;
    fs::create_dir_all(dest).context(|| format!("creating {}", dest.display()))?;
    if options.strip_root
        && fs::read_dir(dest)
            .context(|| format!("reading {}", dest.display()))?
            .next()
            .is_some()
    {
        return Err(InstallError::InvalidData(format!(
            "{} must be empty to strip the archive root",
            dest.display()
        )));
    }
    ctx.cancel.check()?;
    ctx.reporter
        .report_now(Progress::new(Phase::Extracting, 0, 0));

    let result = match kind {
        ArchiveKind::Zip => extract_zip(archive, dest, ctx),
        tar_kind => extract_tar(archive, dest, tar_kind, ctx),
    };
    // A cancel surfaces from deep inside readers as a generic error; report it as what it is.
    if let Err(error) = result {
        return Err(if ctx.cancel.is_cancelled() {
            InstallError::Cancelled
        } else {
            error
        });
    }

    if options.strip_root {
        flatten_single_root(dest)?;
    }
    Ok(())
}

pub async fn extract(
    archive: &Path,
    dest: &Path,
    options: ExtractOptions,
    ctx: &JobContext,
) -> Result<()> {
    let (archive, dest, ctx) = (archive.to_owned(), dest.to_owned(), ctx.clone());
    tokio::task::spawn_blocking(move || extract_blocking(&archive, &dest, options, &ctx))
        .await
        .map_err(|e| InstallError::Archive(format!("extraction task failed: {e}")))?
}

/// Reader wrapper: counts bytes, reports progress and aborts reads once cancelled.
struct ProgressReader<R> {
    inner: R,
    cancel: CancelToken,
    reporter: Reporter,
    read: u64,
    total: u64,
}

impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.cancel.is_cancelled() {
            return Err(io::Error::other("cancelled"));
        }
        let n = self.inner.read(buf)?;
        self.read += n as u64;
        self.reporter
            .report(Progress::new(Phase::Extracting, self.read, self.total));
        Ok(n)
    }
}

fn extract_tar(archive: &Path, dest: &Path, kind: ArchiveKind, ctx: &JobContext) -> Result<()> {
    let file = File::open(archive).context(|| format!("opening {}", archive.display()))?;
    let total = file.metadata().map(|m| m.len()).unwrap_or(0);
    let counted = ProgressReader {
        inner: file,
        cancel: ctx.cancel.clone(),
        reporter: ctx.reporter.clone(),
        read: 0,
        total,
    };
    let buffered = BufReader::with_capacity(256 * 1024, counted);

    let reader: Box<dyn Read> = match kind {
        ArchiveKind::Tar => Box::new(buffered),
        ArchiveKind::TarGz => Box::new(flate2::read::GzDecoder::new(buffered)),
        ArchiveKind::TarXz => Box::new(liblzma::read::XzDecoder::new(buffered)),
        ArchiveKind::TarZst => Box::new(
            zstd::stream::read::Decoder::with_buffer(buffered)
                .map_err(|e| InstallError::Archive(format!("zstd: {e}")))?,
        ),
        ArchiveKind::Zip => unreachable!("zip is handled separately"),
    };

    let mut tar = tar::Archive::new(reader);
    tar.set_preserve_permissions(true);
    tar.set_unpack_xattrs(false);
    tar.set_overwrite(true);
    let entries = tar
        .entries()
        .map_err(|e| InstallError::Archive(e.to_string()))?;
    for entry in entries {
        ctx.cancel.check()?;
        let mut entry = entry.map_err(|e| InstallError::Archive(e.to_string()))?;
        let name = entry
            .path()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        let inside = entry
            .unpack_in(dest)
            .map_err(|e| InstallError::Archive(format!("{name}: {e}")))?;
        if !inside {
            tracing::warn!("skipped archive entry outside the destination: {name}");
        }
    }
    ctx.reporter
        .report_now(Progress::new(Phase::Extracting, total, total));
    Ok(())
}

fn extract_zip(archive: &Path, dest: &Path, ctx: &JobContext) -> Result<()> {
    let file = File::open(archive).context(|| format!("opening {}", archive.display()))?;
    let mut zip = zip::ZipArchive::new(BufReader::new(file))
        .map_err(|e| InstallError::Archive(e.to_string()))?;

    let mut total = 0u64;
    for i in 0..zip.len() {
        if let Ok(entry) = zip.by_index_raw(i) {
            total += entry.size();
        }
    }

    let mut done = 0u64;
    let mut buffer = vec![0u8; 256 * 1024];
    #[cfg(unix)]
    let mut dir_modes: Vec<(PathBuf, u32)> = Vec::new();
    for i in 0..zip.len() {
        ctx.cancel.check()?;
        let mut entry = zip
            .by_index(i)
            .map_err(|e| InstallError::Archive(e.to_string()))?;
        let entry_name = entry
            .name()
            .map(|name| name.into_owned())
            .unwrap_or_else(|_| format!("<invalid entry {i}>"));
        let Some(relative) = entry.enclosed_name() else {
            tracing::warn!("skipped zip entry with an unsafe path: {entry_name}");
            continue;
        };
        let out_path = dest.join(relative);
        let mode = entry.unix_mode();

        if entry.is_dir() {
            fs::create_dir_all(&out_path).context(|| format!("creating {}", out_path.display()))?;
            #[cfg(unix)]
            if let Some(mode) = mode {
                dir_modes.push((out_path, mode & 0o7777));
            }
            continue;
        }
        if entry.is_symlink() {
            tracing::warn!("skipped symbolic link in zip: {entry_name}");
            continue;
        }
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent).context(|| format!("creating {}", parent.display()))?;
        }
        let mut out =
            File::create(&out_path).context(|| format!("creating {}", out_path.display()))?;
        loop {
            ctx.cancel.check()?;
            let n = entry
                .read(&mut buffer)
                .map_err(|e| InstallError::Archive(format!("{entry_name}: {e}")))?;
            if n == 0 {
                break;
            }
            out.write_all(&buffer[..n])
                .context(|| format!("writing {}", out_path.display()))?;
            done += n as u64;
            ctx.reporter
                .report(Progress::new(Phase::Extracting, done, total));
        }
        drop(out);
        #[cfg(unix)]
        if let Some(mode) = mode {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&out_path, fs::Permissions::from_mode(mode & 0o7777));
        }
    }
    // Directory modes last: a read-only directory would block writing its children.
    #[cfg(unix)]
    for (path, mode) in dir_modes {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&path, fs::Permissions::from_mode(mode));
    }
    ctx.reporter
        .report_now(Progress::new(Phase::Extracting, total, total));
    Ok(())
}
