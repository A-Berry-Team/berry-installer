//! HoYoverse LDiff manifest handling and HDiffPatch application.
//!
//! LDiff manifests describe files and one or more version-specific HDiff patches. Each
//! `PatchInfo` points at a byte range inside a downloaded patch bundle; the range is extracted
//! to a temporary file and passed to `berry-patcher` (HDIFF13/HDIFF19/SF20/W26-compatible patcher).
//!
//! The caller is responsible for downloading and verifying the patch bundle before calling
//! [`apply_manifest`]. Run this synchronous function on a blocking worker thread.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use berry_patcher::{HDiff, PatchOptions};
use md5::{Digest, Md5};
use prost::Message;

use crate::error::{InstallError, IoContext, Result};

static TEMP_ID: AtomicU64 = AtomicU64::new(0);
const HASH_BUFFER: usize = 1024 * 1024;

fn md5_hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Protobuf representation of the official HoYoverse `DiffManifest` (`manifest_ldiff.proto`).
#[derive(Clone, PartialEq, Message)]
pub struct DiffManifest {
    #[prost(message, repeated, tag = "1")]
    pub files: Vec<DiffFileInfo>,
    #[prost(message, repeated, tag = "2")]
    pub files_delete: Vec<DeleteFile>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DiffFileInfo {
    #[prost(string, tag = "1")]
    pub filename: String,
    #[prost(int32, tag = "2")]
    pub size: i32,
    #[prost(string, tag = "3")]
    pub hash: String,
    #[prost(message, repeated, tag = "4")]
    pub patches: Vec<Patch>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Patch {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub info: Option<PatchInfo>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PatchInfo {
    #[prost(string, tag = "1")]
    pub patch_id: String,
    #[prost(string, tag = "2")]
    pub tag: String,
    #[prost(string, tag = "3")]
    pub build_id: String,
    #[prost(int64, tag = "4")]
    pub patch_size: i64,
    #[prost(string, tag = "5")]
    pub patch_name: String,
    #[prost(int64, tag = "6")]
    pub patch_offset: i64,
    #[prost(int64, tag = "7")]
    pub patch_length: i64,
    #[prost(string, tag = "8")]
    pub original_name: String,
    #[prost(int64, tag = "9")]
    pub original_size: i64,
    #[prost(string, tag = "10")]
    pub original_hash: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteFile {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub info: Option<DeleteFiles>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteFiles {
    #[prost(message, repeated, tag = "1")]
    pub list: Vec<DeleteFileInfo>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteFileInfo {
    #[prost(string, tag = "1")]
    pub filename: String,
    #[prost(int64, tag = "2")]
    pub size: i64,
    #[prost(string, tag = "3")]
    pub hash: String,
}

/// Decode a raw LDiff protobuf manifest. The manifest is not a Sophon chunk manifest.
pub fn decode_manifest(raw: &[u8]) -> Result<DiffManifest> {
    // CDN manifests may be delivered as raw protobuf or zstd-compressed protobuf.
    let decoded;
    let bytes = if raw.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        decoded = zstd::stream::decode_all(raw).map_err(|e| {
            InstallError::InvalidData(format!("LDiff manifest is not valid zstd: {e}"))
        })?;
        decoded.as_slice()
    } else {
        raw
    };
    DiffManifest::decode(bytes).map_err(|e| {
        InstallError::InvalidData(format!("LDiff manifest is not valid protobuf: {e}"))
    })
}

/// Apply all patches in `manifest` matching `from_version`.
///
/// `patch_bundle` is the already-downloaded concatenated patch data file. Every selected patch
/// is sliced using its manifest offset/length, applied to the original file, and checked against
/// the target MD5 and size before the new file atomically replaces the old one.
pub fn apply_manifest(
    game_dir: &Path,
    patch_bundle: &Path,
    manifest: &DiffManifest,
    from_version: &str,
) -> Result<u64> {
    let bundle_len = fs::metadata(patch_bundle)
        .context(|| format!("stat patch bundle {}", patch_bundle.display()))?
        .len();
    let mut applied = 0u64;

    for file in &manifest.files {
        let Some(patch) = file.patches.iter().find(|p| p.key == from_version) else {
            continue;
        };
        let info = patch.info.as_ref().ok_or_else(|| {
            InstallError::InvalidData(format!(
                "LDiff patch for {} has no PatchInfo",
                file.filename
            ))
        })?;
        let source_rel = safe_relative(&info.original_name)?;
        let target_rel = safe_relative(&file.filename)?;
        let source = game_dir.join(source_rel);
        let target = game_dir.join(target_rel);

        let offset = u64::try_from(info.patch_offset).map_err(|_| {
            InstallError::InvalidData(format!("negative patch offset for {}", file.filename))
        })?;
        let length = u64::try_from(info.patch_length).map_err(|_| {
            InstallError::InvalidData(format!("negative patch length for {}", file.filename))
        })?;
        let end = offset.checked_add(length).ok_or_else(|| {
            InstallError::InvalidData(format!("patch range overflow for {}", file.filename))
        })?;
        if length == 0 || end > bundle_len {
            return Err(InstallError::InvalidData(format!(
                "patch range {offset}..{end} is outside {} ({} bytes)",
                patch_bundle.display(),
                bundle_len
            )));
        }

        if !source.is_file() {
            return Err(InstallError::InvalidData(format!(
                "LDiff source file is missing: {}",
                source.display()
            )));
        }
        if !info.original_hash.is_empty() {
            verify_md5(&source, &info.original_hash)?;
        }

        let parent = target.parent().ok_or_else(|| {
            InstallError::InvalidData(format!("target has no parent: {}", target.display()))
        })?;
        fs::create_dir_all(parent).context(|| format!("creating {}", parent.display()))?;
        let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let stem = target.file_name().unwrap_or_default().to_string_lossy();
        let pid = std::process::id();
        let patch_temp = parent.join(format!(".{stem}.ldiff-{pid}-{id}.hdiff"));
        let output_temp = parent.join(format!(".{stem}.ldiff-{pid}-{id}.part"));

        let result = (|| {
            copy_range(patch_bundle, &patch_temp, offset, length)?;
            apply_hdiff(&source, &patch_temp, &output_temp)?;
            let expected_size = u64::try_from(file.size).map_err(|_| {
                InstallError::InvalidData(format!("negative target size for {}", file.filename))
            })?;
            let actual_size = fs::metadata(&output_temp)
                .context(|| format!("stat patched file {}", output_temp.display()))?
                .len();
            if actual_size != expected_size {
                return Err(InstallError::InvalidData(format!(
                    "patched file {} has size {actual_size}, expected {expected_size}",
                    file.filename
                )));
            }
            verify_md5(&output_temp, &file.hash)?;
            fs::rename(&output_temp, &target)
                .context(|| format!("atomically replacing {}", target.display()))?;
            Ok(())
        })();
        let _ = fs::remove_file(&patch_temp);
        let _ = fs::remove_file(&output_temp);
        result?;
        applied += 1;
    }

    // Apply deletions only after every selected patch succeeded. A partial update must never
    // remove files that may still be needed by the old installation.
    for deletion in manifest
        .files_delete
        .iter()
        .filter(|entry| entry.key == from_version)
    {
        let Some(info) = deletion.info.as_ref() else {
            continue;
        };
        for item in &info.list {
            let relative = safe_relative(&item.filename)?;
            let path = game_dir.join(relative);
            if !path.is_file() {
                continue;
            }
            if !item.hash.is_empty() {
                verify_md5(&path, &item.hash)?;
            }
            fs::remove_file(&path)
                .context(|| format!("removing obsolete file {}", path.display()))?;
        }
    }
    Ok(applied)
}

/// Apply one HDiff patch through `berry-patcher`.
///
/// `berry-patcher` writes single-file output to `<output>.berry-tmp` and renames it over `output`
/// only after the patch (and any digest it carries) verified, so a failure never leaves a
/// half-written `output`. A diff that names a checksum this build cannot compute is refused rather
/// than applied unverified.
pub fn apply_hdiff(source: &Path, patch: &Path, output: &Path) -> Result<()> {
    HDiff::new(source, patch, output)
        .with_options(PatchOptions::auto())
        .try_apply()
        .map_err(|error| {
            InstallError::Archive(format!(
                "berry-patcher could not apply {} to {}: {error}",
                patch.display(),
                source.display()
            ))
        })
}

fn copy_range(source: &Path, target: &Path, offset: u64, length: u64) -> Result<()> {
    let mut input = File::open(source).context(|| format!("opening {}", source.display()))?;
    input
        .seek(SeekFrom::Start(offset))
        .context(|| format!("seeking {}", source.display()))?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)
        .context(|| format!("creating {}", target.display()))?;
    let copied = std::io::copy(&mut input.take(length), &mut output)
        .context(|| format!("extracting patch range from {}", source.display()))?;
    if copied != length {
        return Err(InstallError::InvalidData(format!(
            "truncated patch range: copied {copied} of {length} bytes"
        )));
    }
    output
        .flush()
        .context(|| format!("flushing {}", target.display()))?;
    output
        .sync_all()
        .context(|| format!("syncing {}", target.display()))?;
    Ok(())
}

fn verify_md5(path: &Path, expected: &str) -> Result<()> {
    if expected.len() != 32 || !expected.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(InstallError::InvalidData(format!(
            "invalid MD5 in LDiff manifest: {expected:?}"
        )));
    }
    let mut file = File::open(path).context(|| format!("opening {}", path.display()))?;
    let mut hasher = Md5::new();
    let mut buffer = vec![0u8; HASH_BUFFER];
    loop {
        let count = file
            .read(&mut buffer)
            .context(|| format!("reading {}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let actual = md5_hex(hasher.finalize());
    if actual.eq_ignore_ascii_case(expected) {
        Ok(())
    } else {
        Err(InstallError::ChecksumMismatch {
            file: path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned(),
            expected: expected.to_ascii_lowercase(),
            actual,
        })
    }
}

fn safe_relative(name: &str) -> Result<PathBuf> {
    let path = Path::new(name);
    let valid = !name.is_empty()
        && !name.contains(['\n', '\0'])
        && path
            .components()
            .all(|part| matches!(part, Component::Normal(_)));
    if valid {
        Ok(path.to_path_buf())
    } else {
        Err(InstallError::InvalidData(format!(
            "unsafe path in LDiff manifest: {name:?}"
        )))
    }
}

/// Summary of a LDiff pre-download. Patch ranges are kept in the preload cache so the
/// later update can apply them without contacting the CDN again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LdiffDownloadOutcome {
    pub patches_total: u64,
    pub patches_downloaded: u64,
    pub patches_reused: u64,
    pub bytes_downloaded: u64,
    pub bytes_reused: u64,
}

/// Download only the ranges referenced by the selected installed version in a LDiff manifest.
/// The CDN stores several HDiff payloads inside a shared resource named by `patch_id`; each
/// payload is retrieved with HTTP Range (`patch_offset..patch_offset + patch_length`).
///
/// Nothing in the game directory is modified: the manifest and verified-length patch ranges are
/// stored under `.berry-preload/ldiff` and can be applied later with [`apply_cached_manifest`].
pub async fn preload_manifest(
    http: &crate::http::Http,
    manifest_url: &str,
    patch_base_url: &str,
    game_dir: &Path,
    from_version: &str,
    ctx: &crate::progress::JobContext,
) -> Result<LdiffDownloadOutcome> {
    use crate::progress::{Phase, Progress};
    use std::collections::HashSet;
    use tokio::task::JoinSet;

    ctx.cancel.check()?;
    if !manifest_url.starts_with("https://") || !patch_base_url.starts_with("https://") {
        return Err(InstallError::InvalidData(
            "LDiff manifest and patch resources must use HTTPS".into(),
        ));
    }
    ctx.reporter
        .report_now(Progress::new(Phase::Resolving, 0, 0));
    let raw = http
        .get_bytes(manifest_url, 128 * 1024 * 1024, &ctx.cancel)
        .await?;
    let manifest = decode_manifest(&raw)?;

    let cache_dir = game_dir.join(".berry-preload").join("ldiff");
    tokio::fs::create_dir_all(&cache_dir)
        .await
        .map_err(|e| InstallError::io(format!("creating {}", cache_dir.display()), e))?;

    // Keep the exact manifest used for the preload. Its name is stable per manifest URL and
    // makes later application independent of repository metadata changing between sessions.
    let manifest_key = md5_hex(Md5::digest(manifest_url.as_bytes()));
    let manifest_path = cache_dir.join(format!("manifest-{manifest_key}.pb"));
    let manifest_temp = cache_dir.join(format!(
        ".manifest-{manifest_key}.{}.part",
        TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    tokio::fs::write(&manifest_temp, &raw)
        .await
        .map_err(|e| InstallError::io(format!("writing {}", manifest_temp.display()), e))?;
    tokio::fs::rename(&manifest_temp, &manifest_path)
        .await
        .map_err(|e| InstallError::io(format!("publishing {}", manifest_path.display()), e))?;

    let mut ranges = Vec::new();
    let mut seen = HashSet::new();
    let mut bytes_reused = 0u64;
    for file in &manifest.files {
        let Some(patch) = file.patches.iter().find(|patch| patch.key == from_version) else {
            continue;
        };
        let info = patch.info.as_ref().ok_or_else(|| {
            InstallError::InvalidData(format!(
                "LDiff patch for {} has no PatchInfo",
                file.filename
            ))
        })?;
        if info.patch_id.is_empty()
            || !info
                .patch_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(InstallError::InvalidData(format!(
                "invalid LDiff patch id for {}",
                file.filename
            )));
        }
        let offset = u64::try_from(info.patch_offset).map_err(|_| {
            InstallError::InvalidData(format!("negative LDiff patch offset for {}", file.filename))
        })?;
        let length = u64::try_from(info.patch_length).map_err(|_| {
            InstallError::InvalidData(format!("negative LDiff patch length for {}", file.filename))
        })?;
        let end = offset.checked_add(length).ok_or_else(|| {
            InstallError::InvalidData(format!("LDiff patch range overflow for {}", file.filename))
        })?;
        let bundle_size = u64::try_from(info.patch_size).map_err(|_| {
            InstallError::InvalidData(format!("negative LDiff bundle size for {}", file.filename))
        })?;
        if length == 0 || (bundle_size > 0 && end > bundle_size) {
            return Err(InstallError::InvalidData(format!(
                "invalid LDiff patch range for {}",
                file.filename
            )));
        }

        let cache_path = cached_patch_path(&cache_dir, info)?;
        if !seen.insert(cache_path.clone()) {
            continue;
        }
        match tokio::fs::metadata(&cache_path).await {
            Ok(metadata) if metadata.len() == length => {
                bytes_reused = bytes_reused.saturating_add(length);
            }
            Ok(_) => {
                let _ = tokio::fs::remove_file(&cache_path).await;
                ranges.push((info.patch_id.clone(), offset, length, cache_path));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                ranges.push((info.patch_id.clone(), offset, length, cache_path));
            }
            Err(error) => {
                return Err(InstallError::io(
                    format!("stat {}", cache_path.display()),
                    error,
                ));
            }
        }
    }

    let total = ranges.iter().map(|(_, _, length, _)| *length).sum::<u64>();
    let patches_total = seen.len() as u64;
    let mut outcome = LdiffDownloadOutcome {
        patches_total,
        patches_reused: patches_total.saturating_sub(ranges.len() as u64),
        bytes_reused,
        ..LdiffDownloadOutcome::default()
    };
    ctx.reporter
        .report_now(Progress::new(Phase::Downloading, 0, total));

    const PARALLEL_RANGES: usize = 12;
    let mut tasks = JoinSet::new();
    let mut completed = 0u64;
    for (patch_id, offset, length, cache_path) in ranges {
        ctx.cancel.check()?;
        while tasks.len() >= PARALLEL_RANGES {
            let result = tasks.join_next().await.ok_or_else(|| {
                InstallError::InvalidData("LDiff download task queue ended unexpectedly".into())
            })?;
            let bytes = result.map_err(|e| {
                InstallError::InvalidData(format!("LDiff download task failed: {e}"))
            })??;
            completed = completed.saturating_add(bytes);
            ctx.reporter
                .report(Progress::new(Phase::Downloading, completed, total));
            outcome.patches_downloaded += 1;
            outcome.bytes_downloaded = outcome.bytes_downloaded.saturating_add(bytes);
        }

        let http = http.clone();
        let cancel = ctx.cancel.clone();
        let base = patch_base_url.trim_end_matches('/').to_owned();
        tasks.spawn(async move {
            let url = format!("{base}/{patch_id}");
            let bytes = http.get_range(&url, offset, length, &cancel).await?;
            if bytes.len() as u64 != length {
                return Err(InstallError::InvalidData(format!(
                    "{url}: received {} bytes, expected {length}",
                    bytes.len()
                )));
            }
            let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let temp = cache_path.with_extension(format!("part-{id}"));
            tokio::fs::write(&temp, &bytes)
                .await
                .map_err(|e| InstallError::io(format!("writing {}", temp.display()), e))?;
            tokio::fs::rename(&temp, &cache_path)
                .await
                .map_err(|e| InstallError::io(format!("publishing {}", cache_path.display()), e))?;
            Ok::<u64, InstallError>(length)
        });
    }
    while let Some(result) = tasks.join_next().await {
        let bytes = result
            .map_err(|e| InstallError::InvalidData(format!("LDiff download task failed: {e}")))??;
        completed = completed.saturating_add(bytes);
        ctx.reporter
            .report(Progress::new(Phase::Downloading, completed, total));
        outcome.patches_downloaded += 1;
        outcome.bytes_downloaded = outcome.bytes_downloaded.saturating_add(bytes);
    }
    ctx.reporter
        .report_now(Progress::new(Phase::Downloading, total, total));
    ctx.reporter.report_now(Progress::new(
        Phase::Verifying,
        patches_total,
        patches_total,
    ));
    Ok(outcome)
}

/// Apply a previously downloaded LDiff preload. Every patch range is read from the persistent
/// cache created by [`preload_manifest`], verified against the source and target file hashes,
/// and atomically published. This is deliberately separate from pre-download so the launcher
/// never replaces live game files with a not-yet-released version before the user updates.
pub fn apply_cached_manifest(
    game_dir: &Path,
    cache_dir: &Path,
    manifest: &DiffManifest,
    from_version: &str,
) -> Result<u64> {
    let mut applied = 0u64;
    for file in &manifest.files {
        let Some(patch) = file.patches.iter().find(|patch| patch.key == from_version) else {
            continue;
        };
        let info = patch.info.as_ref().ok_or_else(|| {
            InstallError::InvalidData(format!(
                "LDiff patch for {} has no PatchInfo",
                file.filename
            ))
        })?;
        let patch_path = cached_patch_path(cache_dir, info)?;
        let target = game_dir.join(safe_relative(&file.filename)?);
        let expected_target_size = u64::try_from(file.size).map_err(|_| {
            InstallError::InvalidData(format!("negative target size for {}", file.filename))
        })?;
        // Make interrupted multi-file application resumable: files already replaced by a previous
        // attempt are accepted only when both their target size and target MD5 match.
        if target.is_file()
            && fs::metadata(&target).is_ok_and(|metadata| metadata.len() == expected_target_size)
            && verify_md5(&target, &file.hash).is_ok()
        {
            continue;
        }
        let expected_patch_len = u64::try_from(info.patch_length).map_err(|_| {
            InstallError::InvalidData(format!("negative patch length for {}", file.filename))
        })?;
        let actual_patch_len = fs::metadata(&patch_path)
            .context(|| format!("stat cached patch {}", patch_path.display()))?
            .len();
        if actual_patch_len != expected_patch_len {
            return Err(InstallError::InvalidData(format!(
                "cached LDiff patch {} has {actual_patch_len} bytes, expected {expected_patch_len}",
                patch_path.display()
            )));
        }

        let source = if info.original_name.is_empty() {
            None
        } else {
            Some(game_dir.join(safe_relative(&info.original_name)?))
        };
        if let Some(source) = &source {
            if !source.is_file() {
                return Err(InstallError::InvalidData(format!(
                    "LDiff source file is missing: {}",
                    source.display()
                )));
            }
            if !info.original_hash.is_empty() {
                verify_md5(source, &info.original_hash)?;
            }
        }
        let parent = target.parent().ok_or_else(|| {
            InstallError::InvalidData(format!("target has no parent: {}", target.display()))
        })?;
        fs::create_dir_all(parent).context(|| format!("creating {}", parent.display()))?;
        let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let stem = target.file_name().unwrap_or_default().to_string_lossy();
        let pid = std::process::id();
        let output_temp = parent.join(format!(".{stem}.ldiff-{pid}-{id}.part"));
        let result = (|| {
            if let Some(source) = &source {
                apply_hdiff(source, &patch_path, &output_temp)?;
            } else {
                fs::copy(&patch_path, &output_temp)
                    .context(|| format!("copying new LDiff file {}", file.filename))?;
            }
            let expected_size = expected_target_size;
            let actual_size = fs::metadata(&output_temp)
                .context(|| format!("stat patched file {}", output_temp.display()))?
                .len();
            if actual_size != expected_size {
                return Err(InstallError::InvalidData(format!(
                    "patched file {} has size {actual_size}, expected {expected_size}",
                    file.filename
                )));
            }
            verify_md5(&output_temp, &file.hash)?;
            fs::rename(&output_temp, &target)
                .context(|| format!("atomically replacing {}", target.display()))?;
            Ok(())
        })();
        let _ = fs::remove_file(&output_temp);
        if result.is_err() {
            // The range has no standalone checksum in the LDiff manifest. If HDiff application
            // or the resulting file verification fails, discard this cached range so a retry
            // fetches fresh bytes instead of reusing a same-length corrupt patch forever.
            let _ = fs::remove_file(&patch_path);
        }
        result?;
        applied += 1;
    }

    // Deletions are delayed until all file patches have succeeded.
    for deletion in manifest
        .files_delete
        .iter()
        .filter(|entry| entry.key == from_version)
    {
        let Some(info) = deletion.info.as_ref() else {
            continue;
        };
        for item in &info.list {
            let path = game_dir.join(safe_relative(&item.filename)?);
            if !path.is_file() {
                continue;
            }
            if !item.hash.is_empty() {
                verify_md5(&path, &item.hash)?;
            }
            fs::remove_file(&path)
                .context(|| format!("removing obsolete file {}", path.display()))?;
        }
    }
    Ok(applied)
}

fn cached_patch_path(cache_dir: &Path, info: &PatchInfo) -> Result<PathBuf> {
    if info.patch_id.is_empty()
        || !info
            .patch_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(InstallError::InvalidData(format!(
            "invalid LDiff patch id {:?}",
            info.patch_id
        )));
    }
    let offset = u64::try_from(info.patch_offset).map_err(|_| {
        InstallError::InvalidData(format!("negative patch offset for {}", info.patch_id))
    })?;
    let length = u64::try_from(info.patch_length).map_err(|_| {
        InstallError::InvalidData(format!("negative patch length for {}", info.patch_id))
    })?;
    Ok(cache_dir.join(format!("{}-{offset}-{length}.hdiff", info.patch_id)))
}

/// Apply the cached preload manifest identified by its original CDN URL.
pub fn apply_cached_manifest_for_url(
    game_dir: &Path,
    manifest_url: &str,
    from_version: &str,
) -> Result<u64> {
    let cache_dir = game_dir.join(".berry-preload").join("ldiff");
    let manifest_key = md5_hex(Md5::digest(manifest_url.as_bytes()));
    let manifest_path = cache_dir.join(format!("manifest-{manifest_key}.pb"));
    let raw = fs::read(&manifest_path)
        .context(|| format!("reading cached LDiff manifest {}", manifest_path.display()))?;
    let manifest = decode_manifest(&raw)?;
    apply_cached_manifest(game_dir, &cache_dir, &manifest, from_version)
}

/// Whether a completed LDiff preload is available for a later update.
/// The manifest is published only after it has been fully written; the corresponding
/// patch ranges are individually published after their downloads complete.
pub fn has_cached_preload(game_dir: &Path) -> bool {
    game_dir
        .join(".berry-preload")
        .join("ldiff")
        .join(".complete")
        .is_file()
}

/// Mark the LDiff preload as complete only after every preload job has succeeded.
pub fn mark_preload_complete(game_dir: &Path) -> Result<()> {
    let cache_dir = game_dir.join(".berry-preload").join("ldiff");
    if !cache_dir.is_dir() {
        return Ok(());
    }
    let marker = cache_dir.join(".complete");
    let temp = cache_dir.join(format!(
        ".complete-{}.part",
        TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&temp, b"berry-ldiff-preload-v1")
        .context(|| format!("writing {}", temp.display()))?;
    fs::rename(&temp, &marker).context(|| format!("publishing {}", marker.display()))?;
    Ok(())
}

/// Remove the cached LDiff manifest and patch ranges after a successful live update.
pub fn clear_preload_cache(game_dir: &Path) -> Result<()> {
    let cache_dir = game_dir.join(".berry-preload").join("ldiff");
    match fs::remove_dir_all(&cache_dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(InstallError::io(
            format!("removing {}", cache_dir.display()),
            error,
        )),
    }
}
