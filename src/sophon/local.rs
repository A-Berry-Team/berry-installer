//! Blocking file helpers of the Sophon engine: hashing regions of existing files and writing
//! decoded chunks at their offsets. Run these inside `spawn_blocking`.

use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::Path;

use md5::{Digest, Md5};

use super::manifest::{Chunk, FileEntry};
use crate::error::{InstallError, IoContext, Result};
use crate::progress::CancelToken;

pub(crate) fn md5_hex(data: &[u8]) -> String {
    crate::hash::hex(&Md5::digest(data))
}

/// Open `path` for writing at exactly `size` bytes, creating it (and its folders) when missing.
/// A longer file is cut, a shorter one is extended with zeros, which never match a chunk hash by
/// accident unless the chunk really is zeros.
pub(crate) fn prepare_file(path: &Path, size: u64) -> Result<File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context(|| format!("creating {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .context(|| format!("opening {}", path.display()))?;
    let len = file
        .metadata()
        .context(|| format!("reading {}", path.display()))?
        .len();
    if len != size {
        file.set_len(size)
            .context(|| format!("resizing {}", path.display()))?;
    }
    Ok(file)
}

pub(crate) fn write_at(file: &File, offset: u64, data: &[u8], path: &Path) -> Result<()> {
    file.write_all_at(data, offset)
        .context(|| format!("writing {}", path.display()))
}

/// For every chunk of `entry`: does the file already hold exactly that chunk's data at its
/// offset? A missing or short file answers `false` for what it lacks. `on_chunk(index, ok)` runs
/// as the answers come in so progress can move during long hashing passes.
pub(super) fn inspect_chunks(
    path: &Path,
    entry: &FileEntry,
    cancel: &CancelToken,
    mut on_chunk: impl FnMut(usize, bool),
) -> Result<Vec<bool>> {
    let file = File::open(path).ok();
    let file_len = file
        .as_ref()
        .and_then(|f| f.metadata().ok())
        .map(|m| m.len())
        .unwrap_or(0);
    let mut buffer: Vec<u8> = Vec::new();
    let mut answers = Vec::with_capacity(entry.chunks.len());
    for (index, chunk) in entry.chunks.iter().enumerate() {
        cancel.check()?;
        let ok = match &file {
            Some(file) if chunk.offset.saturating_add(chunk.len) <= file_len => {
                region_matches(file, chunk, &mut buffer)
            }
            _ => false,
        };
        on_chunk(index, ok);
        answers.push(ok);
    }
    Ok(answers)
}

/// Read a chunk from an arbitrary installed-file location and verify its content hash.
pub(super) fn read_matching_chunk(path: &Path, chunk: &Chunk) -> Option<Vec<u8>> {
    if chunk.len > 256 * 1024 * 1024 {
        return None;
    }
    let file = File::open(path).ok()?;
    let file_len = file.metadata().ok()?.len();
    if chunk.offset.saturating_add(chunk.len) > file_len {
        return None;
    }
    let mut data = vec![0u8; chunk.len as usize];
    file.read_exact_at(&mut data, chunk.offset).ok()?;
    (md5_hex(&data) == chunk.md5).then_some(data)
}

fn region_matches(file: &File, chunk: &Chunk, buffer: &mut Vec<u8>) -> bool {
    // A manifest cannot make us allocate more than a chunk can reasonably be.
    if chunk.len > 256 * 1024 * 1024 {
        return false;
    }
    buffer.resize(chunk.len as usize, 0);
    file.read_exact_at(buffer, chunk.offset).is_ok() && md5_hex(buffer) == chunk.md5
}

/// md5 of a whole file (only needed when the manifest's chunks do not tile the file).
pub(super) fn file_md5(path: &Path, cancel: &CancelToken) -> Result<String> {
    let mut file = File::open(path).context(|| format!("opening {}", path.display()))?;
    let mut hasher = Md5::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        cancel.check()?;
        let n = file
            .read(&mut buffer)
            .context(|| format!("reading {}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(crate::hash::hex(&hasher.finalize()))
}

/// Decompress `compressed`, check it is exactly the chunk (length and md5) and return the data.
pub(super) fn decode_chunk(compressed: &[u8], chunk: &Chunk) -> Result<Vec<u8>> {
    let mut decoder = zstd::stream::Decoder::new(compressed)
        .map_err(|e| InstallError::InvalidData(format!("chunk {}: zstd: {e}", chunk.name)))?;
    // `take(len + 1)` so a decompression bomb stops one byte past the promised size.
    let mut data = Vec::with_capacity(chunk.len as usize);
    (&mut decoder)
        .take(chunk.len.saturating_add(1))
        .read_to_end(&mut data)
        .map_err(|e| InstallError::InvalidData(format!("chunk {}: zstd: {e}", chunk.name)))?;
    if data.len() as u64 != chunk.len {
        return Err(InstallError::InvalidData(format!(
            "chunk {} decompressed to {} bytes, manifest says {}",
            chunk.name,
            data.len(),
            chunk.len
        )));
    }
    let actual = md5_hex(&data);
    if actual != chunk.md5 {
        return Err(InstallError::ChecksumMismatch {
            file: chunk.name.clone(),
            expected: chunk.md5.clone(),
            actual,
        });
    }
    Ok(data)
}

/// The compressed bytes must be the ones the manifest promised.
pub(super) fn verify_compressed(bytes: &[u8], chunk: &Chunk) -> Result<()> {
    if chunk.compressed_len != 0 && bytes.len() as u64 != chunk.compressed_len {
        return Err(InstallError::InvalidData(format!(
            "chunk {} is {} bytes, manifest says {}",
            chunk.name,
            bytes.len(),
            chunk.compressed_len
        )));
    }
    if !chunk.compressed_md5.is_empty() {
        let actual = md5_hex(bytes);
        if actual != chunk.compressed_md5 {
            return Err(InstallError::ChecksumMismatch {
                file: chunk.name.clone(),
                expected: chunk.compressed_md5.clone(),
                actual,
            });
        }
    }
    Ok(())
}
