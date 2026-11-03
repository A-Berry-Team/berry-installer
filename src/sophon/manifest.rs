//! Fetching, decoding and validating a Sophon manifest, and the copy kept on disk between runs.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

use prost::Message;

use super::proto::{FileChunk, SophonManifest, TYPE_DIRECTORY};
use crate::error::{InstallError, IoContext, Result};
use crate::fsutil::atomic_write;
use crate::http::Http;
use crate::progress::{JobContext, Phase, Progress};

/// Manifests are a few MB at most (tens of thousands of files); anything bigger is not one.
const MAX_MANIFEST_BYTES: u64 = 256 * 1024 * 1024;

/// Where the manifest of what is installed is kept, so the next update only has to look at files
/// whose entry changed. One file per slot (`game`, `en-us`...).
pub const STORED_DIR: &str = ".berry-sophon";

#[derive(Debug, Clone)]
pub struct Chunk {
    pub name: String,
    /// Where the decompressed data goes in the file.
    pub offset: u64,
    /// Decompressed length.
    pub len: u64,
    /// Bytes on the wire.
    pub compressed_len: u64,
    /// md5 (lower-case hex) of the compressed bytes.
    pub compressed_md5: String,
    /// md5 (lower-case hex) of the decompressed bytes.
    pub md5: String,
}

impl From<&FileChunk> for Chunk {
    fn from(c: &FileChunk) -> Self {
        Self {
            name: c.chunk_name.clone(),
            offset: c.chunk_on_file_offset,
            len: c.chunk_decompressed_size,
            compressed_len: c.chunk_size,
            compressed_md5: c.chunk_md5.to_ascii_lowercase(),
            md5: c.chunk_decompressed_md5.to_ascii_lowercase(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub name: String,
    pub path: PathBuf,
    pub size: u64,
    pub md5: String,
    pub chunks: Vec<Chunk>,
    /// The chunks tile the file exactly (contiguous, no gaps, sizes add up), so verifying every
    /// chunk verifies the whole file and no separate whole-file hash is needed.
    pub covered: bool,
}

impl FileEntry {
    pub fn compressed_size(&self) -> u64 {
        self.chunks.iter().map(|c| c.compressed_len).sum()
    }

    /// Same content as `other` according to the manifests (name aside).
    pub fn same_content(&self, other: &FileEntry) -> bool {
        self.size == other.size
            && self.md5 == other.md5
            && self.chunks.len() == other.chunks.len()
            && self
                .chunks
                .iter()
                .zip(&other.chunks)
                .all(|(a, b)| a.md5 == b.md5 && a.offset == b.offset && a.len == b.len)
    }
}

#[derive(Debug, Clone)]
pub struct Manifest {
    /// Last segment of the manifest URL (content addressed, changes with every build).
    pub id: String,
    pub files: Vec<FileEntry>,
    pub directories: Vec<PathBuf>,
    /// The (zstd compressed) bytes as downloaded, kept to store next to the game.
    raw: Vec<u8>,
}

impl Manifest {
    pub fn compressed_total(&self) -> u64 {
        self.files.iter().map(FileEntry::compressed_size).sum()
    }

    pub fn install_total(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    pub fn by_name(&self) -> HashMap<&str, &FileEntry> {
        self.files.iter().map(|f| (f.name.as_str(), f)).collect()
    }

    pub fn from_raw(id: impl Into<String>, raw: Vec<u8>) -> Result<Self> {
        let decoded = zstd::stream::decode_all(&raw[..])
            .map_err(|e| InstallError::InvalidData(format!("manifest is not valid zstd: {e}")))?;
        let parsed = SophonManifest::decode(&decoded[..]).map_err(|e| {
            InstallError::InvalidData(format!("manifest is not valid protobuf: {e}"))
        })?;
        Self::from_proto(id.into(), parsed, raw)
    }

    fn from_proto(id: String, parsed: SophonManifest, raw: Vec<u8>) -> Result<Self> {
        let mut files = Vec::with_capacity(parsed.files.len());
        let mut directories = Vec::new();
        for entry in parsed.files {
            let path = safe_relative(&entry.name)?;
            if entry.r#type == TYPE_DIRECTORY {
                directories.push(path);
                continue;
            }
            if let Some(bad) = entry
                .chunks
                .iter()
                .find(|c| !is_safe_chunk_name(&c.chunk_name))
            {
                return Err(InstallError::InvalidData(format!(
                    "unsafe chunk name in manifest: {:?}",
                    bad.chunk_name
                )));
            }
            let mut chunks: Vec<Chunk> = entry.chunks.iter().map(Chunk::from).collect();
            chunks.sort_by_key(|c| c.offset);
            let covered = is_tiled(&chunks, entry.size);
            files.push(FileEntry {
                name: entry.name,
                path,
                size: entry.size,
                md5: entry.md5.to_ascii_lowercase(),
                chunks,
                covered,
            });
        }
        Ok(Self {
            id,
            files,
            directories,
            raw,
        })
    }

    /// Persist as the "installed" manifest of `slot`.
    pub fn store(&self, game_dir: &Path, slot: &str) -> Result<()> {
        let dir = game_dir.join(STORED_DIR);
        std::fs::create_dir_all(&dir).context(|| format!("creating {}", dir.display()))?;
        atomic_write(&dir.join(slot_file(slot)), &self.raw)
    }

    /// The manifest stored by the last successful install of `slot`, if readable.
    pub fn load_stored(game_dir: &Path, slot: &str) -> Option<Self> {
        let raw = std::fs::read(game_dir.join(STORED_DIR).join(slot_file(slot))).ok()?;
        Self::from_raw("stored", raw).ok()
    }
}

/// Chunk names become URL segments and cache file names.
fn is_safe_chunk_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\', '\0', '?', '#'])
}

pub(crate) fn safe_slot(slot: &str) -> String {
    slot.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn slot_file(slot: &str) -> String {
    format!("{}.manifest.zst", safe_slot(slot))
}

fn is_tiled(chunks: &[Chunk], size: u64) -> bool {
    let mut next = 0u64;
    for chunk in chunks {
        if chunk.offset != next || chunk.len == 0 {
            return false;
        }
        next = next.saturating_add(chunk.len);
    }
    next == size
}

/// Manifest names decide where files are written: never let one leave the game directory.
pub(crate) fn safe_relative(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(name);
    let ok = !name.is_empty()
        && !name.contains(['\n', '\0'])
        && path.components().all(|c| matches!(c, Component::Normal(_)));
    if ok {
        Ok(path)
    } else {
        Err(InstallError::InvalidData(format!(
            "unsafe file name in manifest: {name:?}"
        )))
    }
}

/// Download and decode the manifest at `url`.
pub async fn fetch(http: &Http, url: &str, ctx: &JobContext) -> Result<Manifest> {
    ctx.reporter
        .report_now(Progress::new(Phase::Resolving, 0, 0));
    let id = url
        .split('?')
        .next()
        .unwrap_or(url)
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_owned();
    if id.is_empty() {
        return Err(InstallError::InvalidData(format!(
            "manifest URL has no file name: {url}"
        )));
    }
    // Small and cheap: always fetch fresh, never trust a leftover. A corrupt answer is retried once.
    let mut last = None;
    for _ in 0..2 {
        let raw = http.get_bytes(url, MAX_MANIFEST_BYTES, &ctx.cancel).await?;
        match Manifest::from_raw(id.clone(), raw) {
            Ok(manifest) => return Ok(manifest),
            Err(error) => last = Some(error),
        }
    }
    Err(last.expect("two attempts were made"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(size: u64, chunks: Vec<Chunk>) -> FileEntry {
        FileEntry {
            name: "data.bin".to_owned(),
            path: PathBuf::from("data.bin"),
            size,
            md5: "file-md5".to_owned(),
            chunks,
            covered: true,
        }
    }

    fn chunk(md5: &str, offset: u64, len: u64) -> Chunk {
        Chunk {
            name: format!("chunk-{md5}"),
            offset,
            len,
            compressed_len: 10,
            compressed_md5: "compressed".to_owned(),
            md5: md5.to_owned(),
        }
    }

    #[test]
    fn unchanged_chunks_are_content_equivalent() {
        let old = entry(8, vec![chunk("a", 0, 4), chunk("b", 4, 4)]);
        let new = entry(8, vec![chunk("a", 0, 4), chunk("b", 4, 4)]);
        assert!(old.same_content(&new));
    }

    #[test]
    fn chunk_size_or_offset_changes_prevent_trusting_a_file() {
        let old = entry(8, vec![chunk("a", 0, 4), chunk("b", 4, 4)]);
        let changed_size = entry(8, vec![chunk("a", 0, 3), chunk("b", 4, 4)]);
        let changed_offset = entry(8, vec![chunk("a", 1, 4), chunk("b", 4, 4)]);
        assert!(!old.same_content(&changed_size));
        assert!(!old.same_content(&changed_offset));
    }
}
