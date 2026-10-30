//! Checking what is already on disk against the manifest, chunk by chunk.
//!
//! This one operation powers resume (a half-written staging file keeps its finished chunks),
//! updates (a changed file keeps every chunk that did not change) and repair (only the broken
//! chunks are fetched). It is stateless: no journal, the content itself says what is done.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use md5::{Digest, Md5};

use super::manifest::FileEntry;
use crate::error::Result;
use crate::hash::hex;
use crate::progress::CancelToken;

/// Which chunks of a file are already correct at their final offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileScan {
    /// The file exists and has exactly the manifest's size.
    pub size_matches: bool,
    /// One flag per chunk of the entry, in manifest order.
    pub valid: Vec<bool>,
}

impl FileScan {
    pub fn missing(entry: &FileEntry) -> Self {
        Self { size_matches: false, valid: vec![false; entry.chunks.len()] }
    }

    /// Nothing to do for this file.
    pub fn is_complete(&self) -> bool {
        self.size_matches && self.valid.iter().all(|v| *v)
    }

    pub fn valid_bytes(&self, entry: &FileEntry) -> u64 {
        entry.chunks.iter().zip(&self.valid).filter(|(_, ok)| **ok).map(|(c, _)| c.size).sum()
    }
}

/// Hash every chunk region of `path`. `on_bytes` is told how much was read (for progress).
/// A missing or unreadable file is simply "nothing valid".
pub fn scan_file(path: &Path, entry: &FileEntry, cancel: &CancelToken, on_bytes: &dyn Fn(u64)) -> Result<FileScan> {
    let Ok(file) = File::open(path) else { return Ok(FileScan::missing(entry)) };
    let Ok(meta) = file.metadata() else { return Ok(FileScan::missing(entry)) };
    let len = meta.len();
    let mut valid = Vec::with_capacity(entry.chunks.len());
    let mut buffer = Vec::new();
    for chunk in &entry.chunks {
        cancel.check()?;
        if chunk.offset + chunk.size > len {
            valid.push(false);
            continue;
        }
        buffer.resize(chunk.size as usize, 0);
        let ok = file.read_exact_at(&mut buffer, chunk.offset).is_ok() && hex(&Md5::digest(&buffer)) == chunk.md5;
        on_bytes(chunk.size);
        valid.push(ok);
    }
    Ok(FileScan { size_matches: len == entry.size, valid })
}
