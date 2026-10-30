//! HoYoverse "Sophon" games: Genshin Impact, Honkai: Star Rail, Zenless Zone Zero, Honkai Impact 3rd.
//!
//! See [`engine`] for how install, update, resume, repair and pre-download all fall out of one
//! rule (compare every chunk region with the manifest, fetch only what differs).

pub mod engine;
pub(crate) mod local;
pub mod manifest;
mod proto;

pub use engine::{Outcome, SophonInstaller, SophonJob};
pub use manifest::Manifest;

#[doc(hidden)]
pub use proto::{FileChunk, ManifestFile, SophonManifest};
