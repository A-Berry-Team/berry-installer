//! # berry-installer
//!
//! Everything Berry Launcher needs to put files on disk and keep them healthy.
//!
//! The crate is built around four reusable pieces that every kind of install shares:
//!
//! | module | job |
//! | --- | --- |
//! | [`download`] | resumable, retrying, checksum-verified file download |
//! | [`archive`] | path-safe tar.{xz,gz,zst} / zip extraction with progress |
//! | [`state`] | "complete and ready" vs "needs (re)installing" markers |
//! | [`game`] | on-disk state of a game directory: ready / resumable / not installed |
//! | [`progress`] | cancellation ([`CancelToken`]) and progress ([`Progress`]) for the UI |
//!
//! On top of them sit the managers: [`steamrt`] for the Steam Linux Runtimes, [`extras`] for add-on
//! packages (FPS unlocker...), and the game
//! backends ([`sophon`], [`kuro`]).

pub mod archive;
pub mod download;
pub mod error;
pub mod extras;
pub mod fsutil;
pub mod game;
pub mod hash;
pub mod http;
pub mod kuro;
pub mod ldiff;
pub mod progress;
pub mod sophon;
pub mod state;
pub mod steamrt;

pub use error::{InstallError, Result};
pub use game::GameState;
pub use http::Http;
pub use progress::{CancelToken, JobContext, PatchStep, Phase, Progress, Reporter};
pub use state::InstallState;
