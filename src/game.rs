//! On-disk state of an installed game: complete and launchable, interrupted and resumable, or
//! simply not there. One definition shared by the library view, the Play button and repair.
//!
//! A game directory is described by four things the installer writes:
//!
//! | file / directory | meaning |
//! | --- | --- |
//! | `.berry-installing` | an install, update or repair is running (or died): written first, removed last |
//! | `downloading/`, `patching/`, `repairing/` | staging areas of the backends; present means unfinished work |
//! | `.berry-version` | version of the files on disk, written after everything else |
//! | `config.ini` (`game_version=`) | version fallback for installs made by other launchers |
//!
//! Whether the game's executable exists is game-specific (catalogue), so the caller passes it in.

use std::fs;
use std::path::Path;

use crate::error::{InstallError, IoContext, Result};
use crate::fsutil::atomic_write;

pub const INSTALLING_MARKER: &str = ".berry-installing";
pub const VERSION_FILE: &str = ".berry-version";
/// Staging directories the download backends use inside the game directory.
pub const STAGING_DIRS: [&str; 3] = ["downloading", "patching", "repairing"];

/// Why a game directory is not considered finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unfinished {
    /// The install marker is still there: the process died or the operation failed.
    InterruptedOperation,
    /// A backend staging directory is left over.
    Staging(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GameState {
    /// No executable and no unfinished work: a fresh install is needed.
    NotInstalled,
    /// Unfinished work on disk. Running the install again resumes it; the game cannot be launched.
    Incomplete(Unfinished),
    /// Executable present and nothing pending. `version` is `None` for installs that never
    /// recorded one.
    Ready { version: Option<String> },
}

impl GameState {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }

    /// Something has to be downloaded before the game can start.
    pub fn needs_download(&self) -> bool {
        !self.is_ready()
    }

    /// A partial install exists that "Install" can resume (as opposed to starting from nothing).
    pub fn is_resumable(&self) -> bool {
        matches!(self, Self::Incomplete(_))
    }

    pub fn version(&self) -> Option<&str> {
        match self {
            Self::Ready { version } => version.as_deref(),
            _ => None,
        }
    }
}

/// Unfinished work in `dir`, if any (independent of the executable).
pub fn unfinished_work(dir: &Path) -> Option<Unfinished> {
    if dir.join(INSTALLING_MARKER).exists() {
        return Some(Unfinished::InterruptedOperation);
    }
    STAGING_DIRS
        .iter()
        .find(|name| dir.join(name).is_dir())
        .map(|name| Unfinished::Staging(name))
}

/// Remove every staging directory. Call only once *all* jobs of an operation succeeded: whatever
/// is left in them (journals of slots that are no longer part of the plan, `.part` files from
/// older launchers, an empty `patching/`) is garbage, and its mere presence would keep the game
/// listed as unfinished forever.
pub fn clear_staging(dir: &Path) -> Result<()> {
    for name in STAGING_DIRS {
        let path = dir.join(name);
        if path.is_dir() {
            fs::remove_dir_all(&path).context(|| format!("removing {}", path.display()))?;
        }
    }
    Ok(())
}

/// Classify a game directory. `has_executable` says whether the game's own executable exists.
pub fn inspect(dir: &Path, has_executable: bool) -> GameState {
    if let Some(unfinished) = unfinished_work(dir) {
        return GameState::Incomplete(unfinished);
    }
    if has_executable {
        GameState::Ready {
            version: installed_version(dir),
        }
    } else {
        GameState::NotInstalled
    }
}

/// Version recorded by Berry (`.berry-version`), or by another launcher (`config.ini`).
pub fn installed_version(dir: &Path) -> Option<String> {
    let recorded = fs::read_to_string(dir.join(VERSION_FILE))
        .ok()
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty());
    recorded.or_else(|| {
        let config = fs::read_to_string(dir.join("config.ini")).ok()?;
        parse_config_version(&config)
    })
}

fn parse_config_version(config: &str) -> Option<String> {
    config
        .lines()
        .find_map(|line| line.trim().strip_prefix("game_version="))
        .map(str::trim)
        .filter(|version| !version.is_empty())
        .map(str::to_owned)
}

/// Flag an operation as running; call before touching the directory. If the process dies, the
/// marker's presence is what makes the game show up as resumable.
pub fn begin_install(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir).context(|| format!("creating {}", dir.display()))?;
    fs::write(dir.join(INSTALLING_MARKER), b"1")
        .context(|| format!("writing the install marker in {}", dir.display()))
}

/// Remove the marker after *everything* (including [`write_version`]) succeeded.
pub fn finish_install(dir: &Path) -> Result<()> {
    match fs::remove_file(dir.join(INSTALLING_MARKER)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(InstallError::io("removing the install marker", error)),
    }
}

/// Record the installed version. Written last: it takes precedence over `config.ini`.
pub fn write_version(dir: &Path, version: &str) -> Result<()> {
    atomic_write(&dir.join(VERSION_FILE), version.as_bytes())
}
