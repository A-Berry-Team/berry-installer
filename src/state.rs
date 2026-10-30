//! "Is this thing complete and ready to use?" -- the answer every install needs.
//!
//! A directory is only [`InstallState::Ready`] when a marker file written *last* by the installer
//! exists. Anything that has content but no marker (interrupted download, crash during extraction,
//! files from an older launcher) is [`InstallState::Incomplete`] and gets repaired by running the
//! install again, which is how games can distinguish "ready to launch" from "needs files".
//!
//! Marker format: `key=value` lines. Lines without `=` are ignored, so the `ok` marker written by
//! earlier Berry versions still reads as a ready marker with an unknown version.

use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Result;
use crate::fsutil::atomic_write;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallState {
    /// Nothing there.
    NotInstalled,
    /// Something is there but it never finished (or the marker is gone). Needs install/repair.
    Incomplete,
    /// Finished. `version` is `None` for installs that predate version tracking.
    Ready { version: Option<String> },
}

impl InstallState {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }

    pub fn version(&self) -> Option<&str> {
        match self {
            Self::Ready { version } => version.as_deref(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Marker {
    pub version: Option<String>,
    /// Seconds since the Unix epoch.
    pub installed_at: Option<u64>,
}

impl Marker {
    pub fn now(version: Option<String>) -> Self {
        let installed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs());
        Self {
            version,
            installed_at,
        }
    }

    pub fn parse(text: &str) -> Self {
        let mut marker = Self::default();
        for line in text.lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let value = value.trim();
            match key.trim() {
                "version" if !value.is_empty() => marker.version = Some(value.to_owned()),
                "installed_at" => marker.installed_at = value.parse().ok(),
                _ => {}
            }
        }
        marker
    }

    pub fn render(&self) -> String {
        let mut text = String::from("ok\n");
        if let Some(version) = &self.version {
            text.push_str(&format!("version={version}\n"));
        }
        if let Some(at) = self.installed_at {
            text.push_str(&format!("installed_at={at}\n"));
        }
        text
    }

    pub fn read(path: &Path) -> Option<Self> {
        fs::read_to_string(path).ok().map(|text| Self::parse(&text))
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        atomic_write(path, self.render().as_bytes())
    }
}

/// Classify `dir`: missing/empty, content without marker, or marked complete.
pub fn state_of(dir: &Path, marker_name: &str) -> InstallState {
    if let Some(marker) = Marker::read(&dir.join(marker_name)) {
        return InstallState::Ready {
            version: marker.version,
        };
    }
    match fs::read_dir(dir) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                InstallState::Incomplete
            } else {
                InstallState::NotInstalled
            }
        }
        Err(_) => InstallState::NotInstalled,
    }
}

pub fn clear_marker(dir: &Path, marker_name: &str) -> Result<()> {
    match fs::remove_file(dir.join(marker_name)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(crate::error::InstallError::io(
            format!("removing marker in {}", dir.display()),
            e,
        )),
    }
}
