//! Steam Linux Runtime ("sniper" / steamrt4) installation and updates.
//!
//! Proton and the pressure-vessel container need a Steam Linux Runtime. Valve publishes them at
//! `repo.steampowered.com`:
//!
//! ```text
//! <base>/<edition>/images/<branch>.txt                      -> "0.20250101.12345"   (latest version)
//! <base>/<edition>/images/<version>/SHA256SUMS              -> checksums
//! <base>/<edition>/images/<version>/SteamLinuxRuntime_<x>.tar.xz
//! ```
//!
//! Resolving the branch to a concrete version first (as fischl-rs does) pins the download to an
//! immutable directory, so the checksum list and the archive always belong together.
//!
//! Install flow, every step cancellable:
//! resolve version -> fetch checksum -> download (resumable, verified) -> extract into a staging
//! directory -> check the entry point -> write the ready marker -> swap into place.
//! Because the marker is written last and the swap is a rename, [`SteamRuntimeManager::state`]
//! never reports a half-installed runtime as ready, and a failed update keeps the old runtime.

use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::archive::{ExtractOptions, extract};
use crate::download::{DownloadSpec, download_file};
use crate::error::{InstallError, Result};
use crate::fsutil::{ensure_free_space, remove_dir_if_exists, sibling, swap_dir};
use crate::hash::find_in_sums;
use crate::http::Http;
use crate::progress::{JobContext, Phase, Progress};
use crate::state::{InstallState, Marker, state_of};

pub const DEFAULT_BASE_URL: &str = "https://repo.steampowered.com";
pub const DEFAULT_BRANCH: &str = "latest-public-beta";
/// Written last; its presence is what makes a runtime "ready". Same name earlier Berry versions used.
pub const READY_MARKER: &str = ".berry-ready";
/// The file Proton / pressure-vessel launches through.
pub const ENTRY_POINT: &str = "_v2-entry-point";

/// Disk needed while installing, as a multiple of the archive size (archive + extracted tree).
const SPACE_FACTOR: u64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Edition {
    /// Steam Linux Runtime 3.0 (sniper): what current Proton / GE-Proton ask for.
    Sniper,
    /// Steam Linux Runtime 4.0.
    SteamRt4,
}

impl Edition {
    pub const ALL: [Edition; 2] = [Edition::Sniper, Edition::SteamRt4];

    /// Directory name on Valve's server and on disk (`steamrt3`, `steamrt4`).
    pub fn repo_name(self) -> &'static str {
        match self {
            Self::Sniper => "steamrt3",
            Self::SteamRt4 => "steamrt4",
        }
    }

    /// The `<x>` in `SteamLinuxRuntime_<x>.tar.xz`.
    fn archive_stem(self) -> &'static str {
        match self {
            Self::Sniper => "sniper",
            Self::SteamRt4 => "4",
        }
    }

    /// Steam app id a runner's `toolmanifest.vdf` uses in `require_tool_appid`.
    pub fn tool_appid(self) -> &'static str {
        match self {
            Self::Sniper => "1628350",
            Self::SteamRt4 => "4183110",
        }
    }

    pub fn from_tool_appid(appid: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|e| e.tool_appid() == appid)
    }

    /// Archive for this CPU, or an error on architectures Valve does not publish for.
    pub fn archive_name(self) -> Result<String> {
        let stem = self.archive_stem();
        if cfg!(target_arch = "x86_64") {
            Ok(format!("SteamLinuxRuntime_{stem}.tar.xz"))
        } else if cfg!(target_arch = "aarch64") {
            Ok(format!("SteamLinuxRuntime_{stem}-arm64.tar.xz"))
        } else {
            Err(InstallError::UnsupportedPlatform(
                "Steam Linux Runtime".into(),
            ))
        }
    }
}

/// What [`SteamRuntimeManager::check_update`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStatus {
    pub installed: Option<String>,
    pub latest: String,
    /// `true` when nothing (usable) is installed or the installed version is older.
    pub update_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledRuntime {
    pub edition: Edition,
    pub version: String,
    pub entry_point: PathBuf,
}

/// Manages every edition under one root (`<extras>/steamrt/<edition>/`).
#[derive(Debug, Clone)]
pub struct SteamRuntimeManager {
    http: Http,
    root: PathBuf,
    base_url: String,
    branch: String,
}

/// One install at a time per process: the startup prefetch and a Play press must not race.
fn install_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

impl SteamRuntimeManager {
    pub fn new(root: impl Into<PathBuf>, http: Http) -> Self {
        Self {
            http,
            root: root.into(),
            base_url: DEFAULT_BASE_URL.into(),
            branch: DEFAULT_BRANCH.into(),
        }
    }

    /// Point at a mirror (or a test server).
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into().trim_end_matches('/').to_owned();
        self
    }

    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = branch.into();
        self
    }

    pub fn dir(&self, edition: Edition) -> PathBuf {
        self.root.join(edition.repo_name())
    }

    pub fn entry_point(&self, edition: Edition) -> PathBuf {
        self.dir(edition).join(ENTRY_POINT)
    }

    /// Is this edition complete and launchable? See [`state_at`].
    pub fn state(&self, edition: Edition) -> InstallState {
        state_at(&self.dir(edition))
    }

    /// Ask Valve which version the branch currently points to.
    pub async fn latest_version(&self, edition: Edition, ctx: &JobContext) -> Result<String> {
        ctx.reporter
            .report_now(Progress::new(Phase::Resolving, 0, 0));
        let url = format!(
            "{}/{}/images/{}.txt",
            self.base_url,
            edition.repo_name(),
            self.branch
        );
        let text = self.http.get_text(&url, &ctx.cancel).await?;
        let version = text.trim();
        // It ends up inside URLs and directory names: accept only what a version looks like.
        let valid = !version.is_empty()
            && version.len() <= 64
            && version
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'));
        if valid {
            Ok(version.to_owned())
        } else {
            Err(InstallError::InvalidData(format!(
                "{url} contains {version:?}"
            )))
        }
    }

    pub async fn check_update(&self, edition: Edition, ctx: &JobContext) -> Result<UpdateStatus> {
        let latest = self.latest_version(edition, ctx).await?;
        let installed = match self.state(edition) {
            InstallState::Ready { version } => version,
            _ => None,
        };
        let update_available = match (&self.state(edition), &installed) {
            (InstallState::Ready { .. }, Some(local)) => {
                compare_versions(local, &latest) == Ordering::Less
            }
            // Ready but of unknown version: do not trigger a 500 MB download on a guess.
            (InstallState::Ready { .. }, None) => false,
            _ => true,
        };
        Ok(UpdateStatus {
            installed,
            latest,
            update_available,
        })
    }

    /// Make sure the edition is usable. Does **not** touch the network when it already is, so the
    /// launcher works offline once the runtime has been installed.
    pub async fn ensure(&self, edition: Edition, ctx: &JobContext) -> Result<PathBuf> {
        if self.state(edition).is_ready() {
            return Ok(self.entry_point(edition));
        }
        let _guard = install_lock().lock().await;
        // Someone else may have finished while we waited for the lock.
        if self.state(edition).is_ready() {
            return Ok(self.entry_point(edition));
        }
        Ok(self.install_locked(edition, ctx).await?.entry_point)
    }

    /// Install or upgrade to the latest version; a no-op (besides one small request) when the
    /// installed version is already current.
    pub async fn update(&self, edition: Edition, ctx: &JobContext) -> Result<InstalledRuntime> {
        let _guard = install_lock().lock().await;
        let status = self.check_update(edition, ctx).await?;
        if !status.update_available {
            return Ok(InstalledRuntime {
                edition,
                version: status.installed.unwrap_or(status.latest),
                entry_point: self.entry_point(edition),
            });
        }
        self.install_locked(edition, ctx).await
    }

    /// Reinstall the latest version even when one is present (repair).
    pub async fn reinstall(&self, edition: Edition, ctx: &JobContext) -> Result<InstalledRuntime> {
        let _guard = install_lock().lock().await;
        self.install_locked(edition, ctx).await
    }

    async fn install_locked(&self, edition: Edition, ctx: &JobContext) -> Result<InstalledRuntime> {
        let archive_name = edition.archive_name()?;
        let version = self.latest_version(edition, ctx).await?;
        let release_url = format!(
            "{}/{}/images/{}",
            self.base_url,
            edition.repo_name(),
            version
        );

        let sums = self
            .http
            .get_text(&format!("{release_url}/SHA256SUMS"), &ctx.cancel)
            .await?;
        let checksum = find_in_sums(&sums, &archive_name).ok_or_else(|| {
            InstallError::InvalidData(format!("SHA256SUMS has no entry for {archive_name}"))
        })?;
        ctx.cancel.check()?;

        tracing::info!("[steamrt] installing {} {version}", edition.repo_name());
        let dest = self.dir(edition);
        let downloads = self.root.join(".downloads").join(edition.repo_name());
        let archive = downloads.join(&archive_name);
        let staging = sibling(&dest, ".staging");

        let spec = DownloadSpec::new(format!("{release_url}/{archive_name}"), &archive)
            .with_checksum(checksum);
        let size = download_file(&self.http, &spec, ctx).await?;
        ensure_free_space(&self.root, size.saturating_mul(SPACE_FACTOR - 1))?;

        remove_dir_if_exists(&staging)?;
        let extracted = extract(&archive, &staging, ExtractOptions { strip_root: true }, ctx).await;
        if let Err(error) = extracted {
            let _ = remove_dir_if_exists(&staging);
            return Err(error);
        }
        if !staging.join(ENTRY_POINT).is_file() {
            let _ = remove_dir_if_exists(&staging);
            return Err(InstallError::InvalidData(format!(
                "{archive_name} does not contain {ENTRY_POINT}"
            )));
        }

        // Point of no return: stop reacting to cancel from here, the swap is two renames.
        ctx.reporter
            .report_now(Progress::new(Phase::Finalizing, 0, 0));
        Marker::now(Some(version.clone())).write(&staging.join(READY_MARKER))?;
        swap_dir(&staging, &dest)?;

        // The archive is only a transfer vehicle; keeping it would double the disk usage.
        let _ = std::fs::remove_file(&archive);
        let _ = std::fs::remove_dir(&downloads);
        ctx.reporter
            .report_now(Progress::new(Phase::Finalizing, 1, 1));
        tracing::info!("[steamrt] {} {version} ready", edition.repo_name());
        Ok(InstalledRuntime {
            edition,
            version,
            entry_point: self.entry_point(edition),
        })
    }
}

/// State of a runtime directory without needing a manager (no network client involved).
///
/// Ready needs both the marker and the entry point, so a runtime someone half-deleted is reported
/// as [`InstallState::Incomplete`] and gets repaired by the next install.
pub fn state_at(dir: &Path) -> InstallState {
    match state_of(dir, READY_MARKER) {
        InstallState::Ready { .. } if !dir.join(ENTRY_POINT).is_file() => InstallState::Incomplete,
        InstallState::Ready { version: None } => {
            // Marker from an older Berry: take the version from the runtime's own list.
            InstallState::Ready {
                version: local_version_from_versions_txt(dir),
            }
        }
        other => other,
    }
}

/// `VERSIONS.txt` ships inside the runtime: `depot<TAB>0.20250101.12345`.
fn local_version_from_versions_txt(dir: &Path) -> Option<String> {
    let text = std::fs::read_to_string(dir.join("VERSIONS.txt")).ok()?;
    text.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        (fields.next() == Some("depot"))
            .then(|| fields.next().map(str::to_owned))
            .flatten()
    })
}

/// Compare dotted versions numerically (`0.20250101.9` < `0.20250101.10`). Components that are not
/// numbers fall back to text comparison, and missing components count as zero.
pub fn compare_versions(a: &str, b: &str) -> Ordering {
    let mut left = a.trim().split('.');
    let mut right = b.trim().split('.');
    loop {
        match (left.next(), right.next()) {
            (None, None) => return Ordering::Equal,
            (l, r) => {
                let (l, r) = (l.unwrap_or("0"), r.unwrap_or("0"));
                let ordering = match (l.parse::<u64>(), r.parse::<u64>()) {
                    (Ok(l), Ok(r)) => l.cmp(&r),
                    _ => l.cmp(r),
                };
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
        }
    }
}
