//! Small add-on packages the launcher fetches next to the games (BerryUnlock, jadeite...).
//!
//! They are described by JSON manifests published by the Twintail project
//! (`<manifest base>/<package id>.json`); only that *data* is shared, no code. A manifest lists
//! packages in one of two download modes:
//!
//! * `DOWNLOAD_MODE_FILE`: one file (`git_url`), optionally an archive to unpack;
//! * `DOWNLOAD_MODE_RAW`: a list of files under `raw_url`.
//!
//! Compared with the fischl-rs version: the result is tracked with a marker (so a startup prefetch
//! costs one tiny request instead of a download), an installed package keeps working when the
//! manifest server is unreachable, archives are unpacked with [`crate::archive`], and every
//! download goes through the resumable [`crate::download`].
//!
//! Packages that are published as GitHub releases (BerryUnlock) use [`ExtrasManager::ensure_github`]
//! instead: it asks the GitHub API for the latest release of a repository, picks one asset by name
//! and installs it with the very same marker / offline / archive handling.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::archive::{ArchiveKind, ExtractOptions, extract};
use crate::download::{DownloadSpec, download_file};
use crate::error::{InstallError, IoContext, Result};
use crate::fsutil::{merge_into, remove_dir_if_exists, sibling};
use crate::hash::{Checksum, sha256_file};
use crate::http::Http;
use crate::progress::JobContext;
use crate::state::{Marker, state_of};

pub const DEFAULT_MANIFEST_BASE: &str =
    "https://dl-public.twintaillauncher.app/launcher_app/manifests";

pub const DEFAULT_GITHUB_API_BASE: &str = "https://api.github.com";

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(default)]
pub struct Package {
    pub package_name: String,
    pub version: String,
    pub default_download_mode: String,
    /// `DOWNLOAD_MODE_FILE`: where the file lives.
    pub git_url: String,
    pub zip_sha256: String,
    pub size: u64,
    /// `DOWNLOAD_MODE_RAW`: base URL and the files below it (`dir/` entries are directories).
    pub raw_url: String,
    pub file_list: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct Manifest {
    retcode: i32,
    message: String,
    data: Option<ManifestData>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct ManifestData {
    packages: Vec<Package>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct GithubRelease {
    tag_name: String,
    draft: bool,
    assets: Vec<GithubAsset>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
    size: u64,
    /// `sha256:<hex>`; only present on releases published after GitHub started recording it.
    digest: Option<String>,
}

/// One asset of the latest release of a GitHub repository.
#[derive(Debug, Clone, Copy)]
pub struct GithubRef<'a> {
    /// Name used for the install marker (`berryunlock`).
    pub id: &'a str,
    /// `owner/name`.
    pub repo: &'a str,
    /// Asset to install, matched loosely (`berry_unlock.exe`).
    pub asset: &'a str,
}

/// Which package of which manifest.
#[derive(Debug, Clone, Copy)]
pub struct ExtraRef<'a> {
    /// Manifest name (`jadeite`).
    pub id: &'a str,
    /// Which package inside the manifest, matched loosely (`jadeite`).
    pub kind: &'a str,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ExtraOptions {
    /// Unpack the downloaded file when it is an archive (bare executables are left alone) and
    /// delete the archive afterwards.
    pub extract: bool,
    /// Drop a single top-level folder of the archive.
    pub strip_root: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledExtra {
    pub version: String,
    /// `false` when it was already up to date (or the manifest was unreachable).
    pub changed: bool,
}

#[derive(Debug, Clone)]
pub struct ExtrasManager {
    http: Http,
    manifest_base: String,
    github_api_base: String,
    enforce_checksums: bool,
}

/// Lower-cases and drops every non-alphanumeric character so that `berry_unlock`,
/// `BerryUnlock.exe` and `berry-unlock.zip` compare as equivalent.
fn normalize(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Tolerant package lookup: by `kind`, then by manifest id, then the only package there is.
pub fn pick_package<'a>(packages: &'a [Package], wanted: ExtraRef<'_>) -> Option<&'a Package> {
    let (kind, id) = (normalize(wanted.kind), normalize(wanted.id));
    packages
        .iter()
        .find(|p| normalize(&p.package_name).contains(&kind))
        .or_else(|| {
            packages
                .iter()
                .find(|p| normalize(&p.package_name).contains(&id))
        })
        .or_else(|| {
            if packages.len() == 1 {
                packages.first()
            } else {
                None
            }
        })
}

/// Asset lookup: exact (normalized) name first, then any asset that contains the wanted name
/// without its extension (`berry_unlock-v1.2.0.exe` for `berry_unlock.exe`).
fn pick_asset<'a>(assets: &'a [GithubAsset], wanted: &str) -> Option<&'a GithubAsset> {
    let exact = normalize(wanted);
    let stem = normalize(wanted.rsplit_once('.').map_or(wanted, |(stem, _)| stem));
    let usable = |a: &&GithubAsset| !a.browser_download_url.is_empty();
    assets
        .iter()
        .filter(usable)
        .find(|a| normalize(&a.name) == exact)
        .or_else(|| {
            assets
                .iter()
                .filter(usable)
                .find(|a| !stem.is_empty() && normalize(&a.name).contains(&stem))
        })
}

fn marker_name(id: &str) -> String {
    format!(".berry-extra-{}", normalize(id))
}

impl ExtrasManager {
    pub fn new(http: Http) -> Self {
        Self {
            http,
            manifest_base: DEFAULT_MANIFEST_BASE.into(),
            github_api_base: DEFAULT_GITHUB_API_BASE.into(),
            enforce_checksums: false,
        }
    }

    pub fn with_manifest_base(mut self, base: impl Into<String>) -> Self {
        self.manifest_base = base.into().trim_end_matches('/').to_owned();
        self
    }

    pub fn with_github_api_base(mut self, base: impl Into<String>) -> Self {
        self.github_api_base = base.into().trim_end_matches('/').to_owned();
        self
    }

    /// Fail (instead of just warn) when a downloaded file does not match the manifest's
    /// `zip_sha256`. Off by default: the field's exact semantics for non-zip files has not been
    /// confirmed against the live manifests, and a wrong guess must not break the FPS unlocker.
    pub fn enforce_checksums(mut self, enforce: bool) -> Self {
        self.enforce_checksums = enforce;
        self
    }

    /// Version recorded for `id` in `dest`, if it was installed completely.
    pub fn installed_version(dest: &Path, id: &str) -> Option<String> {
        state_of(dest, &marker_name(id))
            .version()
            .map(str::to_owned)
    }

    pub async fn fetch_package(&self, wanted: ExtraRef<'_>, ctx: &JobContext) -> Result<Package> {
        if wanted.id.is_empty() {
            return Err(InstallError::InvalidData("empty package id".into()));
        }
        let url = format!("{}/{}.json", self.manifest_base, wanted.id);
        let body = self.http.get_text(&url, &ctx.cancel).await?;
        let manifest: Manifest = serde_json::from_str(&body).map_err(|e| {
            let start: String = body.chars().take(120).collect();
            InstallError::InvalidData(format!(
                "{url} is not a valid manifest ({e}); starts with {start:?}"
            ))
        })?;
        if manifest.retcode != 0 {
            return Err(InstallError::InvalidData(format!(
                "manifest '{}' reports retcode {} ({})",
                wanted.id, manifest.retcode, manifest.message
            )));
        }
        let data = manifest.data.ok_or_else(|| {
            InstallError::InvalidData(format!(
                "manifest '{}' has no data ({})",
                wanted.id, manifest.message
            ))
        })?;
        pick_package(&data.packages, wanted)
            .cloned()
            .ok_or_else(|| {
                let names: Vec<&str> = data
                    .packages
                    .iter()
                    .map(|p| p.package_name.as_str())
                    .collect();
                InstallError::InvalidData(format!(
                    "manifest '{}' has no package matching '{}' (available: {})",
                    wanted.id,
                    wanted.kind,
                    names.join(", ")
                ))
            })
    }

    /// Make sure the package is in `dest` at the manifest's current version.
    ///
    /// Already installed and the manifest cannot be reached: succeeds with `changed = false`, so
    /// an offline start never breaks something that was working.
    pub async fn ensure(
        &self,
        wanted: ExtraRef<'_>,
        dest: &Path,
        options: ExtraOptions,
        ctx: &JobContext,
    ) -> Result<InstalledExtra> {
        let installed = Self::installed_version(dest, wanted.id);
        let package = match self.fetch_package(wanted, ctx).await {
            Ok(package) => package,
            Err(error) if !error.is_cancelled() => match installed {
                Some(version) => {
                    tracing::warn!(
                        "[extras] {}: manifest unavailable ({error}); keeping {version}",
                        wanted.id
                    );
                    return Ok(InstalledExtra {
                        version,
                        changed: false,
                    });
                }
                None => return Err(error),
            },
            Err(error) => return Err(error),
        };
        if installed.as_deref() == Some(package.version.as_str()) {
            return Ok(InstalledExtra {
                version: package.version,
                changed: false,
            });
        }
        self.install(wanted, &package, dest, options, ctx).await
    }

    /// Latest published release of `repo`. `releases/latest` ignores pre-releases, so a project
    /// that only publishes those (BerryUnlock is still one) falls back to the newest listed one.
    async fn fetch_github_release(&self, repo: &str, ctx: &JobContext) -> Result<GithubRelease> {
        if repo.is_empty() || repo.contains("..") || repo.split('/').count() != 2 {
            return Err(InstallError::InvalidData(format!(
                "'{repo}' is not an owner/name GitHub repository"
            )));
        }
        let latest_url = format!("{}/repos/{repo}/releases/latest", self.github_api_base);
        let latest = match self.http.get_text(&latest_url, &ctx.cancel).await {
            Ok(body) => Some(body),
            Err(error) if error.is_cancelled() => return Err(error),
            Err(_) => None,
        };
        if let Some(body) = latest {
            let release: GithubRelease = serde_json::from_str(&body).map_err(|e| {
                InstallError::InvalidData(format!("{latest_url} is not a valid release ({e})"))
            })?;
            return Ok(release);
        }
        let list_url = format!("{}/repos/{repo}/releases?per_page=10", self.github_api_base);
        let body = self.http.get_text(&list_url, &ctx.cancel).await?;
        let releases: Vec<GithubRelease> = serde_json::from_str(&body).map_err(|e| {
            InstallError::InvalidData(format!("{list_url} is not a valid release list ({e})"))
        })?;
        releases
            .into_iter()
            .find(|r| !r.draft && !r.assets.is_empty())
            .ok_or_else(|| {
                InstallError::InvalidData(format!("{repo} has no published release with files"))
            })
    }

    /// Resolves the wanted asset of the latest release into a [`Package`] (version = tag).
    pub async fn fetch_github_package(
        &self,
        wanted: GithubRef<'_>,
        ctx: &JobContext,
    ) -> Result<Package> {
        let release = self.fetch_github_release(wanted.repo, ctx).await?;
        let asset = pick_asset(&release.assets, wanted.asset).ok_or_else(|| {
            let names: Vec<&str> = release.assets.iter().map(|a| a.name.as_str()).collect();
            InstallError::InvalidData(format!(
                "release {} of {} has no asset matching '{}' (available: {})",
                release.tag_name,
                wanted.repo,
                wanted.asset,
                names.join(", ")
            ))
        })?;
        if release.tag_name.is_empty() {
            return Err(InstallError::InvalidData(format!(
                "latest release of {} has no tag",
                wanted.repo
            )));
        }
        let digest = asset
            .digest
            .as_deref()
            .and_then(|d| d.strip_prefix("sha256:"))
            .unwrap_or_default()
            .to_owned();
        Ok(Package {
            package_name: asset.name.clone(),
            version: release.tag_name.clone(),
            default_download_mode: "DOWNLOAD_MODE_FILE".into(),
            git_url: asset.browser_download_url.clone(),
            zip_sha256: digest,
            size: asset.size,
            ..Package::default()
        })
    }

    /// Like [`ensure`](Self::ensure), for an asset of the latest GitHub release of a repository.
    /// The release tag is the installed version; offline with a copy installed still succeeds.
    pub async fn ensure_github(
        &self,
        wanted: GithubRef<'_>,
        dest: &Path,
        options: ExtraOptions,
        ctx: &JobContext,
    ) -> Result<InstalledExtra> {
        let installed = Self::installed_version(dest, wanted.id);
        let package = match self.fetch_github_package(wanted, ctx).await {
            Ok(package) => package,
            Err(error) if !error.is_cancelled() => match installed {
                Some(version) => {
                    tracing::warn!(
                        "[extras] {}: GitHub unavailable ({error}); keeping {version}",
                        wanted.id
                    );
                    return Ok(InstalledExtra {
                        version,
                        changed: false,
                    });
                }
                None => return Err(error),
            },
            Err(error) => return Err(error),
        };
        if installed.as_deref() == Some(package.version.as_str()) {
            return Ok(InstalledExtra {
                version: package.version,
                changed: false,
            });
        }
        let as_extra = ExtraRef {
            id: wanted.id,
            kind: wanted.asset,
        };
        self.install(as_extra, &package, dest, options, ctx).await
    }

    async fn install(
        &self,
        wanted: ExtraRef<'_>,
        package: &Package,
        dest: &Path,
        options: ExtraOptions,
        ctx: &JobContext,
    ) -> Result<InstalledExtra> {
        std::fs::create_dir_all(dest).context(|| format!("creating {}", dest.display()))?;
        // From here the old version is being replaced: it must not look ready if we die midway.
        crate::state::clear_marker(dest, &marker_name(wanted.id))?;

        match package.default_download_mode.as_str() {
            "DOWNLOAD_MODE_FILE" => self.install_file(package, dest, options, ctx).await?,
            "DOWNLOAD_MODE_RAW" => self.install_raw(package, dest, ctx).await?,
            other => {
                return Err(InstallError::InvalidData(format!(
                    "package '{}' uses an unsupported download mode '{other}'",
                    package.package_name
                )));
            }
        }

        Marker::now(Some(package.version.clone())).write(&dest.join(marker_name(wanted.id)))?;
        tracing::info!(
            "[extras] {} {} installed in {}",
            wanted.id,
            package.version,
            dest.display()
        );
        Ok(InstalledExtra {
            version: package.version.clone(),
            changed: true,
        })
    }

    async fn install_file(
        &self,
        package: &Package,
        dest: &Path,
        options: ExtraOptions,
        ctx: &JobContext,
    ) -> Result<()> {
        if package.git_url.is_empty() {
            return Err(InstallError::InvalidData(format!(
                "package '{}' has no download url",
                package.package_name
            )));
        }
        let file_name = safe_file_name(&package.package_name)?;
        let target = dest.join(file_name);
        // A previous version of the same file must not satisfy (or resume into) this download.
        let _ = std::fs::remove_file(&target);
        download_file(
            &self.http,
            &DownloadSpec::new(&package.git_url, &target),
            ctx,
        )
        .await?;
        self.check_checksum(package, &target, ctx).await?;

        if options.extract && ArchiveKind::detect(&target).is_some() {
            let staging = sibling(
                &dest.join(".extract"),
                &format!("-{}", normalize(&package.package_name)),
            );
            remove_dir_if_exists(&staging)?;
            let unpacked = extract(
                &target,
                &staging,
                ExtractOptions {
                    strip_root: options.strip_root,
                },
                ctx,
            )
            .await;
            let merged = unpacked.and_then(|()| merge_into(&staging, dest));
            if merged.is_err() {
                let _ = remove_dir_if_exists(&staging);
            }
            merged?;
            let _ = std::fs::remove_file(&target);
        }
        Ok(())
    }

    async fn install_raw(&self, package: &Package, dest: &Path, ctx: &JobContext) -> Result<()> {
        if package.file_list.is_empty() {
            return Err(InstallError::InvalidData(format!(
                "package '{}' has an empty file list",
                package.package_name
            )));
        }
        let base = package.raw_url.trim_end_matches('/');
        for entry in &package.file_list {
            let relative = safe_relative(entry.trim_end_matches('/'))?;
            if entry.ends_with('/') {
                std::fs::create_dir_all(dest.join(&relative))
                    .context(|| format!("creating {}", relative.display()))?;
            } else {
                download_file(
                    &self.http,
                    &DownloadSpec::new(format!("{base}/{entry}"), dest.join(&relative)),
                    ctx,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn check_checksum(&self, package: &Package, file: &Path, ctx: &JobContext) -> Result<()> {
        let Ok(expected) = Checksum::sha256(&package.zip_sha256) else {
            return Ok(());
        };
        let (path, job) = (file.to_owned(), ctx.clone());
        let actual = tokio::task::spawn_blocking(move || sha256_file(&path, &job))
            .await
            .map_err(|e| InstallError::Archive(format!("hash task failed: {e}")))??;
        if actual == expected.expected() {
            return Ok(());
        }
        let error = InstallError::ChecksumMismatch {
            file: package.package_name.clone(),
            expected: expected.expected().to_owned(),
            actual,
        };
        if self.enforce_checksums {
            let _ = std::fs::remove_file(file);
            Err(error)
        } else {
            tracing::warn!("[extras] {error} (not enforced)");
            Ok(())
        }
    }
}

/// Manifest data decides file names; never let it write outside the destination.
fn safe_relative(value: &str) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    let ok = !value.is_empty()
        && path.components().all(|c| {
            matches!(
                c,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        });
    if ok {
        Ok(path)
    } else {
        Err(InstallError::InvalidData(format!(
            "unsafe path in manifest: {value:?}"
        )))
    }
}

fn safe_file_name(value: &str) -> Result<PathBuf> {
    let path = safe_relative(value)?;
    if path.components().count() == 1 {
        Ok(path)
    } else {
        Err(InstallError::InvalidData(format!(
            "package name is not a file name: {value:?}"
        )))
    }
}
