//! Small filesystem helpers: free space, atomic writes, directory swaps.

use std::ffi::CString;
use std::fs;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use crate::error::{InstallError, IoContext, Result};

/// Free bytes for unprivileged users on the filesystem holding `path` (or its nearest existing
/// ancestor). `None` when it cannot be determined.
pub fn available_space(path: &Path) -> Option<u64> {
    let mut probe: PathBuf = path.to_owned();
    while !probe.exists() {
        if !probe.pop() {
            return None;
        }
    }
    let c_path = CString::new(probe.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `c_path` is a valid NUL-terminated string and `stat` points to writable memory
    // of the right type; `statvfs` only writes to it.
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: statvfs returned 0, so the structure is initialised.
    let stat = unsafe { stat.assume_init() };
    #[allow(clippy::unnecessary_cast)] // field widths differ between platforms
    Some((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

/// Fail early with [`InstallError::NoSpace`] when `needed` bytes cannot fit. Unknown free space
/// is not an error.
pub fn ensure_free_space(path: &Path, needed: u64) -> Result<()> {
    match available_space(path) {
        Some(available) if available < needed => Err(InstallError::NoSpace {
            path: path.to_owned(),
            needed,
            available,
        }),
        _ => Ok(()),
    }
}

/// Write `contents` so readers see either the old file or the new one, never half of it.
pub fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let tmp = sibling(path, ".tmp");
    let mut file = fs::File::create(&tmp).context(|| format!("creating {}", tmp.display()))?;
    file.write_all(contents)
        .context(|| format!("writing {}", tmp.display()))?;
    file.sync_all()
        .context(|| format!("syncing {}", tmp.display()))?;
    fs::rename(&tmp, path).context(|| format!("renaming {} to {}", tmp.display(), path.display()))
}

/// `<path><suffix>` next to `path` (same directory, so renames never cross filesystems).
pub fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().map(|n| n.to_owned()).unwrap_or_default();
    name.push(suffix);
    path.with_file_name(name)
}

pub fn remove_dir_if_exists(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(InstallError::io(format!("removing {}", path.display()), e)),
    }
}

/// Replace `dest` with the fully prepared `staged` directory. The old content is renamed away
/// first and deleted only after the new one is in place, so a crash leaves one of the two intact.
pub fn swap_dir(staged: &Path, dest: &Path) -> Result<()> {
    let backup = sibling(dest, ".old");
    remove_dir_if_exists(&backup)?;
    let had_old = dest.exists();
    if had_old {
        fs::rename(dest, &backup)
            .context(|| format!("moving {} out of the way", dest.display()))?;
    }
    if let Err(error) = fs::rename(staged, dest) {
        if had_old {
            let _ = fs::rename(&backup, dest);
        }
        return Err(InstallError::io(
            format!("moving {} into place", staged.display()),
            error,
        ));
    }
    remove_dir_if_exists(&backup)
}

/// If `dir` contains exactly one entry and it is a directory, move that directory's content up
/// into `dir` (`dir/Root/a` becomes `dir/a`). Archives usually wrap everything in one folder.
pub fn flatten_single_root(dir: &Path) -> Result<bool> {
    let mut entries = fs::read_dir(dir).context(|| format!("reading {}", dir.display()))?;
    let (Some(Ok(only)), None) = (entries.next(), entries.next()) else {
        return Ok(false);
    };
    let root = only.path();
    if !only.file_type().map(|t| t.is_dir()).unwrap_or(false) {
        return Ok(false);
    }
    // Rename the root out of the way first: a child may carry the same name as the root.
    let parked = sibling(&root, ".flatten");
    fs::rename(&root, &parked).context(|| format!("renaming {}", root.display()))?;
    for child in fs::read_dir(&parked).context(|| format!("reading {}", parked.display()))? {
        let child = child.context(|| format!("reading {}", parked.display()))?;
        let target = dir.join(child.file_name());
        fs::rename(child.path(), &target).context(|| format!("moving {}", target.display()))?;
    }
    fs::remove_dir(&parked).context(|| format!("removing {}", parked.display()))?;
    Ok(true)
}

/// Move every entry of `src` into `dest`, replacing entries that already exist there (files or
/// whole directories). `src` itself is removed afterwards.
pub fn merge_into(src: &Path, dest: &Path) -> Result<()> {
    fs::create_dir_all(dest).context(|| format!("creating {}", dest.display()))?;
    for child in fs::read_dir(src).context(|| format!("reading {}", src.display()))? {
        let child = child.context(|| format!("reading {}", src.display()))?;
        let target = dest.join(child.file_name());
        match fs::symlink_metadata(&target) {
            Ok(meta) if meta.is_dir() => remove_dir_if_exists(&target)?,
            Ok(_) => {
                fs::remove_file(&target).context(|| format!("replacing {}", target.display()))?
            }
            Err(_) => {}
        }
        fs::rename(child.path(), &target).context(|| format!("moving {}", target.display()))?;
    }
    remove_dir_if_exists(src)
}
