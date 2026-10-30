use std::fs;

use berry_installer::InstallError;
use berry_installer::error::format_bytes;
use berry_installer::fsutil::{
    atomic_write, available_space, ensure_free_space, flatten_single_root, merge_into, sibling,
    swap_dir,
};
use berry_installer::state::{InstallState, Marker, clear_marker, state_of};

#[test]
fn atomic_write_replaces_content_and_leaves_no_temp_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.json");
    atomic_write(&path, b"one").unwrap();
    atomic_write(&path, b"two").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"two");
    let names: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(names, ["data.json"]);
}

#[test]
fn sibling_stays_in_the_same_directory() {
    let path = std::path::Path::new("/a/b/game");
    assert_eq!(sibling(path, ".old"), std::path::Path::new("/a/b/game.old"));
}

#[test]
fn swap_dir_replaces_the_old_directory() {
    let root = tempfile::tempdir().unwrap();
    let (staged, dest) = (root.path().join("staged"), root.path().join("dest"));
    fs::create_dir_all(&staged).unwrap();
    fs::create_dir_all(&dest).unwrap();
    fs::write(staged.join("new.txt"), "new").unwrap();
    fs::write(dest.join("old.txt"), "old").unwrap();

    swap_dir(&staged, &dest).unwrap();

    assert!(dest.join("new.txt").is_file());
    assert!(!dest.join("old.txt").exists());
    assert!(!staged.exists());
    assert!(!sibling(&dest, ".old").exists(), "backup is cleaned up");
}

#[test]
fn swap_dir_works_when_there_is_nothing_to_replace() {
    let root = tempfile::tempdir().unwrap();
    let (staged, dest) = (root.path().join("staged"), root.path().join("dest"));
    fs::create_dir_all(&staged).unwrap();
    fs::write(staged.join("a"), "a").unwrap();
    swap_dir(&staged, &dest).unwrap();
    assert!(dest.join("a").is_file());
}

#[test]
fn single_root_is_flattened_even_when_a_child_has_the_same_name() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("Root/Root")).unwrap();
    fs::write(dir.path().join("Root/file.txt"), "x").unwrap();

    assert!(flatten_single_root(dir.path()).unwrap());
    assert!(dir.path().join("file.txt").is_file());
    assert!(dir.path().join("Root").is_dir());
}

#[test]
fn several_entries_are_not_flattened() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("a")).unwrap();
    fs::create_dir(dir.path().join("b")).unwrap();
    assert!(!flatten_single_root(dir.path()).unwrap());
    assert!(dir.path().join("a").is_dir());
}

#[test]
fn a_lone_file_is_not_flattened() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("only.txt"), "x").unwrap();
    assert!(!flatten_single_root(dir.path()).unwrap());
}

#[test]
fn merge_into_replaces_files_and_directories() {
    let root = tempfile::tempdir().unwrap();
    let (src, dest) = (root.path().join("src"), root.path().join("dest"));
    fs::create_dir_all(src.join("dir")).unwrap();
    fs::write(src.join("dir/new.txt"), "new").unwrap();
    fs::write(src.join("file.txt"), "new").unwrap();
    fs::create_dir_all(dest.join("dir")).unwrap();
    fs::write(dest.join("dir/stale.txt"), "stale").unwrap();
    fs::write(dest.join("file.txt"), "old").unwrap();
    fs::write(dest.join("untouched.txt"), "keep").unwrap();

    merge_into(&src, &dest).unwrap();

    assert!(!src.exists());
    assert_eq!(fs::read_to_string(dest.join("file.txt")).unwrap(), "new");
    assert!(dest.join("dir/new.txt").is_file());
    assert!(!dest.join("dir/stale.txt").exists());
    assert!(dest.join("untouched.txt").is_file());
}

#[test]
fn free_space_looks_at_the_nearest_existing_ancestor() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("a/b/c");
    assert!(available_space(&missing).is_some());
    ensure_free_space(&missing, 1).unwrap();
    match ensure_free_space(dir.path(), u64::MAX) {
        Err(InstallError::NoSpace { needed, .. }) => assert_eq!(needed, u64::MAX),
        other => panic!("expected NoSpace, got {other:?}"),
    }
}

#[test]
fn bytes_are_formatted_for_humans() {
    assert_eq!(format_bytes(0), "0 B");
    assert_eq!(format_bytes(1023), "1023 B");
    assert_eq!(format_bytes(1024), "1.0 KiB");
    assert_eq!(format_bytes(1536), "1.5 KiB");
    assert_eq!(format_bytes(5 * 1024 * 1024 * 1024), "5.0 GiB");
}

#[test]
fn marker_round_trips() {
    let marker = Marker {
        version: Some("1.2.3".into()),
        installed_at: Some(1_700_000_000),
    };
    assert_eq!(Marker::parse(&marker.render()), marker);
}

#[test]
fn the_legacy_ok_marker_still_counts_as_ready() {
    let marker = Marker::parse("ok\n");
    assert_eq!(marker, Marker::default());
    assert_eq!(
        Marker::parse("version=\ninstalled_at=oops\n"),
        Marker::default()
    );
}

#[test]
fn state_goes_from_missing_to_incomplete_to_ready() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("runtime");
    assert_eq!(state_of(&dir, ".ok"), InstallState::NotInstalled);

    fs::create_dir_all(&dir).unwrap();
    assert_eq!(state_of(&dir, ".ok"), InstallState::NotInstalled, "empty");

    fs::write(dir.join("partial"), "x").unwrap();
    assert_eq!(state_of(&dir, ".ok"), InstallState::Incomplete);

    Marker::now(Some("9".into()))
        .write(&dir.join(".ok"))
        .unwrap();
    let state = state_of(&dir, ".ok");
    assert!(state.is_ready());
    assert_eq!(state.version(), Some("9"));

    clear_marker(&dir, ".ok").unwrap();
    clear_marker(&dir, ".ok").unwrap(); // already gone: not an error
    assert_eq!(state_of(&dir, ".ok"), InstallState::Incomplete);
}
