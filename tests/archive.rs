use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use berry_installer::archive::{ArchiveKind, ExtractOptions, extract, extract_blocking};
use berry_installer::{CancelToken, InstallError, JobContext};

/// Files of a tar archive: `(path, contents)`.
fn write_tar(path: &Path, files: &[(&str, &str)]) {
    let mut builder = tar::Builder::new(File::create(path).unwrap());
    for (name, contents) in files {
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, name, contents.as_bytes())
            .unwrap();
    }
    builder.finish().unwrap();
}

fn write_tar_gz(path: &Path, files: &[(&str, &str)]) {
    let plain = path.with_extension("plain");
    write_tar(&plain, files);
    let mut encoder =
        flate2::write::GzEncoder::new(File::create(path).unwrap(), flate2::Compression::fast());
    encoder.write_all(&fs::read(&plain).unwrap()).unwrap();
    encoder.finish().unwrap();
    fs::remove_file(plain).unwrap();
}

fn write_zip(path: &Path, files: &[(&str, &str)]) {
    let mut zip = zip::ZipWriter::new(File::create(path).unwrap());
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o755);
    for (name, contents) in files {
        zip.start_file(*name, options).unwrap();
        zip.write_all(contents.as_bytes()).unwrap();
    }
    zip.finish().unwrap();
}

fn read(path: PathBuf) -> String {
    fs::read_to_string(path).unwrap()
}

#[test]
fn kinds_are_guessed_from_the_file_name() {
    use ArchiveKind::*;
    for (name, kind) in [
        ("runtime.tar.xz", Some(TarXz)),
        ("RUNTIME.TXZ", Some(TarXz)),
        ("a.tar.gz", Some(TarGz)),
        ("a.tgz", Some(TarGz)),
        ("a.tar.zst", Some(TarZst)),
        ("a.tzst", Some(TarZst)),
        ("a.tar", Some(Tar)),
        ("a.zip", Some(Zip)),
        ("a.7z", None),
        ("noextension", None),
    ] {
        assert_eq!(ArchiveKind::from_name(name), kind, "{name}");
    }
}

#[test]
fn kinds_are_sniffed_from_the_content_when_the_name_lies() {
    let dir = tempfile::tempdir().unwrap();
    let gz = dir.path().join("download.bin");
    write_tar_gz(&gz, &[("a.txt", "a")]);
    assert_eq!(ArchiveKind::detect(&gz), Some(ArchiveKind::TarGz));

    let zip = dir.path().join("package.dat");
    write_zip(&zip, &[("a.txt", "a")]);
    assert_eq!(ArchiveKind::detect(&zip), Some(ArchiveKind::Zip));

    let text = dir.path().join("notes.txt");
    fs::write(&text, "just text, no magic").unwrap();
    assert_eq!(ArchiveKind::detect(&text), None);
}

#[test]
fn tar_gz_is_extracted() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("a.tar.gz");
    write_tar_gz(&archive, &[("top.txt", "top"), ("sub/inner.txt", "inner")]);
    let dest = dir.path().join("out");

    extract_blocking(
        &archive,
        &dest,
        ExtractOptions::default(),
        &JobContext::new(),
    )
    .unwrap();

    assert_eq!(read(dest.join("top.txt")), "top");
    assert_eq!(read(dest.join("sub/inner.txt")), "inner");
}

#[test]
fn zip_is_extracted_with_its_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("a.zip");
    write_zip(
        &archive,
        &[("bin/run.sh", "#!/bin/sh\n"), ("readme.txt", "hi")],
    );
    let dest = dir.path().join("out");

    extract_blocking(
        &archive,
        &dest,
        ExtractOptions::default(),
        &JobContext::new(),
    )
    .unwrap();

    assert_eq!(read(dest.join("readme.txt")), "hi");
    let mode = fs::metadata(dest.join("bin/run.sh"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o755);
}

#[test]
fn strip_root_drops_the_single_top_level_folder() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("a.tar.gz");
    write_tar_gz(&archive, &[("Root/a.txt", "a"), ("Root/b/c.txt", "c")]);
    let dest = dir.path().join("out");

    extract_blocking(
        &archive,
        &dest,
        ExtractOptions { strip_root: true },
        &JobContext::new(),
    )
    .unwrap();

    assert_eq!(read(dest.join("a.txt")), "a");
    assert_eq!(read(dest.join("b/c.txt")), "c");
    assert!(!dest.join("Root").exists());
}

#[test]
fn strip_root_refuses_a_destination_that_is_not_empty() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("a.tar.gz");
    write_tar_gz(&archive, &[("Root/a.txt", "a")]);
    let dest = dir.path().join("out");
    fs::create_dir(&dest).unwrap();
    fs::write(dest.join("existing"), "x").unwrap();

    let error = extract_blocking(
        &archive,
        &dest,
        ExtractOptions { strip_root: true },
        &JobContext::new(),
    )
    .unwrap_err();
    assert!(matches!(error, InstallError::InvalidData(_)), "{error:?}");
}

#[test]
fn tar_entries_outside_the_destination_are_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("evil.tar");
    // `Header::set_path` refuses `..`, so the hostile name is written into the header by hand.
    let mut builder = tar::Builder::new(File::create(&archive).unwrap());
    for (name, contents) in [("../escaped.txt", "evil"), ("safe.txt", "ok")] {
        let mut header = tar::Header::new_gnu();
        header.as_old_mut().name[..name.len()].copy_from_slice(name.as_bytes());
        header.set_size(contents.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, contents.as_bytes()).unwrap();
    }
    builder.finish().unwrap();
    let dest = dir.path().join("inside/out");

    extract_blocking(
        &archive,
        &dest,
        ExtractOptions::default(),
        &JobContext::new(),
    )
    .unwrap();

    assert_eq!(read(dest.join("safe.txt")), "ok");
    assert!(!dir.path().join("inside/escaped.txt").exists());
    assert!(!dir.path().join("escaped.txt").exists());
}

#[test]
fn zip_entries_outside_the_destination_are_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("evil.zip");
    write_zip(&archive, &[("../escaped.txt", "evil"), ("safe.txt", "ok")]);
    let dest = dir.path().join("inside/out");

    extract_blocking(
        &archive,
        &dest,
        ExtractOptions::default(),
        &JobContext::new(),
    )
    .unwrap();

    assert_eq!(read(dest.join("safe.txt")), "ok");
    assert!(!dir.path().join("inside/escaped.txt").exists());
}

#[test]
fn unknown_formats_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("a.rar");
    fs::write(&archive, "not an archive").unwrap();
    let error = extract_blocking(
        &archive,
        &dir.path().join("out"),
        ExtractOptions::default(),
        &JobContext::new(),
    )
    .unwrap_err();
    assert!(
        matches!(error, InstallError::UnsupportedArchive(_)),
        "{error:?}"
    );
}

#[test]
fn a_corrupt_archive_is_an_error_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("broken.zip");
    fs::write(&archive, b"PK\x03\x04 but then garbage").unwrap();
    assert!(
        extract_blocking(
            &archive,
            &dir.path().join("out"),
            ExtractOptions::default(),
            &JobContext::new(),
        )
        .is_err()
    );
}

#[test]
fn a_cancelled_extraction_reports_cancelled() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("a.tar.gz");
    write_tar_gz(&archive, &[("a.txt", "a")]);
    let cancel = CancelToken::new();
    cancel.cancel();
    let error = extract_blocking(
        &archive,
        &dir.path().join("out"),
        ExtractOptions::default(),
        &JobContext::new().with_cancel(cancel),
    )
    .unwrap_err();
    assert!(error.is_cancelled(), "{error:?}");
}

#[tokio::test]
async fn the_async_wrapper_extracts_too() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("a.zip");
    write_zip(&archive, &[("a.txt", "async")]);
    let dest = dir.path().join("out");
    extract(
        &archive,
        &dest,
        ExtractOptions::default(),
        &JobContext::new(),
    )
    .await
    .unwrap();
    assert_eq!(read(dest.join("a.txt")), "async");
}
