use std::fs;

use berry_installer::hash::{
    Checksum, find_in_sums, hex, md5_file, sha256_file, verify_file, verify_file_async,
};
use berry_installer::{CancelToken, InstallError, JobContext};

// Well-known digests of the ASCII string "abc".
const ABC_SHA256: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
const ABC_MD5: &str = "900150983cd24fb0d6963f7d28e17f72";

fn abc_file() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("abc.bin");
    fs::write(&path, b"abc").unwrap();
    (dir, path)
}

#[test]
fn hex_is_lowercase_and_zero_padded() {
    assert_eq!(hex(&[0x00, 0x0f, 0xa0, 0xff]), "000fa0ff");
}

#[test]
fn checksum_constructors_validate_and_normalise() {
    assert_eq!(
        Checksum::sha256(format!("  {}\n", ABC_SHA256.to_uppercase())).unwrap(),
        Checksum::Sha256(ABC_SHA256.to_owned())
    );
    assert_eq!(Checksum::md5(ABC_MD5).unwrap().expected(), ABC_MD5);
    assert!(Checksum::sha256(ABC_MD5).is_err(), "wrong length");
    assert!(Checksum::md5("z".repeat(32)).is_err(), "not hex");
}

#[test]
fn files_are_hashed() {
    let (_dir, path) = abc_file();
    let ctx = JobContext::new();
    assert_eq!(sha256_file(&path, &ctx).unwrap(), ABC_SHA256);
    assert_eq!(md5_file(&path, &ctx).unwrap(), ABC_MD5);
}

#[test]
fn verify_file_accepts_a_match_and_reports_a_mismatch() {
    let (_dir, path) = abc_file();
    let ctx = JobContext::new();
    verify_file(&path, &Checksum::sha256(ABC_SHA256).unwrap(), &ctx).unwrap();
    verify_file(&path, &Checksum::md5(ABC_MD5).unwrap(), &ctx).unwrap();

    let wrong = Checksum::sha256("0".repeat(64)).unwrap();
    match verify_file(&path, &wrong, &ctx) {
        Err(InstallError::ChecksumMismatch {
            file,
            expected,
            actual,
        }) => {
            assert_eq!(file, "abc.bin");
            assert_eq!(expected, "0".repeat(64));
            assert_eq!(actual, ABC_SHA256);
        }
        other => panic!("expected a checksum mismatch, got {other:?}"),
    }
}

#[test]
fn hashing_stops_when_cancelled() {
    let (_dir, path) = abc_file();
    let cancel = CancelToken::new();
    cancel.cancel();
    let ctx = JobContext::new().with_cancel(cancel);
    assert!(sha256_file(&path, &ctx).unwrap_err().is_cancelled());
}

#[test]
fn missing_file_is_an_io_error() {
    let dir = tempfile::tempdir().unwrap();
    let error = sha256_file(&dir.path().join("nope"), &JobContext::new()).unwrap_err();
    assert!(matches!(error, InstallError::Io { .. }), "{error:?}");
}

#[tokio::test]
async fn async_verification_matches_the_blocking_one() {
    let (_dir, path) = abc_file();
    let ctx = JobContext::new();
    verify_file_async(&path, &Checksum::sha256(ABC_SHA256).unwrap(), &ctx)
        .await
        .unwrap();
    let wrong = Checksum::md5("1".repeat(32)).unwrap();
    assert!(verify_file_async(&path, &wrong, &ctx).await.is_err());
}

#[test]
fn sums_files_are_parsed_in_both_styles() {
    let other = "a".repeat(64);
    let sums = format!("{other}  other.tar.xz\n{ABC_SHA256} *wanted.tar.xz\n\nnot a line\n");
    assert_eq!(
        find_in_sums(&sums, "wanted.tar.xz"),
        Some(Checksum::Sha256(ABC_SHA256.to_owned()))
    );
    assert_eq!(
        find_in_sums(&sums, "other.tar.xz"),
        Some(Checksum::Sha256(other))
    );
    assert_eq!(find_in_sums(&sums, "missing"), None);
}
