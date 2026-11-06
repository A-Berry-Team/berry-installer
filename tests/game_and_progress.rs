use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use berry_installer::game::{
    INSTALLING_MARKER, Unfinished, begin_install, clear_staging, finish_install, inspect,
    installed_version, unfinished_work, write_version,
};
use berry_installer::{
    CancelToken, GameState, InstallError, JobContext, PatchStep, Phase, Progress, Reporter,
};

#[test]
fn a_fresh_directory_is_not_installed_until_the_executable_exists() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(inspect(dir.path(), false), GameState::NotInstalled);
    let state = inspect(dir.path(), true);
    assert!(state.is_ready());
    assert!(!state.needs_download());
    assert_eq!(state.version(), None);
}

#[test]
fn the_install_marker_makes_a_game_resumable_until_it_is_finished() {
    let dir = tempfile::tempdir().unwrap();
    let game = dir.path().join("game");

    begin_install(&game).unwrap();
    assert!(game.join(INSTALLING_MARKER).is_file());
    let state = inspect(&game, true);
    assert_eq!(
        state,
        GameState::Incomplete(Unfinished::InterruptedOperation)
    );
    assert!(state.is_resumable() && state.needs_download());

    write_version(&game, "5.0.1").unwrap();
    finish_install(&game).unwrap();
    finish_install(&game).unwrap(); // idempotent
    let state = inspect(&game, true);
    assert_eq!(
        state,
        GameState::Ready {
            version: Some("5.0.1".into())
        }
    );
    assert_eq!(state.version(), Some("5.0.1"));
}

#[test]
fn leftover_staging_directories_are_unfinished_work_and_can_be_cleared() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("downloading")).unwrap();
    assert_eq!(
        unfinished_work(dir.path()),
        Some(Unfinished::Staging("downloading"))
    );
    assert!(inspect(dir.path(), true).is_resumable());

    clear_staging(dir.path()).unwrap();
    assert_eq!(unfinished_work(dir.path()), None);
}

#[test]
fn the_version_falls_back_to_another_launchers_config() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(installed_version(dir.path()), None);

    fs::write(
        dir.path().join("config.ini"),
        "[General]\n game_version=4.2.0 \n",
    )
    .unwrap();
    assert_eq!(installed_version(dir.path()).as_deref(), Some("4.2.0"));

    write_version(dir.path(), "5.0.0").unwrap();
    assert_eq!(
        installed_version(dir.path()).as_deref(),
        Some("5.0.0"),
        "ours wins"
    );
}

#[test]
fn progress_fraction_is_clamped_and_safe_without_a_total() {
    assert_eq!(Progress::new(Phase::Downloading, 5, 0).fraction(), 0.0);
    assert_eq!(Progress::new(Phase::Downloading, 50, 200).fraction(), 0.25);
    assert_eq!(Progress::new(Phase::Downloading, 300, 200).fraction(), 1.0);
}

#[test]
fn phases_are_ordered_and_only_the_point_of_no_return_is_critical() {
    assert!(Phase::Resolving < Phase::Downloading);
    assert!(Phase::Verifying < Phase::Extracting);
    assert!(Phase::Extracting < Phase::Patching);
    assert!(Phase::Patching < Phase::Finalizing);
    assert!(Phase::Finalizing.is_critical());
    assert!(Phase::Patching.is_critical());
    assert!(!Phase::Extracting.is_critical());
}

#[test]
fn cancellation_is_shared_between_clones() {
    let token = CancelToken::new();
    let clone = token.clone();
    assert!(token.check().is_ok());
    clone.cancel();
    assert!(token.is_cancelled());
    assert!(token.check().unwrap_err().is_cancelled());
}

#[test]
fn a_token_can_wrap_a_flag_the_caller_already_owns() {
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let token = CancelToken::from_flag(Arc::clone(&flag));
    flag.store(true, Ordering::SeqCst);
    assert!(token.is_cancelled());
}

#[tokio::test]
async fn guard_gives_up_when_cancelled_and_passes_the_value_otherwise() {
    let token = CancelToken::new();
    assert_eq!(token.guard(async { 7 }).await.unwrap(), 7);

    let waiter = token.clone();
    let canceller = tokio::spawn({
        let token = token.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            token.cancel();
        }
    });
    let result = waiter.guard(std::future::pending::<()>()).await;
    canceller.await.unwrap();
    assert!(matches!(result, Err(InstallError::Cancelled)));
    assert!(token.guard(async {}).await.is_err(), "already cancelled");
}

#[test]
fn the_reporter_throttles_hot_updates_but_always_delivers_forced_ones() {
    let seen = Arc::new(AtomicUsize::new(0));
    let reporter = Reporter::new({
        let seen = Arc::clone(&seen);
        move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
        }
    });
    for current in 0..1000 {
        reporter.report(Progress::new(Phase::Downloading, current, 1000));
    }
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "a tight loop yields one update"
    );

    reporter.report_now(Progress::new(Phase::Finalizing, 1, 1));
    reporter.report_now(Progress::new(Phase::Finalizing, 1, 1));
    assert_eq!(seen.load(Ordering::SeqCst), 3);
}

#[test]
fn a_context_without_a_sink_ignores_progress() {
    let ctx = JobContext::new();
    ctx.reporter
        .report_now(Progress::new(Phase::Resolving, 0, 0));
    ctx.reporter.report(Progress::new(Phase::Resolving, 0, 0));
}

#[test]
fn a_context_delivers_progress_to_its_callback() {
    let phases = Arc::new(Mutex::new(Vec::new()));
    let ctx = JobContext::new().with_progress({
        let phases = Arc::clone(&phases);
        move |p| phases.lock().unwrap().push(p.phase)
    });
    ctx.reporter
        .report_now(Progress::new(Phase::Extracting, 0, 0));
    assert_eq!(*phases.lock().unwrap(), [Phase::Extracting]);
}

#[test]
fn only_network_trouble_and_retryable_statuses_are_transient() {
    let status = |status| InstallError::HttpStatus {
        status,
        url: "https://example.invalid".into(),
    };
    assert!(InstallError::Network("reset".into()).is_transient());
    assert!(status(503).is_transient());
    assert!(status(429).is_transient());
    assert!(status(408).is_transient());
    assert!(!status(404).is_transient());
    assert!(!InstallError::Cancelled.is_transient());
    assert!(InstallError::Cancelled.is_cancelled());
}

#[test]
fn patch_steps_reach_their_sink_whatever_the_builder_order() {
    let step = PatchStep {
        index: 3,
        total: 9,
        bytes: 1024,
        done_bytes: 2048,
        total_bytes: 9000,
    };
    let seen: Arc<Mutex<Vec<PatchStep>>> = Arc::default();

    let sink = seen.clone();
    let patch_first = JobContext::new()
        .with_patch_progress(move |step| sink.lock().unwrap().push(step))
        .with_progress(|_| {});
    patch_first.reporter.patch(step);

    let sink = seen.clone();
    let progress_first = JobContext::new()
        .with_progress(|_| {})
        .with_patch_progress(move |step| sink.lock().unwrap().push(step));
    progress_first.reporter.patch(step);

    assert_eq!(*seen.lock().unwrap(), vec![step, step]);
    // Without a patch sink announcing a patch is a no-op.
    JobContext::new().reporter.patch(step);
}
