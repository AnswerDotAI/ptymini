use ptymini::PtyCore;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("ptymini-test-{tag}-{}-{nanos}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(".zshrc"), "echo hi\n").unwrap();
    dir
}

#[test]
fn terminate_escalates_and_reaps_trapping_child_with_cleanup_dir() {
    let dir = temp_dir("trap");
    let argv = vec!["sh".into(), "-c".into(), "trap '' HUP INT TERM; echo ready; sleep 30".into()];
    let pty = PtyCore::spawn(&argv, None, None, 24, 80, 65_536).unwrap();
    pty.set_cleanup_dir(dir.clone());
    while !String::from_utf8_lossy(&pty.read_from(0, None).0).contains("ready") {
        assert!(pty.wait_change(None, Some(5.0)));
    }
    assert!(!pty.terminate(false));
    assert!(pty.alive() && dir.exists());
    assert!(pty.terminate(true));
    assert!(!pty.alive() && pty.exit_code().is_some());
    assert!(!dir.exists());
}

#[test]
fn cleanup_dir_removed_on_natural_exit_and_post_exit_registration() {
    let dir1 = temp_dir("exit");
    let argv = vec!["sh".into(), "-c".into(), "exit 7".into()];
    let pty = PtyCore::spawn(&argv, None, None, 24, 80, 65_536).unwrap();
    pty.set_cleanup_dir(dir1.clone());
    assert_eq!(pty.wait(Some(5.0)), Some(7));
    assert!(!dir1.exists());

    let dir2 = temp_dir("late");
    pty.set_cleanup_dir(dir2.clone());
    assert!(!dir2.exists());
}
