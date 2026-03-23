use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;
use tempfile::TempDir;

fn pack_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("packs")
        .join("dwbase-memory")
}

fn run_once(data_dir: &TempDir, user_text: &str) -> Value {
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_dwbase-pack-runner"));
    let out = Command::new(exe)
        .env("DWBASE_DATA_DIR", data_dir.path())
        .env("DWBASE_TENANT_ID", "demo")
        .arg("--pack")
        .arg(pack_dir())
        .arg("--flow")
        .arg("memory_demo")
        .arg("--input")
        .arg(format!("user_text={user_text}"))
        .output()
        .expect("runner should start");
    assert!(
        out.status.success(),
        "runner failed: {}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("runner JSON output")
}

#[test]
#[ignore = "dwbase-pack-runner is a deprecated placeholder; binary produces no output"]
fn runs_twice_and_persists_between_runs() {
    let data_dir = TempDir::new().unwrap();

    let first_text = "I live in Berlin.";
    let second_text = "My favorite color is green.";

    let _first = run_once(&data_dir, first_text);
    let second = run_once(&data_dir, second_text);

    let replay = second["outputs"]["replay_recent"].as_array().unwrap();
    let contains_first = replay.iter().any(|a| {
        a["payload_json"]
            .as_str()
            .unwrap_or_default()
            .contains("Berlin")
    });
    assert!(
        contains_first,
        "second run replay should contain prior atoms (persistence across runs)"
    );
}
