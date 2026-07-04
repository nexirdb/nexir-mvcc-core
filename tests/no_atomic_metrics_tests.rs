use std::fs;
use std::path::PathBuf;

#[test]
fn test_no_forbidden_concurrency_or_metrics_in_src() {
    let forbidden_patterns = [
        "AtomicUsize",
        "AtomicU64",
        "fetch_add",
        "metrics::",
        "prometheus",
        "tokio::",
    ];

    let mut src_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    src_dir.push("src");

    let mut files_to_check = vec![src_dir];
    let mut checked_files = 0;

    while let Some(path) = files_to_check.pop() {
        if path.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                let entry = entry.unwrap();
                files_to_check.push(entry.path());
            }
        } else if path.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
            let content = fs::read_to_string(&path).unwrap();
            for pattern in &forbidden_patterns {
                if content.contains(pattern) {
                    panic!(
                        "Forbidden pattern '{}' found in core MVCC file: {}\n\
                        Concurrency and metrics belong in the adapter layer. See docs/ADAPTER_CONTRACT.md",
                        pattern,
                        path.display()
                    );
                }
            }
            checked_files += 1;
        }
    }

    assert!(checked_files > 0, "No source files found to check!");
}
