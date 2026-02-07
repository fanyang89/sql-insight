use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn load_json(path: &Path) -> Value {
    let text = fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("failed reading {}: {err}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|err| {
        panic!("failed parsing json {}: {err}", path.display());
    })
}

fn list_json_files(path: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(path)
        .unwrap_or_else(|err| panic!("failed reading dir {}: {err}", path.display()))
        .filter_map(|it| it.ok().map(|entry| entry.path()))
        .filter(|it| it.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    files.sort();
    files
}

fn validate_against_schema(schema: &Value, instance: &Value, label: &str) {
    let validator = jsonschema::validator_for(schema).expect("invalid schema");
    let errors = validator
        .iter_errors(instance)
        .map(|it| it.to_string())
        .collect::<Vec<_>>();
    assert!(
        errors.is_empty(),
        "schema validation failed for {label}:\n{}",
        errors.join("\n")
    );
}

fn bool_value(v: Option<&Value>) -> bool {
    v.and_then(Value::as_bool).unwrap_or(false)
}

fn summarize_record(file_name: String, record: &Value) -> Value {
    let mut source_status = record["source_status"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|it| {
            json!({
                "source": it["source"],
                "ok": it["ok"],
            })
        })
        .collect::<Vec<_>>();
    source_status.sort_by(|a, b| {
        a["source"]
            .as_str()
            .unwrap_or_default()
            .cmp(b["source"].as_str().unwrap_or_default())
    });

    let level1 = record["payload"]["level1"].as_object();
    let mut categories = level1
        .and_then(|it| it.get("error_log"))
        .and_then(|it| it.get("alerts"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|it| it["category"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    categories.sort();
    categories.dedup();

    let digest_count = level1
        .and_then(|it| it.get("slow_log"))
        .and_then(|it| it.get("digest_count"))
        .and_then(Value::as_u64)
        .unwrap_or(0);

    json!({
        "file": file_name,
        "contract_version": record["contract_version"],
        "engine": record["engine"],
        "requested_level": record["requested_level"],
        "selected_level": record["selected_level"],
        "status": record["status"],
        "source_status": source_status,
        "level1": {
            "enabled": level1.is_some(),
            "hot_switch_ok": bool_value(level1.and_then(|it| it.get("capability")).and_then(|it| it.get("can_enable_slow_log_hot_switch"))),
            "slow_log_ok": bool_value(level1.and_then(|it| it.get("capability")).and_then(|it| it.get("can_read_slow_log"))),
            "error_log_ok": bool_value(level1.and_then(|it| it.get("capability")).and_then(|it| it.get("can_read_error_log"))),
            "digest_count_ge_1": digest_count >= 1,
            "alert_categories": categories
        }
    })
}

fn minimal_mysql_record() -> Value {
    json!({
      "contract_version": "v1",
      "run_id": "run-sample",
      "cycle": 1,
      "engine": "mysql",
      "requested_level": "Level 0",
      "selected_level": "Level 0",
      "schedule": {
        "mode": "once",
        "interval_secs": 60,
        "jitter_pct": 0.1,
        "timeout_secs": 120,
        "retry_times": 1,
        "retry_backoff_ms": 1000,
        "max_cycles": null
      },
      "window": {
        "start_unix_ms": 1,
        "end_unix_ms": 2,
        "duration_ms": 1
      },
      "attempts": [
        {"attempt": 1, "duration_ms": 1, "status": "ok", "error": null}
      ],
      "source_status": [
        {"source": "os.basic_metrics", "ok": true}
      ],
      "warnings": [],
      "status": "ok",
      "error": null,
      "payload": {
        "engine": "mysql",
        "requested_level": "Level 0",
        "selected_level": "Level 0",
        "downgrade_reasons": [],
        "level0": {
          "collected_at_unix_ms": 1,
          "capability": {
            "mysql_connected": true,
            "mysql_status_access": true,
            "mysql_variables_access": true,
            "information_schema_access": true,
            "replication_status_access": true,
            "os_metrics_access": true
          },
          "mysql": {
            "global_status": {},
            "global_variables": {},
            "table_sizes": [],
            "indexes": [],
            "replication_status": null,
            "replication_status_source": "SHOW SLAVE STATUS"
          },
          "os": {
            "proc_cpu": {},
            "proc_mem": {},
            "load_average": {},
            "vmstat": {"command": "vmstat", "args": [], "available": false, "status_code": null, "output": null, "error": null},
            "iostat": {"command": "iostat", "args": [], "available": false, "status_code": null, "output": null, "error": null},
            "sar": {"command": "sar", "args": [], "available": false, "status_code": null, "output": null, "error": null}
          },
          "warnings": []
        },
        "postgres_level0": null,
        "level1": null
      }
    })
}

fn minimal_postgres_level1_record() -> Value {
    json!({
      "contract_version": "v1",
      "run_id": "run-sample-pg",
      "cycle": 1,
      "engine": "postgres",
      "requested_level": "Level 1",
      "selected_level": "Level 1",
      "schedule": {
        "mode": "once",
        "interval_secs": 60,
        "jitter_pct": 0.1,
        "timeout_secs": 120,
        "retry_times": 1,
        "retry_backoff_ms": 1000,
        "max_cycles": null
      },
      "window": {
        "start_unix_ms": 1,
        "end_unix_ms": 2,
        "duration_ms": 1
      },
      "attempts": [
        {"attempt": 1, "duration_ms": 1, "status": "ok", "error": null}
      ],
      "source_status": [
        {"source": "postgres.statement_log", "ok": true}
      ],
      "warnings": [],
      "status": "ok",
      "error": null,
      "payload": {
        "engine": "postgres",
        "requested_level": "Level 1",
        "selected_level": "Level 1",
        "downgrade_reasons": [],
        "level0": {
          "collected_at_unix_ms": 1,
          "capability": {
            "mysql_connected": false,
            "mysql_status_access": false,
            "mysql_variables_access": false,
            "information_schema_access": false,
            "replication_status_access": false,
            "os_metrics_access": true
          },
          "mysql": {
            "global_status": {},
            "global_variables": {},
            "table_sizes": [],
            "indexes": [],
            "replication_status": null,
            "replication_status_source": "SHOW SLAVE STATUS"
          },
          "os": {
            "proc_cpu": {},
            "proc_mem": {},
            "load_average": {},
            "vmstat": {"command": "vmstat", "args": [], "available": false, "status_code": null, "output": null, "error": null},
            "iostat": {"command": "iostat", "args": [], "available": false, "status_code": null, "output": null, "error": null},
            "sar": {"command": "sar", "args": [], "available": false, "status_code": null, "output": null, "error": null}
          },
          "warnings": []
        },
        "postgres_level0": {
          "collected_at_unix_ms": 1,
          "capability": {
            "postgres_connected": true,
            "has_status_access": true,
            "has_settings_access": true,
            "has_storage_access": true,
            "has_replication_status_access": true
          },
          "postgres": {},
          "warnings": []
        },
        "level1": {
          "collected_at_unix_ms": 1,
          "capability": {
            "mysql_connected": false,
            "postgres_connected": true,
            "can_enable_slow_log_hot_switch": true,
            "can_read_slow_log": true,
            "can_read_error_log": true
          },
          "slow_log": {
            "enabled_for_window": true,
            "window_secs": 5,
            "long_query_time_secs": 0.1,
            "slow_log_path": "/var/log/postgresql/postgresql.log",
            "previous_slow_query_log": null,
            "previous_long_query_time": "200ms",
            "collected_bytes": 1,
            "parsed_entries": 1,
            "digest_count": 1,
            "digests": []
          },
          "error_log": {
            "error_log_path": "/var/log/postgresql/postgresql.log",
            "sampled_lines": 3,
            "alert_count": 1,
            "alerts": []
          },
          "warnings": []
        }
      }
    })
}

#[test]
fn schema_accepts_minimal_samples() {
    let schema = load_json(&repo_path("contracts/unified-record-v1.schema.json"));
    validate_against_schema(&schema, &minimal_mysql_record(), "minimal mysql");
    validate_against_schema(
        &schema,
        &minimal_postgres_level1_record(),
        "minimal postgres level1",
    );
}

#[test]
fn matrix_results_match_schema_and_golden_summary() {
    let schema = load_json(&repo_path("contracts/unified-record-v1.schema.json"));
    let default_results = repo_path("tests/artifacts/results");
    let results_dir = env::var("CONTRACT_RESULTS_DIR")
        .map(PathBuf::from)
        .unwrap_or(default_results);
    let require_results = env::var("CONTRACT_REQUIRE_RESULTS")
        .map(|it| it == "1" || it.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if !require_results {
        return;
    }

    if !results_dir.exists() {
        assert!(
            require_results,
            "results dir {} missing",
            results_dir.display()
        );
        return;
    }

    let files = list_json_files(&results_dir);
    if files.is_empty() {
        assert!(
            require_results,
            "no json files under {}",
            results_dir.display()
        );
        return;
    }

    let mut summaries = Vec::new();
    for file in files {
        let instance = load_json(&file);
        let label = file
            .file_name()
            .and_then(|it| it.to_str())
            .unwrap_or("unknown");
        validate_against_schema(&schema, &instance, label);
        summaries.push(summarize_record(label.to_string(), &instance));
    }

    let expected_golden = env::var("CONTRACT_GOLDEN_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_path("tests/contracts/golden/matrix-summary.full.json"));
    let expected = load_json(&expected_golden);
    let actual = Value::Array(summaries);
    assert_eq!(
        actual, expected,
        "matrix summary mismatch; re-run tests/scripts/run-matrix.sh and update tests/contracts/golden/matrix-summary.json if intentional"
    );
}
