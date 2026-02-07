use std::cmp::Reverse;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mysql::prelude::Queryable;
use mysql::{Opts, Pool, PooledConn};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct Level1CollectorConfig {
    pub mysql_url: Option<String>,
    pub slow_log_window_secs: u64,
    pub slow_log_long_query_time_secs: f64,
    pub enable_slow_log_hot_switch: bool,
    pub restore_slow_log_settings: bool,
    pub slow_log_path: Option<String>,
    pub error_log_path: Option<String>,
    pub max_slow_log_bytes: usize,
    pub max_error_log_bytes: usize,
    pub max_error_log_lines: usize,
}

impl Default for Level1CollectorConfig {
    fn default() -> Self {
        Self {
            mysql_url: None,
            slow_log_window_secs: 30,
            slow_log_long_query_time_secs: 0.2,
            enable_slow_log_hot_switch: true,
            restore_slow_log_settings: true,
            slow_log_path: None,
            error_log_path: None,
            max_slow_log_bytes: 2_000_000,
            max_error_log_bytes: 2_000_000,
            max_error_log_lines: 2_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct Level1Capability {
    pub mysql_connected: bool,
    pub can_enable_slow_log_hot_switch: bool,
    pub can_read_slow_log: bool,
    pub can_read_error_log: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct Level1CollectionReport {
    pub collected_at_unix_ms: u128,
    pub capability: Level1Capability,
    pub slow_log: SlowLogSnapshot,
    pub error_log: ErrorLogSnapshot,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SlowLogSnapshot {
    pub enabled_for_window: bool,
    pub window_secs: u64,
    pub long_query_time_secs: f64,
    pub slow_log_path: Option<String>,
    pub previous_slow_query_log: Option<String>,
    pub previous_long_query_time: Option<String>,
    pub collected_bytes: usize,
    pub parsed_entries: usize,
    pub digest_count: usize,
    pub digests: Vec<SlowSqlDigest>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SlowSqlDigest {
    pub fingerprint: String,
    pub sample_sql: String,
    pub count: u64,
    pub total_query_time_secs: f64,
    pub avg_query_time_secs: f64,
    pub total_lock_time_secs: f64,
    pub total_rows_sent: u64,
    pub total_rows_examined: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ErrorLogSnapshot {
    pub error_log_path: Option<String>,
    pub sampled_lines: usize,
    pub alert_count: usize,
    pub alerts: Vec<ErrorAlert>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ErrorAlert {
    pub category: String,
    pub count: u64,
    pub sample_lines: Vec<String>,
}

pub fn collect_level1(config: &Level1CollectorConfig) -> Level1CollectionReport {
    let mut report = Level1CollectionReport {
        collected_at_unix_ms: now_unix_ms(),
        ..Level1CollectionReport::default()
    };

    let Some(mysql_url) = config.mysql_url.as_deref() else {
        report
            .warnings
            .push("MYSQL_URL not provided; skip Level 1 collection".to_string());
        return report;
    };

    let opts = match Opts::from_url(mysql_url) {
        Ok(it) => it,
        Err(err) => {
            report
                .warnings
                .push(format!("invalid MYSQL_URL format: {err}"));
            return report;
        }
    };

    let pool = match Pool::new(opts) {
        Ok(it) => it,
        Err(err) => {
            report
                .warnings
                .push(format!("failed to create MySQL pool: {err}"));
            return report;
        }
    };

    let mut conn = match pool.get_conn() {
        Ok(it) => it,
        Err(err) => {
            report
                .warnings
                .push(format!("failed to connect MySQL: {err}"));
            return report;
        }
    };
    report.capability.mysql_connected = true;

    collect_slow_log(&mut conn, config, &mut report);
    collect_error_log(&mut conn, config, &mut report);
    report
}

fn collect_slow_log(
    conn: &mut PooledConn,
    config: &Level1CollectorConfig,
    report: &mut Level1CollectionReport,
) {
    report.slow_log.window_secs = config.slow_log_window_secs;
    report.slow_log.long_query_time_secs = config.slow_log_long_query_time_secs;
    report.slow_log.previous_slow_query_log = fetch_variable(conn, "slow_query_log")
        .map_err(|it| it.to_string())
        .ok()
        .flatten();
    report.slow_log.previous_long_query_time = fetch_variable(conn, "long_query_time")
        .map_err(|it| it.to_string())
        .ok()
        .flatten();
    let discovered_slow_log_path = fetch_variable(conn, "slow_query_log_file")
        .map_err(|it| it.to_string())
        .ok()
        .flatten()
        .filter(|it| !it.trim().is_empty());
    let slow_log_path = config
        .slow_log_path
        .clone()
        .or(discovered_slow_log_path)
        .filter(|it| !it.trim().is_empty());
    report.slow_log.slow_log_path = slow_log_path.clone();

    let Some(slow_log_path) = slow_log_path else {
        report.warnings.push(
            "slow log path unavailable (provide --slow-log-path or MySQL slow_query_log_file)"
                .to_string(),
        );
        return;
    };

    let initial_offset = file_len(&slow_log_path).unwrap_or(0);

    if config.enable_slow_log_hot_switch {
        let set_long = conn.query_drop(format!(
            "SET GLOBAL long_query_time = {:.6}",
            config.slow_log_long_query_time_secs
        ));
        let set_slow_log = conn.query_drop("SET GLOBAL slow_query_log = 'ON'");
        if set_long.is_ok() && set_slow_log.is_ok() {
            report.capability.can_enable_slow_log_hot_switch = true;
            report.slow_log.enabled_for_window = true;
        } else {
            if let Err(err) = set_long {
                report
                    .warnings
                    .push(format!("failed to set long_query_time: {err}"));
            }
            if let Err(err) = set_slow_log {
                report
                    .warnings
                    .push(format!("failed to enable slow_query_log: {err}"));
            }
        }
    }

    if config.slow_log_window_secs > 0 {
        std::thread::sleep(Duration::from_secs(config.slow_log_window_secs));
    }

    match read_file_appended_segment(&slow_log_path, initial_offset, config.max_slow_log_bytes) {
        Ok(segment) => {
            report.capability.can_read_slow_log = true;
            report.slow_log.collected_bytes = segment.len();
            let parsed_entries = parse_slow_log_entries(&segment);
            report.slow_log.parsed_entries = parsed_entries.len();
            report.slow_log.digests = aggregate_slow_sql_digests(parsed_entries);
            report.slow_log.digest_count = report.slow_log.digests.len();
        }
        Err(err) => report.warnings.push(format!(
            "failed reading slow log file {slow_log_path}: {err}"
        )),
    }

    if config.enable_slow_log_hot_switch && config.restore_slow_log_settings {
        restore_slow_log_settings(conn, report);
    }
}

fn collect_error_log(
    conn: &mut PooledConn,
    config: &Level1CollectorConfig,
    report: &mut Level1CollectionReport,
) {
    let discovered_error_log_path = fetch_variable(conn, "log_error")
        .map_err(|it| it.to_string())
        .ok()
        .flatten()
        .filter(|it| !it.trim().is_empty() && it != "stderr");
    let error_log_path = config
        .error_log_path
        .clone()
        .or(discovered_error_log_path)
        .filter(|it| !it.trim().is_empty());
    report.error_log.error_log_path = error_log_path.clone();

    let Some(path) = error_log_path else {
        report.warnings.push(
            "error log path unavailable (provide --error-log-path or MySQL log_error)".to_string(),
        );
        return;
    };

    match read_file_tail(&path, config.max_error_log_bytes) {
        Ok(raw) => {
            report.capability.can_read_error_log = true;
            let lines = take_last_lines(&raw, config.max_error_log_lines);
            report.error_log.sampled_lines = lines.len();
            report.error_log.alerts = extract_error_log_alerts(&lines);
            report.error_log.alert_count = report.error_log.alerts.len();
        }
        Err(err) => report
            .warnings
            .push(format!("failed reading error log file {path}: {err}")),
    }
}

fn restore_slow_log_settings(conn: &mut PooledConn, report: &mut Level1CollectionReport) {
    if let Some(previous) = report.slow_log.previous_long_query_time.clone() {
        match previous.parse::<f64>() {
            Ok(value) => {
                if let Err(err) =
                    conn.query_drop(format!("SET GLOBAL long_query_time = {:.6}", value))
                {
                    report
                        .warnings
                        .push(format!("failed to restore long_query_time: {err}"));
                }
            }
            Err(_) => report.warnings.push(format!(
                "skip restoring long_query_time due to non-numeric previous value: {previous}"
            )),
        }
    }

    if let Some(previous) = report.slow_log.previous_slow_query_log.clone() {
        let target = if is_mysql_truthy(&previous) {
            "ON"
        } else {
            "OFF"
        };
        if let Err(err) = conn.query_drop(format!("SET GLOBAL slow_query_log = '{target}'")) {
            report
                .warnings
                .push(format!("failed to restore slow_query_log: {err}"));
        }
    }
}

fn fetch_variable(conn: &mut PooledConn, name: &str) -> Result<Option<String>, mysql::Error> {
    let sql = format!("SHOW VARIABLES LIKE '{name}'");
    let row: Option<(String, String)> = conn.query_first(sql)?;
    Ok(row.map(|(_, value)| value))
}

#[derive(Debug, Clone)]
struct SlowLogEntry {
    sql: String,
    query_time_secs: f64,
    lock_time_secs: f64,
    rows_sent: u64,
    rows_examined: u64,
}

#[derive(Debug, Default)]
struct SlowLogBuilder {
    sql_lines: Vec<String>,
    query_time_secs: f64,
    lock_time_secs: f64,
    rows_sent: u64,
    rows_examined: u64,
}

fn parse_slow_log_entries(content: &str) -> Vec<SlowLogEntry> {
    let mut entries = Vec::new();
    let mut current: Option<SlowLogBuilder> = None;

    for line in content.lines() {
        let trimmed = line.trim();
        if line.starts_with("# Time:") {
            finalize_entry(&mut entries, current.take());
            current = Some(SlowLogBuilder::default());
            continue;
        }

        let Some(ref mut item) = current else {
            continue;
        };

        if line.contains("Query_time:") {
            item.query_time_secs = extract_f64_metric(line, "Query_time").unwrap_or_default();
            item.lock_time_secs = extract_f64_metric(line, "Lock_time").unwrap_or_default();
            item.rows_sent = extract_u64_metric(line, "Rows_sent").unwrap_or_default();
            item.rows_examined = extract_u64_metric(line, "Rows_examined").unwrap_or_default();
            continue;
        }
        if trimmed.starts_with('#') || trimmed.starts_with("SET timestamp=") || trimmed.is_empty() {
            continue;
        }

        item.sql_lines.push(trimmed.to_string());
    }
    finalize_entry(&mut entries, current);
    entries
}

fn finalize_entry(entries: &mut Vec<SlowLogEntry>, item: Option<SlowLogBuilder>) {
    let Some(item) = item else {
        return;
    };
    let sql = item.sql_lines.join("\n").trim().to_string();
    if sql.is_empty() {
        return;
    }
    entries.push(SlowLogEntry {
        sql,
        query_time_secs: item.query_time_secs,
        lock_time_secs: item.lock_time_secs,
        rows_sent: item.rows_sent,
        rows_examined: item.rows_examined,
    });
}

fn aggregate_slow_sql_digests(entries: Vec<SlowLogEntry>) -> Vec<SlowSqlDigest> {
    #[derive(Debug, Default)]
    struct Agg {
        sample_sql: String,
        count: u64,
        total_query_time_secs: f64,
        total_lock_time_secs: f64,
        total_rows_sent: u64,
        total_rows_examined: u64,
    }

    let mut grouped: HashMap<String, Agg> = HashMap::new();
    for entry in entries {
        let fingerprint = normalize_sql_fingerprint(&entry.sql);
        let agg = grouped.entry(fingerprint).or_default();
        if agg.sample_sql.is_empty() {
            agg.sample_sql = entry.sql.clone();
        }
        agg.count += 1;
        agg.total_query_time_secs += entry.query_time_secs;
        agg.total_lock_time_secs += entry.lock_time_secs;
        agg.total_rows_sent += entry.rows_sent;
        agg.total_rows_examined += entry.rows_examined;
    }

    let mut digests = grouped
        .into_iter()
        .map(|(fingerprint, agg)| SlowSqlDigest {
            fingerprint,
            sample_sql: agg.sample_sql,
            count: agg.count,
            total_query_time_secs: agg.total_query_time_secs,
            avg_query_time_secs: if agg.count == 0 {
                0.0
            } else {
                agg.total_query_time_secs / agg.count as f64
            },
            total_lock_time_secs: agg.total_lock_time_secs,
            total_rows_sent: agg.total_rows_sent,
            total_rows_examined: agg.total_rows_examined,
        })
        .collect::<Vec<_>>();

    digests.sort_by_key(|it| {
        (
            Reverse((it.total_query_time_secs * 1_000_000.0) as u64),
            Reverse(it.count),
        )
    });
    digests
}

fn normalize_sql_fingerprint(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut prev_space = false;

    while let Some(ch) = chars.next() {
        if in_single_quote {
            if ch == '\'' {
                in_single_quote = false;
                out.push('?');
                prev_space = false;
            }
            continue;
        }
        if in_double_quote {
            if ch == '"' {
                in_double_quote = false;
                out.push('?');
                prev_space = false;
            }
            continue;
        }

        if ch == '\'' {
            in_single_quote = true;
            continue;
        }
        if ch == '"' {
            in_double_quote = true;
            continue;
        }
        if ch.is_ascii_digit() {
            out.push('?');
            while let Some(next) = chars.peek() {
                if next.is_ascii_digit() || *next == '.' {
                    chars.next();
                } else {
                    break;
                }
            }
            prev_space = false;
            continue;
        }
        if ch.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
            continue;
        }
        out.push(ch.to_ascii_lowercase());
        prev_space = false;
    }

    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn extract_f64_metric(line: &str, key: &str) -> Option<f64> {
    let marker = format!("{key}:");
    let idx = line.find(&marker)?;
    let value = line[(idx + marker.len())..].split_whitespace().next()?;
    value.parse::<f64>().ok()
}

fn extract_u64_metric(line: &str, key: &str) -> Option<u64> {
    let marker = format!("{key}:");
    let idx = line.find(&marker)?;
    let value = line[(idx + marker.len())..].split_whitespace().next()?;
    value.parse::<u64>().ok()
}

fn extract_error_log_alerts(lines: &[String]) -> Vec<ErrorAlert> {
    #[derive(Default)]
    struct Agg {
        count: u64,
        sample_lines: Vec<String>,
    }

    let mut grouped: HashMap<&'static str, Agg> = HashMap::new();
    for line in lines {
        let lower = line.to_ascii_lowercase();
        for category in classify_error_line(&lower) {
            let agg = grouped.entry(category).or_default();
            agg.count += 1;
            if agg.sample_lines.len() < 3 {
                agg.sample_lines.push(line.clone());
            }
        }
    }

    let mut alerts = grouped
        .into_iter()
        .map(|(category, agg)| ErrorAlert {
            category: category.to_string(),
            count: agg.count,
            sample_lines: agg.sample_lines,
        })
        .collect::<Vec<_>>();
    alerts.sort_by_key(|it| Reverse(it.count));
    alerts
}

fn classify_error_line(lower: &str) -> Vec<&'static str> {
    let mut categories = Vec::new();

    if lower.contains("deadlock") {
        categories.push("deadlock");
    }
    if lower.contains("crash recovery")
        || lower.contains("starting crash recovery")
        || lower.contains("recovery completed")
    {
        categories.push("crash_recovery");
    }
    if lower.contains("purge") {
        categories.push("purge");
    }
    if lower.contains("replication")
        || lower.contains("replica")
        || lower.contains("slave")
        || lower.contains("relay log")
        || lower.contains("group replication")
        || lower.contains("binlog")
    {
        categories.push("replication");
    }
    categories
}

fn read_file_appended_segment(path: &str, offset: u64, max_bytes: usize) -> io::Result<String> {
    let len = file_len(path)?;
    let mut start = offset.min(len);
    if len < offset {
        start = len.saturating_sub(max_bytes as u64);
    }
    if len.saturating_sub(start) > max_bytes as u64 {
        start = len.saturating_sub(max_bytes as u64);
    }
    read_file_range(path, start, len)
}

fn read_file_tail(path: &str, max_bytes: usize) -> io::Result<String> {
    let len = file_len(path)?;
    let start = len.saturating_sub(max_bytes as u64);
    read_file_range(path, start, len)
}

fn read_file_range(path: &str, start: u64, end: u64) -> io::Result<String> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut buffer = Vec::with_capacity(end.saturating_sub(start) as usize);
    file.take(end.saturating_sub(start))
        .read_to_end(&mut buffer)?;
    Ok(String::from_utf8_lossy(&buffer).to_string())
}

fn file_len(path: &str) -> io::Result<u64> {
    Ok(std::fs::metadata(path)?.len())
}

fn take_last_lines(content: &str, max_lines: usize) -> Vec<String> {
    let mut lines = content
        .lines()
        .map(|it| it.trim().to_string())
        .filter(|it| !it.is_empty())
        .collect::<Vec<_>>();
    if lines.len() > max_lines {
        lines = lines.split_off(lines.len() - max_lines);
    }
    lines
}

fn is_mysql_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "on" | "1" | "true"
    )
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|it| it.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_aggregate_slow_log_by_fingerprint() {
        let fixture = r#"
# Time: 2026-02-07T12:00:00.100000Z
# Query_time: 1.200 Lock_time: 0.010 Rows_sent: 1 Rows_examined: 100
SET timestamp=1770430000;
SELECT * FROM orders WHERE id = 100;
# Time: 2026-02-07T12:00:02.100000Z
# Query_time: 0.800 Lock_time: 0.002 Rows_sent: 1 Rows_examined: 90
SET timestamp=1770430002;
SELECT * FROM orders WHERE id = 101;
"#;

        let entries = parse_slow_log_entries(fixture);
        let digests = aggregate_slow_sql_digests(entries);
        assert_eq!(digests.len(), 1);
        assert_eq!(digests[0].count, 2);
        assert!(digests[0].fingerprint.contains("where id = ?"));
    }

    #[test]
    fn should_extract_error_alerts() {
        let lines = vec![
            "InnoDB: Deadlock found when trying to get lock".to_string(),
            "InnoDB: Starting crash recovery from checkpoint".to_string(),
            "replication applier thread stopped with error".to_string(),
            "InnoDB: purge lag increased".to_string(),
        ];
        let alerts = extract_error_log_alerts(&lines);
        assert!(alerts.iter().any(|it| it.category == "deadlock"));
        assert!(alerts.iter().any(|it| it.category == "crash_recovery"));
        assert!(alerts.iter().any(|it| it.category == "replication"));
        assert!(alerts.iter().any(|it| it.category == "purge"));
    }

    #[test]
    fn should_normalize_sql_fingerprint() {
        let sql = r#"SELECT * FROM t WHERE user_id = 123 AND name = "alice" AND score > 1.5"#;
        let fp = normalize_sql_fingerprint(sql);
        assert_eq!(
            fp,
            "select * from t where user_id = ? and name = ? and score > ?"
        );
    }
}
