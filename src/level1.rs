use std::cmp::Reverse;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mysql::prelude::Queryable;
use mysql::{Opts, Pool, PooledConn};
use postgres::{Client, NoTls};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level1Engine {
    Mysql,
    Postgres,
}

#[derive(Debug, Clone)]
pub struct Level1CollectorConfig {
    pub engine: Level1Engine,
    pub mysql_url: Option<String>,
    pub postgres_url: Option<String>,
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
            engine: Level1Engine::Mysql,
            mysql_url: None,
            postgres_url: None,
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
    pub postgres_connected: bool,
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

    match config.engine {
        Level1Engine::Mysql => collect_mysql_level1(config, &mut report),
        Level1Engine::Postgres => collect_postgres_level1(config, &mut report),
    }

    report
}

fn collect_mysql_level1(config: &Level1CollectorConfig, report: &mut Level1CollectionReport) {
    let Some(mysql_url) = config.mysql_url.as_deref() else {
        report
            .warnings
            .push("MYSQL_URL not provided; skip Level 1 collection".to_string());
        return;
    };

    let opts = match Opts::from_url(mysql_url) {
        Ok(it) => it,
        Err(err) => {
            report
                .warnings
                .push(format!("invalid MYSQL_URL format: {err}"));
            return;
        }
    };

    let pool = match Pool::new(opts) {
        Ok(it) => it,
        Err(err) => {
            report
                .warnings
                .push(format!("failed to create MySQL pool: {err}"));
            return;
        }
    };

    let mut conn = match pool.get_conn() {
        Ok(it) => it,
        Err(err) => {
            report
                .warnings
                .push(format!("failed to connect MySQL: {err}"));
            return;
        }
    };
    report.capability.mysql_connected = true;

    collect_mysql_slow_log(&mut conn, config, report);
    collect_mysql_error_log(&mut conn, config, report);
}

fn collect_postgres_level1(config: &Level1CollectorConfig, report: &mut Level1CollectionReport) {
    let Some(postgres_url) = config.postgres_url.as_deref() else {
        report
            .warnings
            .push("POSTGRES_URL not provided; skip Level 1 collection".to_string());
        return;
    };

    let mut client = match Client::connect(postgres_url, NoTls) {
        Ok(it) => it,
        Err(err) => {
            report
                .warnings
                .push(format!("failed to connect PostgreSQL: {err}"));
            return;
        }
    };
    report.capability.postgres_connected = true;

    collect_postgres_statement_log(&mut client, config, report);
    collect_postgres_error_log(&mut client, config, report);
}

fn collect_mysql_slow_log(
    conn: &mut PooledConn,
    config: &Level1CollectorConfig,
    report: &mut Level1CollectionReport,
) {
    report.slow_log.window_secs = config.slow_log_window_secs;
    report.slow_log.long_query_time_secs = config.slow_log_long_query_time_secs;
    report.slow_log.previous_slow_query_log = fetch_mysql_variable(conn, "slow_query_log")
        .map_err(|it| it.to_string())
        .ok()
        .flatten();
    report.slow_log.previous_long_query_time = fetch_mysql_variable(conn, "long_query_time")
        .map_err(|it| it.to_string())
        .ok()
        .flatten();
    let discovered_slow_log_path = fetch_mysql_variable(conn, "slow_query_log_file")
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
            let parsed_entries = parse_mysql_slow_log_entries(&segment);
            report.slow_log.parsed_entries = parsed_entries.len();
            report.slow_log.digests = aggregate_slow_sql_digests(parsed_entries);
            report.slow_log.digest_count = report.slow_log.digests.len();
        }
        Err(err) => report.warnings.push(format!(
            "failed reading slow log file {slow_log_path}: {err}"
        )),
    }

    if config.enable_slow_log_hot_switch && config.restore_slow_log_settings {
        restore_mysql_slow_log_settings(conn, report);
    }
}

fn collect_postgres_statement_log(
    client: &mut Client,
    config: &Level1CollectorConfig,
    report: &mut Level1CollectionReport,
) {
    report.slow_log.window_secs = config.slow_log_window_secs;
    report.slow_log.long_query_time_secs = config.slow_log_long_query_time_secs;
    let discovered_slow_log_path = discover_postgres_log_path(client)
        .map_err(|it| it.to_string())
        .ok()
        .flatten();
    let slow_log_path = config
        .slow_log_path
        .clone()
        .or(discovered_slow_log_path)
        .filter(|it| !it.trim().is_empty());
    report.slow_log.slow_log_path = slow_log_path.clone();
    report.slow_log.previous_long_query_time =
        fetch_postgres_setting(client, "log_min_duration_statement")
            .map_err(|it| it.to_string())
            .ok()
            .flatten();

    let Some(slow_log_path) = slow_log_path else {
        report.warnings.push(
            "postgres statement log path unavailable (provide --slow-log-path or enable logging_collector with local log access)".to_string(),
        );
        return;
    };

    let initial_offset = file_len(&slow_log_path).unwrap_or(0);

    if config.enable_slow_log_hot_switch {
        let threshold_ms = (config.slow_log_long_query_time_secs * 1000.0)
            .max(1.0)
            .round() as u64;
        let sql = format!("ALTER SYSTEM SET log_min_duration_statement = {threshold_ms};");
        let set_threshold = client.batch_execute(&sql);
        let reload = client.simple_query("SELECT pg_reload_conf();").map(|_| ());
        if set_threshold.is_ok() && reload.is_ok() {
            report.capability.can_enable_slow_log_hot_switch = true;
            report.slow_log.enabled_for_window = true;
        } else {
            if let Err(err) = set_threshold {
                report.warnings.push(format!(
                    "failed to set PostgreSQL log_min_duration_statement: {err}"
                ));
            }
            if let Err(err) = reload {
                report
                    .warnings
                    .push(format!("failed to reload PostgreSQL config: {err}"));
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
            let parsed_entries = parse_postgres_statement_log_entries(&segment);
            report.slow_log.parsed_entries = parsed_entries.len();
            report.slow_log.digests = aggregate_slow_sql_digests(parsed_entries);
            report.slow_log.digest_count = report.slow_log.digests.len();
        }
        Err(err) => report.warnings.push(format!(
            "failed reading postgres statement log file {slow_log_path}: {err}"
        )),
    }

    if config.enable_slow_log_hot_switch && config.restore_slow_log_settings {
        restore_postgres_statement_settings(client, report);
    }
}

fn collect_mysql_error_log(
    conn: &mut PooledConn,
    config: &Level1CollectorConfig,
    report: &mut Level1CollectionReport,
) {
    let discovered_error_log_path = fetch_mysql_variable(conn, "log_error")
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

fn collect_postgres_error_log(
    _client: &mut Client,
    config: &Level1CollectorConfig,
    report: &mut Level1CollectionReport,
) {
    let error_log_path = config
        .error_log_path
        .clone()
        .or_else(|| report.slow_log.slow_log_path.clone())
        .filter(|it| !it.trim().is_empty());
    report.error_log.error_log_path = error_log_path.clone();

    let Some(path) = error_log_path else {
        report.warnings.push(
            "postgres error log path unavailable (provide --error-log-path or --slow-log-path)"
                .to_string(),
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
        Err(err) => report.warnings.push(format!(
            "failed reading postgres error log file {path}: {err}"
        )),
    }
}

fn restore_mysql_slow_log_settings(conn: &mut PooledConn, report: &mut Level1CollectionReport) {
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
        let target = if is_truthy(&previous) { "ON" } else { "OFF" };
        if let Err(err) = conn.query_drop(format!("SET GLOBAL slow_query_log = '{target}'")) {
            report
                .warnings
                .push(format!("failed to restore slow_query_log: {err}"));
        }
    }
}

fn restore_postgres_statement_settings(client: &mut Client, report: &mut Level1CollectionReport) {
    let restore_sql = match report.slow_log.previous_long_query_time.clone() {
        Some(previous) => format!(
            "ALTER SYSTEM SET log_min_duration_statement = '{}';",
            escape_sql_literal(&previous)
        ),
        None => "ALTER SYSTEM RESET log_min_duration_statement;".to_string(),
    };

    if let Err(err) = client.batch_execute(&restore_sql) {
        report.warnings.push(format!(
            "failed to restore PostgreSQL log_min_duration_statement: {err}"
        ));
        return;
    }
    if let Err(err) = client.simple_query("SELECT pg_reload_conf();") {
        report
            .warnings
            .push(format!("failed to reload PostgreSQL config: {err}"));
    }
}

fn fetch_mysql_variable(conn: &mut PooledConn, name: &str) -> Result<Option<String>, mysql::Error> {
    let sql = format!("SHOW VARIABLES LIKE '{name}'");
    let row: Option<(String, String)> = conn.query_first(sql)?;
    Ok(row.map(|(_, value)| value))
}

fn fetch_postgres_setting(
    client: &mut Client,
    name: &str,
) -> Result<Option<String>, postgres::Error> {
    let sql = "SELECT setting FROM pg_settings WHERE name = $1";
    let row = client.query_opt(sql, &[&name])?;
    Ok(row.map(|it| it.get::<_, String>(0)))
}

fn discover_postgres_log_path(client: &mut Client) -> Result<Option<String>, postgres::Error> {
    let logging_collector = fetch_postgres_setting(client, "logging_collector")?
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !is_truthy(&logging_collector) {
        return Ok(None);
    }

    let data_dir = fetch_postgres_setting(client, "data_directory")?;
    let log_dir = fetch_postgres_setting(client, "log_directory")?;
    let log_filename = fetch_postgres_setting(client, "log_filename")?;
    let (Some(data_dir), Some(log_dir), Some(log_filename)) = (data_dir, log_dir, log_filename)
    else {
        return Ok(None);
    };

    let dir = if Path::new(&log_dir).is_absolute() {
        PathBuf::from(log_dir)
    } else {
        Path::new(&data_dir).join(log_dir)
    };

    if log_filename.contains('%') {
        return Ok(find_latest_log_file(&dir));
    }

    let candidate = dir.join(log_filename);
    if candidate.exists() {
        return Ok(Some(candidate.to_string_lossy().to_string()));
    }
    Ok(find_latest_log_file(&dir))
}

fn find_latest_log_file(dir: &Path) -> Option<String> {
    let mut latest: Option<(SystemTime, PathBuf)> = None;
    for entry in fs::read_dir(dir).ok()? {
        let entry = entry.ok()?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let modified = entry.metadata().ok()?.modified().ok()?;
        match &latest {
            Some((current, _)) if modified <= *current => {}
            _ => latest = Some((modified, path)),
        }
    }
    latest.map(|(_, path)| path.to_string_lossy().to_string())
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
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

fn parse_mysql_slow_log_entries(content: &str) -> Vec<SlowLogEntry> {
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

fn parse_postgres_statement_log_entries(content: &str) -> Vec<SlowLogEntry> {
    let mut entries = Vec::new();
    let mut pending_duration_secs: Option<f64> = None;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(duration_secs) = extract_postgres_duration_secs(trimmed) {
            if let Some(sql) = extract_postgres_statement_sql(trimmed) {
                entries.push(SlowLogEntry {
                    sql,
                    query_time_secs: duration_secs,
                    lock_time_secs: 0.0,
                    rows_sent: 0,
                    rows_examined: 0,
                });
                pending_duration_secs = None;
            } else {
                pending_duration_secs = Some(duration_secs);
            }
            continue;
        }

        if let Some(duration_secs) = pending_duration_secs.take()
            && let Some(sql) = extract_postgres_statement_sql(trimmed)
        {
            entries.push(SlowLogEntry {
                sql,
                query_time_secs: duration_secs,
                lock_time_secs: 0.0,
                rows_sent: 0,
                rows_examined: 0,
            });
        }
    }

    entries
}

fn extract_postgres_duration_secs(line: &str) -> Option<f64> {
    let lower = line.to_ascii_lowercase();
    let marker = "duration:";
    let idx = lower.find(marker)?;
    let rest = line[(idx + marker.len())..].trim_start();
    let mut parts = rest.split_whitespace();
    let value = parts.next()?.trim_end_matches(',').parse::<f64>().ok()?;
    let unit = parts.next().unwrap_or("ms").trim_end_matches(',');

    match unit {
        "s" | "sec" | "secs" => Some(value),
        "us" => Some(value / 1_000_000.0),
        _ => Some(value / 1_000.0),
    }
}

fn extract_postgres_statement_sql(line: &str) -> Option<String> {
    let lower = line.to_ascii_lowercase();
    if let Some(idx) = lower.find("statement:") {
        let sql = line[(idx + "statement:".len())..].trim();
        if !sql.is_empty() {
            return Some(sql.to_string());
        }
    }

    if let Some(idx) = lower.find("execute ") {
        let tail = &line[idx..];
        if let Some(colon_pos) = tail.find(':') {
            let sql = tail[(colon_pos + 1)..].trim();
            if !sql.is_empty() {
                return Some(sql.to_string());
            }
        }
    }

    None
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
        || lower.contains("database system is starting up")
        || lower.contains("redo starts at")
        || lower.contains("redo done at")
    {
        categories.push("crash_recovery");
    }
    if lower.contains("purge") || lower.contains("vacuum") {
        categories.push("purge");
    }
    if lower.contains("replication")
        || lower.contains("replica")
        || lower.contains("slave")
        || lower.contains("relay log")
        || lower.contains("group replication")
        || lower.contains("binlog")
        || lower.contains("wal sender")
        || lower.contains("wal receiver")
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

fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "on" | "1" | "true" | "yes"
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

        let entries = parse_mysql_slow_log_entries(fixture);
        let digests = aggregate_slow_sql_digests(entries);
        assert_eq!(digests.len(), 1);
        assert_eq!(digests[0].count, 2);
        assert!(digests[0].fingerprint.contains("where id = ?"));
    }

    #[test]
    fn should_parse_postgres_statement_logs() {
        let fixture = r#"
2026-02-07 16:00:01.123 UTC [77] postgres@app LOG:  duration: 350.123 ms  statement: SELECT * FROM orders WHERE user_id = 1;
2026-02-07 16:00:01.456 UTC [77] postgres@app LOG:  duration: 401.000 ms
2026-02-07 16:00:01.456 UTC [77] postgres@app STATEMENT:  SELECT * FROM orders WHERE user_id = 2;
"#;

        let entries = parse_postgres_statement_log_entries(fixture);
        let digests = aggregate_slow_sql_digests(entries);
        assert_eq!(digests.len(), 1);
        assert_eq!(digests[0].count, 2);
        assert!(digests[0].fingerprint.contains("where user_id = ?"));
    }

    #[test]
    fn should_extract_error_alerts() {
        let lines = vec![
            "InnoDB: Deadlock found when trying to get lock".to_string(),
            "InnoDB: Starting crash recovery from checkpoint".to_string(),
            "replication applier thread stopped with error".to_string(),
            "InnoDB: purge lag increased".to_string(),
            "ERROR: deadlock detected".to_string(),
            "LOG: wal receiver process terminated".to_string(),
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
