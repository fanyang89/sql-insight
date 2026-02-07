use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use mysql::prelude::Queryable;
use mysql::{Opts, Pool, PooledConn, Row, Value};
use serde::Serialize;

const SYSTEM_SCHEMAS: [&str; 4] = ["information_schema", "mysql", "performance_schema", "sys"];

#[derive(Debug, Clone)]
pub struct Level0CollectorConfig {
    pub mysql_url: Option<String>,
    pub table_limit: usize,
    pub index_limit: usize,
}

impl Default for Level0CollectorConfig {
    fn default() -> Self {
        Self {
            mysql_url: None,
            table_limit: 200,
            index_limit: 500,
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct Level0Capability {
    pub mysql_connected: bool,
    pub mysql_status_access: bool,
    pub mysql_variables_access: bool,
    pub information_schema_access: bool,
    pub replication_status_access: bool,
    pub os_metrics_access: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct Level0CollectionReport {
    pub collected_at_unix_ms: u128,
    pub capability: Level0Capability,
    pub mysql: MysqlLevel0Snapshot,
    pub os: OsLevel0Snapshot,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct MysqlLevel0Snapshot {
    pub global_status: BTreeMap<String, String>,
    pub global_variables: BTreeMap<String, String>,
    pub table_sizes: Vec<TableSizeEntry>,
    pub indexes: Vec<IndexEntry>,
    pub replication_status: Option<BTreeMap<String, String>>,
    pub replication_status_source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct TableSizeEntry {
    pub table_schema: String,
    pub table_name: String,
    pub engine: String,
    pub table_rows: u64,
    pub data_length: u64,
    pub index_length: u64,
    pub total_length: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct IndexEntry {
    pub table_schema: String,
    pub table_name: String,
    pub index_name: String,
    pub non_unique: u64,
    pub seq_in_index: u64,
    pub column_name: String,
    pub cardinality: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct OsLevel0Snapshot {
    pub proc_cpu: Option<ProcCpuStat>,
    pub proc_mem: Option<ProcMemInfo>,
    pub load_average: Option<LoadAverage>,
    pub vmstat: Option<CommandSample>,
    pub iostat: Option<CommandSample>,
    pub sar: Option<CommandSample>,
}

impl OsLevel0Snapshot {
    fn has_any_metric(&self) -> bool {
        self.proc_cpu.is_some()
            || self.proc_mem.is_some()
            || self.load_average.is_some()
            || self.vmstat.as_ref().is_some_and(|it| it.available)
            || self.iostat.as_ref().is_some_and(|it| it.available)
            || self.sar.as_ref().is_some_and(|it| it.available)
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ProcCpuStat {
    pub user: u64,
    pub nice: u64,
    pub system: u64,
    pub idle: u64,
    pub iowait: u64,
    pub irq: u64,
    pub softirq: u64,
    pub steal: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ProcMemInfo {
    pub mem_total_kb: u64,
    pub mem_available_kb: u64,
    pub swap_total_kb: u64,
    pub swap_free_kb: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LoadAverage {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
    pub running_tasks: String,
    pub last_pid: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct CommandSample {
    pub command: String,
    pub args: Vec<String>,
    pub available: bool,
    pub status_code: Option<i32>,
    pub output: Option<String>,
    pub error: Option<String>,
}

pub fn collect_level0(config: &Level0CollectorConfig) -> Level0CollectionReport {
    let mut report = Level0CollectionReport {
        collected_at_unix_ms: now_unix_ms(),
        ..Level0CollectionReport::default()
    };

    if let Some(mysql_url) = config.mysql_url.as_deref() {
        let mysql_result = collect_mysql(mysql_url, config);
        report.capability.mysql_connected = mysql_result.connected;
        report.capability.mysql_status_access = mysql_result.status_ok;
        report.capability.mysql_variables_access = mysql_result.variables_ok;
        report.capability.information_schema_access = mysql_result.info_schema_ok;
        report.capability.replication_status_access = mysql_result.replication_ok;
        report.mysql = mysql_result.snapshot;
        report.warnings.extend(mysql_result.warnings);
    } else {
        report
            .warnings
            .push("MYSQL_URL not provided; skip MySQL Level 0 collection".to_string());
    }

    let os_result = collect_os();
    report.capability.os_metrics_access = os_result.snapshot.has_any_metric();
    report.os = os_result.snapshot;
    report.warnings.extend(os_result.warnings);

    report
}

#[derive(Debug, Default)]
struct MysqlCollectResult {
    connected: bool,
    status_ok: bool,
    variables_ok: bool,
    info_schema_ok: bool,
    replication_ok: bool,
    snapshot: MysqlLevel0Snapshot,
    warnings: Vec<String>,
}

fn collect_mysql(mysql_url: &str, config: &Level0CollectorConfig) -> MysqlCollectResult {
    let mut result = MysqlCollectResult::default();
    let opts = match Opts::from_url(mysql_url) {
        Ok(it) => it,
        Err(err) => {
            result
                .warnings
                .push(format!("invalid MYSQL_URL format: {err}"));
            return result;
        }
    };

    let pool = match Pool::new(opts) {
        Ok(it) => it,
        Err(err) => {
            result
                .warnings
                .push(format!("failed to create MySQL pool: {err}"));
            return result;
        }
    };

    let mut conn = match pool.get_conn() {
        Ok(it) => it,
        Err(err) => {
            result
                .warnings
                .push(format!("failed to connect MySQL: {err}"));
            return result;
        }
    };
    result.connected = true;

    match fetch_show_pairs(&mut conn, "SHOW GLOBAL STATUS") {
        Ok(map) => {
            result.status_ok = true;
            result.snapshot.global_status = map;
        }
        Err(err) => result
            .warnings
            .push(format!("SHOW GLOBAL STATUS failed: {err}")),
    }

    match fetch_show_pairs(&mut conn, "SHOW VARIABLES") {
        Ok(map) => {
            result.variables_ok = true;
            result.snapshot.global_variables = map;
        }
        Err(err) => result
            .warnings
            .push(format!("SHOW VARIABLES failed: {err}")),
    }

    let table_sizes_result = fetch_table_sizes(&mut conn, config.table_limit);
    let index_result = fetch_indexes(&mut conn, config.index_limit);
    let mut table_query_ok = false;
    let mut index_query_ok = false;
    if let Ok(rows) = table_sizes_result {
        table_query_ok = true;
        result.snapshot.table_sizes = rows;
    } else if let Err(err) = table_sizes_result {
        result
            .warnings
            .push(format!("information_schema TABLES query failed: {err}"));
    }
    if let Ok(rows) = index_result {
        index_query_ok = true;
        result.snapshot.indexes = rows;
    } else if let Err(err) = index_result {
        result
            .warnings
            .push(format!("information_schema STATISTICS query failed: {err}"));
    }
    result.info_schema_ok = table_query_ok && index_query_ok;

    match fetch_optional_row_map(&mut conn, "SHOW REPLICA STATUS") {
        Ok(replica_status) => {
            result.replication_ok = true;
            result.snapshot.replication_status = replica_status;
            result.snapshot.replication_status_source = Some("SHOW REPLICA STATUS".to_string());
        }
        Err(replica_err) => match fetch_optional_row_map(&mut conn, "SHOW SLAVE STATUS") {
            Ok(slave_status) => {
                result.replication_ok = true;
                result.snapshot.replication_status = slave_status;
                result.snapshot.replication_status_source = Some("SHOW SLAVE STATUS".to_string());
            }
            Err(slave_err) => result.warnings.push(format!(
                "replication status query failed (REPLICA and SLAVE): {replica_err}; {slave_err}"
            )),
        },
    }

    result
}

fn fetch_show_pairs(
    conn: &mut PooledConn,
    sql: &str,
) -> Result<BTreeMap<String, String>, mysql::Error> {
    let rows: Vec<Row> = conn.query(sql)?;
    let mut map = BTreeMap::new();
    for row in rows {
        let values = row.unwrap();
        if values.len() < 2 {
            continue;
        }
        let key = mysql_value_to_string(&values[0]);
        let val = mysql_value_to_string(&values[1]);
        map.insert(key, val);
    }
    Ok(map)
}

fn fetch_optional_row_map(
    conn: &mut PooledConn,
    sql: &str,
) -> Result<Option<BTreeMap<String, String>>, mysql::Error> {
    let rows: Vec<Row> = conn.query(sql)?;
    Ok(rows.into_iter().next().map(row_to_map))
}

fn fetch_table_sizes(
    conn: &mut PooledConn,
    limit: usize,
) -> Result<Vec<TableSizeEntry>, mysql::Error> {
    let query = format!(
        "SELECT TABLE_SCHEMA, TABLE_NAME, COALESCE(ENGINE, '') AS ENGINE, \
         COALESCE(TABLE_ROWS, 0) AS TABLE_ROWS, COALESCE(DATA_LENGTH, 0) AS DATA_LENGTH, \
         COALESCE(INDEX_LENGTH, 0) AS INDEX_LENGTH \
         FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA NOT IN ('{}', '{}', '{}', '{}') \
         ORDER BY (COALESCE(DATA_LENGTH, 0) + COALESCE(INDEX_LENGTH, 0)) DESC \
         LIMIT {}",
        SYSTEM_SCHEMAS[0], SYSTEM_SCHEMAS[1], SYSTEM_SCHEMAS[2], SYSTEM_SCHEMAS[3], limit
    );
    let rows: Vec<Row> = conn.query(query)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let m = row_to_map(row);
        let data_length = parse_u64(m.get("DATA_LENGTH"));
        let index_length = parse_u64(m.get("INDEX_LENGTH"));
        out.push(TableSizeEntry {
            table_schema: get_string(&m, "TABLE_SCHEMA"),
            table_name: get_string(&m, "TABLE_NAME"),
            engine: get_string(&m, "ENGINE"),
            table_rows: parse_u64(m.get("TABLE_ROWS")),
            data_length,
            index_length,
            total_length: data_length + index_length,
        });
    }
    Ok(out)
}

fn fetch_indexes(conn: &mut PooledConn, limit: usize) -> Result<Vec<IndexEntry>, mysql::Error> {
    let query = format!(
        "SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, \
         COALESCE(CARDINALITY, 0) AS CARDINALITY \
         FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA NOT IN ('{}', '{}', '{}', '{}') \
         ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX \
         LIMIT {}",
        SYSTEM_SCHEMAS[0], SYSTEM_SCHEMAS[1], SYSTEM_SCHEMAS[2], SYSTEM_SCHEMAS[3], limit
    );
    let rows: Vec<Row> = conn.query(query)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let m = row_to_map(row);
        out.push(IndexEntry {
            table_schema: get_string(&m, "TABLE_SCHEMA"),
            table_name: get_string(&m, "TABLE_NAME"),
            index_name: get_string(&m, "INDEX_NAME"),
            non_unique: parse_u64(m.get("NON_UNIQUE")),
            seq_in_index: parse_u64(m.get("SEQ_IN_INDEX")),
            column_name: get_string(&m, "COLUMN_NAME"),
            cardinality: parse_u64(m.get("CARDINALITY")),
        });
    }
    Ok(out)
}

fn row_to_map(row: Row) -> BTreeMap<String, String> {
    let columns = row
        .columns_ref()
        .iter()
        .map(|it| it.name_str().to_string())
        .collect::<Vec<_>>();
    let values = row.unwrap();
    columns
        .into_iter()
        .zip(values)
        .map(|(k, v)| (k, mysql_value_to_string(&v)))
        .collect()
}

fn mysql_value_to_string(value: &Value) -> String {
    match value {
        Value::NULL => "NULL".to_string(),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).to_string(),
        Value::Int(it) => it.to_string(),
        Value::UInt(it) => it.to_string(),
        Value::Float(it) => it.to_string(),
        Value::Double(it) => it.to_string(),
        Value::Date(year, month, day, hour, minute, second, micro_second) => format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{:06}",
            micro_second
        ),
        Value::Time(is_negative, days, hours, minutes, seconds, micro_second) => {
            let sign = if *is_negative { "-" } else { "" };
            format!(
                "{sign}{days} {hours:02}:{minutes:02}:{seconds:02}.{:06}",
                micro_second
            )
        }
    }
}

#[derive(Debug, Default)]
struct OsCollectResult {
    snapshot: OsLevel0Snapshot,
    warnings: Vec<String>,
}

fn collect_os() -> OsCollectResult {
    let mut result = OsCollectResult::default();

    match fs::read_to_string("/proc/stat") {
        Ok(content) => result.snapshot.proc_cpu = parse_proc_cpu_stat(&content),
        Err(err) => result
            .warnings
            .push(format!("failed reading /proc/stat: {err}")),
    }
    match fs::read_to_string("/proc/meminfo") {
        Ok(content) => result.snapshot.proc_mem = parse_proc_meminfo(&content),
        Err(err) => result
            .warnings
            .push(format!("failed reading /proc/meminfo: {err}")),
    }
    match fs::read_to_string("/proc/loadavg") {
        Ok(content) => result.snapshot.load_average = parse_load_average(&content),
        Err(err) => result
            .warnings
            .push(format!("failed reading /proc/loadavg: {err}")),
    }

    result.snapshot.vmstat = Some(run_optional_command("vmstat", &[]));
    result.snapshot.iostat = Some(run_optional_command("iostat", &[]));
    result.snapshot.sar = Some(run_optional_command("sar", &["-u", "1", "1"]));
    append_command_warning(&mut result.warnings, result.snapshot.vmstat.as_ref());
    append_command_warning(&mut result.warnings, result.snapshot.iostat.as_ref());
    append_command_warning(&mut result.warnings, result.snapshot.sar.as_ref());

    result
}

fn append_command_warning(warnings: &mut Vec<String>, sample: Option<&CommandSample>) {
    if let Some(sample) = sample {
        if let Some(err) = &sample.error {
            if sample.available {
                warnings.push(format!("{} failed: {}", sample.command, err));
            }
        }
    }
}

fn run_optional_command(command: &str, args: &[&str]) -> CommandSample {
    match Command::new(command).args(args).output() {
        Ok(output) => {
            let status_code = output.status.code();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let mut sample = CommandSample {
                command: command.to_string(),
                args: args.iter().map(|it| it.to_string()).collect(),
                available: true,
                status_code,
                output: truncate_output(stdout),
                error: None,
            };
            if !output.status.success() {
                sample.error = Some(if stderr.is_empty() {
                    format!("exit status: {}", output.status)
                } else {
                    stderr
                });
            }
            sample
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => CommandSample {
            command: command.to_string(),
            args: args.iter().map(|it| it.to_string()).collect(),
            available: false,
            status_code: None,
            output: None,
            error: Some("command not found".to_string()),
        },
        Err(err) => CommandSample {
            command: command.to_string(),
            args: args.iter().map(|it| it.to_string()).collect(),
            available: true,
            status_code: None,
            output: None,
            error: Some(err.to_string()),
        },
    }
}

fn truncate_output(text: String) -> Option<String> {
    if text.is_empty() {
        return None;
    }
    const MAX_CHARS: usize = 4000;
    let mut out = text;
    if out.chars().count() > MAX_CHARS {
        out = out.chars().take(MAX_CHARS).collect::<String>();
        out.push_str("\n...[truncated]");
    }
    Some(out)
}

fn parse_proc_cpu_stat(content: &str) -> Option<ProcCpuStat> {
    let first_line = content.lines().next()?;
    let mut parts = first_line.split_whitespace();
    if parts.next()? != "cpu" {
        return None;
    }
    let user = parts.next()?.parse::<u64>().ok()?;
    let nice = parts.next()?.parse::<u64>().ok()?;
    let system = parts.next()?.parse::<u64>().ok()?;
    let idle = parts.next()?.parse::<u64>().ok()?;
    let iowait = parts
        .next()
        .and_then(|it| it.parse::<u64>().ok())
        .unwrap_or(0);
    let irq = parts
        .next()
        .and_then(|it| it.parse::<u64>().ok())
        .unwrap_or(0);
    let softirq = parts
        .next()
        .and_then(|it| it.parse::<u64>().ok())
        .unwrap_or(0);
    let steal = parts
        .next()
        .and_then(|it| it.parse::<u64>().ok())
        .unwrap_or(0);
    Some(ProcCpuStat {
        user,
        nice,
        system,
        idle,
        iowait,
        irq,
        softirq,
        steal,
    })
}

fn parse_proc_meminfo(content: &str) -> Option<ProcMemInfo> {
    let mut values: HashMap<String, u64> = HashMap::new();
    for line in content.lines() {
        if let Some((key, rest)) = line.split_once(':') {
            if let Some(first_value) = rest.split_whitespace().next() {
                if let Ok(parsed) = first_value.parse::<u64>() {
                    values.insert(key.trim().to_string(), parsed);
                }
            }
        }
    }

    let mem_total_kb = *values.get("MemTotal")?;
    let mem_available_kb = *values.get("MemAvailable")?;
    Some(ProcMemInfo {
        mem_total_kb,
        mem_available_kb,
        swap_total_kb: *values.get("SwapTotal").unwrap_or(&0),
        swap_free_kb: *values.get("SwapFree").unwrap_or(&0),
    })
}

fn parse_load_average(content: &str) -> Option<LoadAverage> {
    let parts = content.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 5 {
        return None;
    }
    Some(LoadAverage {
        one: parts[0].parse::<f64>().ok()?,
        five: parts[1].parse::<f64>().ok()?,
        fifteen: parts[2].parse::<f64>().ok()?,
        running_tasks: parts[3].to_string(),
        last_pid: parts[4].parse::<u64>().ok()?,
    })
}

fn parse_u64(value: Option<&String>) -> u64 {
    value
        .and_then(|it| it.parse::<u64>().ok())
        .unwrap_or_default()
}

fn get_string(map: &BTreeMap<String, String>, key: &str) -> String {
    map.get(key).cloned().unwrap_or_default()
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
    fn should_parse_proc_cpu_stat() {
        let fixture = "cpu  101 202 303 404 11 12 13 14 0 0\ncpu0 1 2 3 4 5 6 7 8 9 10";
        let parsed = parse_proc_cpu_stat(fixture).expect("cpu stat should parse");
        assert_eq!(parsed.user, 101);
        assert_eq!(parsed.system, 303);
        assert_eq!(parsed.idle, 404);
        assert_eq!(parsed.steal, 14);
    }

    #[test]
    fn should_parse_proc_meminfo() {
        let fixture = "MemTotal:       8000000 kB\nMemAvailable:   3000000 kB\nSwapTotal:      1000000 kB\nSwapFree:        750000 kB\n";
        let parsed = parse_proc_meminfo(fixture).expect("meminfo should parse");
        assert_eq!(parsed.mem_total_kb, 8_000_000);
        assert_eq!(parsed.mem_available_kb, 3_000_000);
        assert_eq!(parsed.swap_total_kb, 1_000_000);
        assert_eq!(parsed.swap_free_kb, 750_000);
    }

    #[test]
    fn should_parse_load_average() {
        let fixture = "0.10 0.20 0.30 2/222 98765";
        let parsed = parse_load_average(fixture).expect("loadavg should parse");
        assert_eq!(parsed.one, 0.10);
        assert_eq!(parsed.five, 0.20);
        assert_eq!(parsed.fifteen, 0.30);
        assert_eq!(parsed.running_tasks, "2/222");
        assert_eq!(parsed.last_pid, 98_765);
    }
}
