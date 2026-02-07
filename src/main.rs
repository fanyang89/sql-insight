use std::thread;
use std::time::{Duration, Instant};

use clap::{ArgAction, Parser, ValueEnum};
use serde::Serialize;
use sql_insight::collection::{
    CapabilityProbe, CollectionLevel, CollectionPolicy, negotiate_collection_level,
};
use sql_insight::level0::{Level0CollectionReport, Level0CollectorConfig, collect_level0};
use sql_insight::level1::{Level1CollectionReport, Level1CollectorConfig, collect_level1};
use sql_insight::pipeline::{
    AttemptTrace, RunMode, SchedulerConfig, SourceStatus, UnifiedCollectionRecord,
    jittered_interval, new_run_id, now_unix_ms, run_with_timeout,
};
use sql_insight::postgres_level0::{
    PostgresLevel0CollectionReport, PostgresLevel0CollectorConfig, collect_postgres_level0,
};
use tracing::{debug, info, warn};
use tracing_subscriber::filter::LevelFilter;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "sql-insight",
    version,
    about = "SQL Insight collector with unified data contract and scheduler"
)]
struct Cli {
    #[arg(long, value_enum, env = "DB_ENGINE", default_value_t = DatabaseEngine::Mysql)]
    engine: DatabaseEngine,
    #[arg(long, value_enum, default_value_t = CollectLevel::Level1)]
    collect_level: CollectLevel,
    #[arg(long, value_enum, env = "RUN_MODE", default_value_t = RunMode::Once)]
    run_mode: RunMode,
    #[arg(long, env = "RUN_INTERVAL_SECS", default_value_t = 60)]
    interval_secs: u64,
    #[arg(long, env = "RUN_JITTER_PCT", default_value_t = 0.1)]
    jitter_pct: f64,
    #[arg(long, env = "RUN_TIMEOUT_SECS", default_value_t = 120)]
    timeout_secs: u64,
    #[arg(long, env = "RUN_RETRY_TIMES", default_value_t = 1)]
    retry_times: u32,
    #[arg(long, env = "RUN_RETRY_BACKOFF_MS", default_value_t = 1_000)]
    retry_backoff_ms: u64,
    #[arg(long, env = "RUN_MAX_CYCLES")]
    max_cycles: Option<u32>,
    #[arg(long, env = "MYSQL_URL")]
    mysql_url: Option<String>,
    #[arg(long, env = "POSTGRES_URL")]
    postgres_url: Option<String>,
    #[arg(long, env = "LEVEL0_TABLE_LIMIT", default_value_t = 200)]
    table_limit: usize,
    #[arg(long, env = "LEVEL0_INDEX_LIMIT", default_value_t = 500)]
    index_limit: usize,
    #[arg(long, env = "LEVEL1_SLOW_LOG_WINDOW_SECS", default_value_t = 30)]
    slow_log_window_secs: u64,
    #[arg(long, env = "LEVEL1_LONG_QUERY_TIME_SECS", default_value_t = 0.2)]
    slow_log_long_query_time_secs: f64,
    #[arg(long, env = "LEVEL1_SLOW_LOG_PATH")]
    slow_log_path: Option<String>,
    #[arg(long, env = "LEVEL1_ERROR_LOG_PATH")]
    error_log_path: Option<String>,
    #[arg(long, env = "LEVEL1_MAX_SLOW_LOG_BYTES", default_value_t = 2_000_000)]
    max_slow_log_bytes: usize,
    #[arg(long, env = "LEVEL1_MAX_ERROR_LOG_BYTES", default_value_t = 2_000_000)]
    max_error_log_bytes: usize,
    #[arg(long, env = "LEVEL1_MAX_ERROR_LOG_LINES", default_value_t = 2_000)]
    max_error_log_lines: usize,
    #[arg(long, default_value_t = false)]
    no_slow_log_hot_switch: bool,
    #[arg(long, default_value_t = false)]
    no_restore_slow_log_settings: bool,
    #[arg(long, value_enum, default_value_t = ReportFormat::PrettyJson)]
    output: ReportFormat,
    #[arg(short, long, action = ArgAction::Count, global = true)]
    verbose: u8,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum CollectLevel {
    Level0,
    Level1,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum ReportFormat {
    Json,
    PrettyJson,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum DatabaseEngine {
    Mysql,
    Postgres,
}

#[derive(Debug, Clone, Serialize)]
struct CliOutput {
    engine: String,
    requested_level: String,
    selected_level: String,
    downgrade_reasons: Vec<String>,
    level0: Level0CollectionReport,
    postgres_level0: Option<PostgresLevel0CollectionReport>,
    level1: Option<Level1CollectionReport>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    let scheduler = SchedulerConfig {
        mode: cli.run_mode,
        interval_secs: normalize_u64_limit("interval_secs", cli.interval_secs, 60),
        jitter_pct: normalize_jitter("jitter_pct", cli.jitter_pct, 0.1),
        timeout_secs: normalize_u64_limit("timeout_secs", cli.timeout_secs, 120),
        retry_times: cli.retry_times,
        retry_backoff_ms: normalize_u64_limit("retry_backoff_ms", cli.retry_backoff_ms, 1_000),
        max_cycles: cli.max_cycles,
    };
    let run_id = new_run_id();

    info!(
        run_id = run_id,
        mode = ?scheduler.mode,
        interval_secs = scheduler.interval_secs,
        timeout_secs = scheduler.timeout_secs,
        retry_times = scheduler.retry_times,
        "scheduler initialized"
    );

    let mut cycle: u32 = 0;
    loop {
        cycle += 1;
        let cycle_start_ms = now_unix_ms();
        let mut attempts = Vec::new();
        let mut payload = None;
        let total_attempts = scheduler.retry_times.saturating_add(1);

        info!(
            run_id = run_id,
            cycle = cycle,
            engine = ?cli.engine,
            collect_level = ?cli.collect_level,
            "collection cycle started"
        );

        for attempt in 1..=total_attempts {
            let attempt_start = Instant::now();
            let attempt_cli = cli.clone();
            let timeout = Duration::from_secs(scheduler.timeout_secs);
            let result = run_with_timeout(timeout, move || {
                collect_once(&attempt_cli).map_err(|it| it.to_string())
            });
            let duration_ms = attempt_start.elapsed().as_millis();

            match result {
                Ok(output) => {
                    attempts.push(AttemptTrace {
                        attempt,
                        duration_ms,
                        status: "ok".to_string(),
                        error: None,
                    });
                    payload = Some(output);
                    info!(
                        run_id = run_id,
                        cycle = cycle,
                        attempt = attempt,
                        duration_ms = duration_ms,
                        "collection attempt succeeded"
                    );
                    break;
                }
                Err(err) => {
                    attempts.push(AttemptTrace {
                        attempt,
                        duration_ms,
                        status: "failed".to_string(),
                        error: Some(err.clone()),
                    });
                    warn!(
                        run_id = run_id,
                        cycle = cycle,
                        attempt = attempt,
                        duration_ms = duration_ms,
                        error = err,
                        "collection attempt failed"
                    );
                    if attempt < total_attempts {
                        thread::sleep(Duration::from_millis(scheduler.retry_backoff_ms));
                    }
                }
            }
        }

        let cycle_end_ms = now_unix_ms();
        let last_error = attempts.iter().rev().find_map(|it| it.error.clone());
        let source_status = payload
            .as_ref()
            .map(derive_source_status)
            .unwrap_or_default();
        let warnings = payload.as_ref().map(collect_warnings).unwrap_or_default();
        let selected_level = payload.as_ref().map(|it| it.selected_level.clone());
        let record = UnifiedCollectionRecord {
            contract_version: "v1".to_string(),
            run_id: run_id.clone(),
            cycle,
            engine: engine_label(cli.engine).to_string(),
            requested_level: format!("{}", requested_to_collection_level(cli.collect_level)),
            selected_level,
            schedule: scheduler.clone(),
            window: sql_insight::pipeline::ScheduleWindow {
                start_unix_ms: cycle_start_ms,
                end_unix_ms: cycle_end_ms,
                duration_ms: cycle_end_ms.saturating_sub(cycle_start_ms),
            },
            attempts,
            source_status,
            warnings,
            status: if payload.is_some() {
                "ok".to_string()
            } else {
                "failed".to_string()
            },
            error: last_error,
            payload,
        };
        emit_record(&record, cli.output)?;

        if scheduler.mode == RunMode::Once {
            break;
        }
        if let Some(max_cycles) = scheduler.max_cycles {
            if cycle >= max_cycles {
                info!(
                    run_id = run_id,
                    cycle = cycle,
                    "scheduler reached max cycles"
                );
                break;
            }
        }

        let sleep_for = jittered_interval(scheduler.interval_secs, scheduler.jitter_pct);
        info!(
            run_id = run_id,
            cycle = cycle,
            sleep_ms = sleep_for.as_millis() as u64,
            "scheduler sleeping before next cycle"
        );
        thread::sleep(sleep_for);
    }

    Ok(())
}

fn collect_once(cli: &Cli) -> anyhow::Result<CliOutput> {
    let table_limit = normalize_limit("table_limit", cli.table_limit, 200);
    let index_limit = normalize_limit("index_limit", cli.index_limit, 500);
    let level0_config = Level0CollectorConfig {
        mysql_url: if cli.engine == DatabaseEngine::Mysql {
            cli.mysql_url.clone()
        } else {
            None
        },
        table_limit,
        index_limit,
        suppress_mysql_missing_warning: cli.engine == DatabaseEngine::Postgres,
    };

    info!(
        engine = ?cli.engine,
        collect_level = ?cli.collect_level,
        mysql_url_set = level0_config.mysql_url.is_some(),
        postgres_url_set = cli.postgres_url.is_some(),
        "starting collection run"
    );
    info!(
        table_limit = level0_config.table_limit,
        index_limit = level0_config.index_limit,
        "running level 0 collector"
    );
    let level0_report = collect_level0(&level0_config);
    log_report_warnings("level0", &level0_report.warnings);
    let postgres_level0_report = if cli.engine == DatabaseEngine::Postgres {
        info!(
            table_limit = table_limit,
            index_limit = index_limit,
            "running postgres level 0 collector"
        );
        let report = collect_postgres_level0(&PostgresLevel0CollectorConfig {
            postgres_url: cli.postgres_url.clone(),
            table_limit,
            index_limit,
        });
        log_report_warnings("postgres_level0", &report.warnings);
        Some(report)
    } else {
        None
    };

    let level1_report = if cli.collect_level == CollectLevel::Level1
        && cli.engine == DatabaseEngine::Mysql
    {
        let slow_log_window_secs =
            normalize_u64_limit("slow_log_window_secs", cli.slow_log_window_secs, 30);
        let slow_log_long_query_time_secs = normalize_f64_limit(
            "slow_log_long_query_time_secs",
            cli.slow_log_long_query_time_secs,
            0.2,
        );
        let level1_config = Level1CollectorConfig {
            mysql_url: cli.mysql_url.clone(),
            slow_log_window_secs,
            slow_log_long_query_time_secs,
            enable_slow_log_hot_switch: !cli.no_slow_log_hot_switch,
            restore_slow_log_settings: !cli.no_restore_slow_log_settings,
            slow_log_path: cli.slow_log_path.clone(),
            error_log_path: cli.error_log_path.clone(),
            max_slow_log_bytes: normalize_limit(
                "max_slow_log_bytes",
                cli.max_slow_log_bytes,
                2_000_000,
            ),
            max_error_log_bytes: normalize_limit(
                "max_error_log_bytes",
                cli.max_error_log_bytes,
                2_000_000,
            ),
            max_error_log_lines: normalize_limit(
                "max_error_log_lines",
                cli.max_error_log_lines,
                2_000,
            ),
        };
        info!(
            slow_log_window_secs = level1_config.slow_log_window_secs,
            slow_log_long_query_time_secs = level1_config.slow_log_long_query_time_secs,
            enable_slow_log_hot_switch = level1_config.enable_slow_log_hot_switch,
            restore_slow_log_settings = level1_config.restore_slow_log_settings,
            "running level 1 collector"
        );
        let report = collect_level1(&level1_config);
        log_report_warnings("level1", &report.warnings);
        Some(report)
    } else if cli.collect_level == CollectLevel::Level1 && cli.engine == DatabaseEngine::Postgres {
        warn!("level1 collector is currently mysql-only; postgres request will downgrade");
        None
    } else {
        None
    };

    let probe = build_capability_probe(
        cli.engine,
        &level0_report,
        postgres_level0_report.as_ref(),
        level1_report.as_ref(),
    );
    let target_level = requested_to_collection_level(cli.collect_level);
    let policy = CollectionPolicy {
        preferred_level: target_level,
        max_accepted_level: target_level,
        expert_mode_enabled: false,
    };
    let negotiation = negotiate_collection_level(&policy, &probe);

    info!(level = %negotiation.selected_level, "collection level selected");
    for evaluation in &negotiation.evaluations {
        let reasons = evaluation
            .reasons
            .iter()
            .map(|it| map_reason_for_engine(cli.engine, it))
            .collect::<Vec<_>>()
            .join("; ");
        if evaluation.ok {
            info!(level = %evaluation.level, "level evaluation passed");
        } else {
            warn!(
                level = %evaluation.level,
                reasons = %reasons,
                "level evaluation downgraded"
            );
        }
    }
    for task in &negotiation.tasks {
        debug!(
            level = %task.level,
            task = task.name,
            source = task.source,
            purpose = task.purpose,
            "enabled collection task"
        );
    }

    Ok(CliOutput {
        engine: engine_label(cli.engine).to_string(),
        requested_level: format!("{}", target_level),
        selected_level: format!("{}", negotiation.selected_level),
        downgrade_reasons: negotiation
            .evaluations
            .iter()
            .filter(|it| !it.ok)
            .map(|it| {
                let reasons = it
                    .reasons
                    .iter()
                    .map(|reason| map_reason_for_engine(cli.engine, reason))
                    .collect::<Vec<_>>()
                    .join("; ");
                format!("[{}] {}", it.level, reasons)
            })
            .collect(),
        level0: level0_report,
        postgres_level0: postgres_level0_report,
        level1: level1_report,
    })
}

fn init_tracing(verbose: u8) {
    let level = match verbose {
        0 => LevelFilter::INFO,
        1 => LevelFilter::DEBUG,
        _ => LevelFilter::TRACE,
    };
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}

fn emit_record(
    record: &UnifiedCollectionRecord<CliOutput>,
    format: ReportFormat,
) -> anyhow::Result<()> {
    let output_text = match format {
        ReportFormat::Json => serde_json::to_string(record)?,
        ReportFormat::PrettyJson => serde_json::to_string_pretty(record)?,
    };
    println!("{output_text}");
    Ok(())
}

fn derive_source_status(output: &CliOutput) -> Vec<SourceStatus> {
    let mut statuses = vec![
        SourceStatus {
            source: "os.basic_metrics".to_string(),
            ok: output.level0.capability.os_metrics_access,
        },
        SourceStatus {
            source: "mysql.global_status".to_string(),
            ok: output.level0.capability.mysql_status_access,
        },
        SourceStatus {
            source: "mysql.variables".to_string(),
            ok: output.level0.capability.mysql_variables_access,
        },
        SourceStatus {
            source: "mysql.information_schema".to_string(),
            ok: output.level0.capability.information_schema_access,
        },
        SourceStatus {
            source: "mysql.replication_status".to_string(),
            ok: output.level0.capability.replication_status_access,
        },
    ];

    if let Some(level1) = &output.level1 {
        statuses.push(SourceStatus {
            source: "mysql.slow_log_hot_switch".to_string(),
            ok: level1.capability.can_enable_slow_log_hot_switch,
        });
        statuses.push(SourceStatus {
            source: "mysql.slow_log".to_string(),
            ok: level1.capability.can_read_slow_log,
        });
        statuses.push(SourceStatus {
            source: "mysql.error_log".to_string(),
            ok: level1.capability.can_read_error_log,
        });
    }
    if let Some(pg) = &output.postgres_level0 {
        statuses.push(SourceStatus {
            source: "postgres.global_status".to_string(),
            ok: pg.capability.has_status_access,
        });
        statuses.push(SourceStatus {
            source: "postgres.settings".to_string(),
            ok: pg.capability.has_settings_access,
        });
        statuses.push(SourceStatus {
            source: "postgres.storage".to_string(),
            ok: pg.capability.has_storage_access,
        });
        statuses.push(SourceStatus {
            source: "postgres.replication_status".to_string(),
            ok: pg.capability.has_replication_status_access,
        });
    }

    statuses
}

fn collect_warnings(output: &CliOutput) -> Vec<String> {
    let mut warnings = Vec::new();
    warnings.extend(output.level0.warnings.clone());
    if let Some(level1) = &output.level1 {
        warnings.extend(level1.warnings.clone());
    }
    if let Some(pg) = &output.postgres_level0 {
        warnings.extend(pg.warnings.clone());
    }
    warnings
}

fn normalize_limit(name: &str, value: usize, fallback: usize) -> usize {
    if value == 0 {
        warn!(
            name,
            value, fallback, "invalid zero limit, fallback applied"
        );
        fallback
    } else {
        value
    }
}

fn normalize_u64_limit(name: &str, value: u64, fallback: u64) -> u64 {
    if value == 0 {
        warn!(
            name,
            value, fallback, "invalid zero limit, fallback applied"
        );
        fallback
    } else {
        value
    }
}

fn normalize_f64_limit(name: &str, value: f64, fallback: f64) -> f64 {
    if value <= 0.0 {
        warn!(
            name,
            value, fallback, "invalid non-positive value, fallback applied"
        );
        fallback
    } else {
        value
    }
}

fn normalize_jitter(name: &str, value: f64, fallback: f64) -> f64 {
    if value < 0.0 || value >= 1.0 {
        warn!(name, value, fallback, "invalid jitter, fallback applied");
        fallback
    } else {
        value
    }
}

fn requested_to_collection_level(level: CollectLevel) -> CollectionLevel {
    match level {
        CollectLevel::Level0 => CollectionLevel::Level0,
        CollectLevel::Level1 => CollectionLevel::Level1,
    }
}

fn build_capability_probe(
    engine: DatabaseEngine,
    mysql_level0: &Level0CollectionReport,
    postgres_level0: Option<&PostgresLevel0CollectionReport>,
    mysql_level1: Option<&Level1CollectionReport>,
) -> CapabilityProbe {
    match engine {
        DatabaseEngine::Mysql => CapabilityProbe {
            has_mysql_status_access: mysql_level0.capability.mysql_status_access,
            has_mysql_variables_access: mysql_level0.capability.mysql_variables_access,
            has_information_schema_access: mysql_level0.capability.information_schema_access,
            has_replication_status_access: mysql_level0.capability.replication_status_access,
            has_os_metrics_access: mysql_level0.capability.os_metrics_access,
            can_enable_slow_log_hot_switch: mysql_level1
                .map(|it| it.capability.can_enable_slow_log_hot_switch)
                .unwrap_or(false),
            can_read_slow_log: mysql_level1
                .map(|it| it.capability.can_read_slow_log)
                .unwrap_or(false),
            can_read_error_log: mysql_level1
                .map(|it| it.capability.can_read_error_log)
                .unwrap_or(false),
            ..CapabilityProbe::default()
        },
        DatabaseEngine::Postgres => {
            let pg = postgres_level0.cloned().unwrap_or_default();
            CapabilityProbe {
                has_mysql_status_access: pg.capability.has_status_access,
                has_mysql_variables_access: pg.capability.has_settings_access,
                has_information_schema_access: pg.capability.has_storage_access,
                has_replication_status_access: pg.capability.has_replication_status_access,
                has_os_metrics_access: mysql_level0.capability.os_metrics_access,
                can_enable_slow_log_hot_switch: false,
                can_read_slow_log: false,
                can_read_error_log: false,
                ..CapabilityProbe::default()
            }
        }
    }
}

fn map_reason_for_engine(engine: DatabaseEngine, reason: &str) -> String {
    match engine {
        DatabaseEngine::Mysql => reason.to_string(),
        DatabaseEngine::Postgres => match reason {
            "missing SHOW GLOBAL STATUS access" => "missing pg_stat_database access".to_string(),
            "missing SHOW VARIABLES access" => "missing pg_settings access".to_string(),
            "missing INFORMATION_SCHEMA access" => {
                "missing relation/index metadata access".to_string()
            }
            "missing replication status access" => {
                "missing pg_stat_replication/pg_stat_wal_receiver access".to_string()
            }
            "cannot hot-enable slow log" => "postgres Level 1 is not implemented yet".to_string(),
            "cannot collect slow log for digest aggregation" => {
                "postgres Level 1 is not implemented yet".to_string()
            }
            "cannot collect error log" => "postgres Level 1 is not implemented yet".to_string(),
            other => other.to_string(),
        },
    }
}

fn log_report_warnings(scope: &str, warnings: &[String]) {
    for message in warnings {
        warn!(scope, warning = %message, "collector warning");
    }
}

fn engine_label(engine: DatabaseEngine) -> &'static str {
    match engine {
        DatabaseEngine::Mysql => "mysql",
        DatabaseEngine::Postgres => "postgres",
    }
}
