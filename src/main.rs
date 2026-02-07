use clap::{ArgAction, Parser, ValueEnum};
use serde::Serialize;
use sql_insight::collection::{
    CapabilityProbe, CollectionLevel, CollectionPolicy, negotiate_collection_level,
};
use sql_insight::level0::{Level0CollectionReport, Level0CollectorConfig, collect_level0};
use sql_insight::level1::{Level1CollectionReport, Level1CollectorConfig, collect_level1};
use sql_insight::postgres_level0::{
    PostgresLevel0CollectionReport, PostgresLevel0CollectorConfig, collect_postgres_level0,
};
use tracing::{debug, info, warn};
use tracing_subscriber::filter::LevelFilter;

#[derive(Debug, Parser)]
#[command(
    name = "sql-insight",
    version,
    about = "SQL Insight MVP collector for MySQL and PostgreSQL diagnostics"
)]
struct Cli {
    #[arg(long, value_enum, env = "DB_ENGINE", default_value_t = DatabaseEngine::Mysql)]
    engine: DatabaseEngine,
    #[arg(long, value_enum, default_value_t = CollectLevel::Level1)]
    collect_level: CollectLevel,
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ReportFormat {
    Json,
    PrettyJson,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum DatabaseEngine {
    Mysql,
    Postgres,
}

#[derive(Debug, Serialize)]
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

    let output_payload = CliOutput {
        engine: match cli.engine {
            DatabaseEngine::Mysql => "mysql".to_string(),
            DatabaseEngine::Postgres => "postgres".to_string(),
        },
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
    };
    let output_text = match cli.output {
        ReportFormat::Json => serde_json::to_string(&output_payload)?,
        ReportFormat::PrettyJson => serde_json::to_string_pretty(&output_payload)?,
    };
    println!("{output_text}");

    Ok(())
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
        .init();
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
