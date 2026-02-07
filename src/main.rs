use clap::{ArgAction, Parser, ValueEnum};
use sql_insight::collection::{CapabilityProbe, CollectionPolicy, negotiate_collection_level};
use sql_insight::level0::{Level0CollectorConfig, collect_level0};
use tracing::{debug, info, warn};
use tracing_subscriber::filter::LevelFilter;

#[derive(Debug, Parser)]
#[command(
    name = "sql-insight",
    version,
    about = "SQL Insight MVP collector for MySQL Level 0 diagnostics"
)]
struct Cli {
    #[arg(long, env = "MYSQL_URL")]
    mysql_url: Option<String>,
    #[arg(long, env = "LEVEL0_TABLE_LIMIT", default_value_t = 200)]
    table_limit: usize,
    #[arg(long, env = "LEVEL0_INDEX_LIMIT", default_value_t = 500)]
    index_limit: usize,
    #[arg(long, value_enum, default_value_t = ReportFormat::PrettyJson)]
    output: ReportFormat,
    #[arg(short, long, action = ArgAction::Count, global = true)]
    verbose: u8,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ReportFormat {
    Json,
    PrettyJson,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    let table_limit = normalize_limit("table_limit", cli.table_limit, 200);
    let index_limit = normalize_limit("index_limit", cli.index_limit, 500);
    let config = Level0CollectorConfig {
        mysql_url: cli.mysql_url,
        table_limit,
        index_limit,
    };

    info!(
        mysql_url_set = config.mysql_url.is_some(),
        table_limit = config.table_limit,
        index_limit = config.index_limit,
        "starting level 0 collection"
    );

    let report = collect_level0(&config);
    for message in &report.warnings {
        warn!(warning = %message, "collector warning");
    }

    let probe = CapabilityProbe {
        has_mysql_status_access: report.capability.mysql_status_access,
        has_mysql_variables_access: report.capability.mysql_variables_access,
        has_information_schema_access: report.capability.information_schema_access,
        has_replication_status_access: report.capability.replication_status_access,
        has_os_metrics_access: report.capability.os_metrics_access,
        ..CapabilityProbe::default()
    };
    let negotiation = negotiate_collection_level(&CollectionPolicy::default(), &probe);

    info!(level = %negotiation.selected_level, "collection level selected");
    for evaluation in &negotiation.evaluations {
        if evaluation.ok {
            info!(level = %evaluation.level, "level evaluation passed");
        } else {
            warn!(
                level = %evaluation.level,
                reasons = %evaluation.reasons.join("; "),
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

    let output = match cli.output {
        ReportFormat::Json => serde_json::to_string(&report)?,
        ReportFormat::PrettyJson => serde_json::to_string_pretty(&report)?,
    };
    println!("{output}");

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
