use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use postgres::{Client, NoTls, Row};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct PostgresLevel0CollectorConfig {
    pub postgres_url: Option<String>,
    pub table_limit: usize,
    pub index_limit: usize,
}

impl Default for PostgresLevel0CollectorConfig {
    fn default() -> Self {
        Self {
            postgres_url: None,
            table_limit: 200,
            index_limit: 500,
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PostgresLevel0Capability {
    pub postgres_connected: bool,
    pub has_status_access: bool,
    pub has_settings_access: bool,
    pub has_storage_access: bool,
    pub has_replication_status_access: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PostgresLevel0CollectionReport {
    pub collected_at_unix_ms: u128,
    pub capability: PostgresLevel0Capability,
    pub postgres: PostgresLevel0Snapshot,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PostgresLevel0Snapshot {
    pub global_status: BTreeMap<String, String>,
    pub global_variables: BTreeMap<String, String>,
    pub table_sizes: Vec<PostgresTableSizeEntry>,
    pub indexes: Vec<PostgresIndexEntry>,
    pub replication_status: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PostgresTableSizeEntry {
    pub table_schema: String,
    pub table_name: String,
    pub estimated_rows: i64,
    pub data_length: i64,
    pub index_length: i64,
    pub total_length: i64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PostgresIndexEntry {
    pub table_schema: String,
    pub table_name: String,
    pub index_name: String,
    pub index_def: String,
}

pub fn collect_postgres_level0(
    config: &PostgresLevel0CollectorConfig,
) -> PostgresLevel0CollectionReport {
    let mut report = PostgresLevel0CollectionReport {
        collected_at_unix_ms: now_unix_ms(),
        ..PostgresLevel0CollectionReport::default()
    };

    let Some(url) = config.postgres_url.as_deref() else {
        report
            .warnings
            .push("POSTGRES_URL not provided; skip PostgreSQL Level 0 collection".to_string());
        return report;
    };

    let mut client = match Client::connect(url, NoTls) {
        Ok(it) => it,
        Err(err) => {
            report
                .warnings
                .push(format!("failed to connect PostgreSQL: {err}"));
            return report;
        }
    };
    report.capability.postgres_connected = true;

    match fetch_status_pairs(&mut client) {
        Ok(map) => {
            report.capability.has_status_access = true;
            report.postgres.global_status = map;
        }
        Err(err) => report
            .warnings
            .push(format!("failed querying pg_stat_database: {err}")),
    }

    match fetch_settings_pairs(&mut client) {
        Ok(map) => {
            report.capability.has_settings_access = true;
            report.postgres.global_variables = map;
        }
        Err(err) => report
            .warnings
            .push(format!("failed querying pg_settings: {err}")),
    }

    let table_result = fetch_table_sizes(&mut client, config.table_limit);
    let index_result = fetch_indexes(&mut client, config.index_limit);
    let mut table_ok = false;
    let mut index_ok = false;
    if let Ok(rows) = table_result {
        table_ok = true;
        report.postgres.table_sizes = rows;
    } else if let Err(err) = table_result {
        report
            .warnings
            .push(format!("failed querying relation sizes: {err}"));
    }
    if let Ok(rows) = index_result {
        index_ok = true;
        report.postgres.indexes = rows;
    } else if let Err(err) = index_result {
        report
            .warnings
            .push(format!("failed querying pg_indexes: {err}"));
    }
    report.capability.has_storage_access = table_ok && index_ok;

    match fetch_replication_status(&mut client) {
        Ok(map) => {
            report.capability.has_replication_status_access = true;
            report.postgres.replication_status = map;
        }
        Err(err) => report
            .warnings
            .push(format!("failed querying replication status: {err}")),
    }

    report
}

fn fetch_settings_pairs(client: &mut Client) -> Result<BTreeMap<String, String>, postgres::Error> {
    let rows = client.query("SELECT name, setting FROM pg_settings", &[])?;
    Ok(rows
        .into_iter()
        .map(|it| (it.get::<_, String>(0), it.get::<_, String>(1)))
        .collect())
}

fn fetch_status_pairs(client: &mut Client) -> Result<BTreeMap<String, String>, postgres::Error> {
    let rows = client.query(
        "SELECT metric, value FROM (
             SELECT 'numbackends' AS metric, COALESCE(SUM(numbackends), 0)::text AS value FROM pg_stat_database
             UNION ALL
             SELECT 'xact_commit', COALESCE(SUM(xact_commit), 0)::text FROM pg_stat_database
             UNION ALL
             SELECT 'xact_rollback', COALESCE(SUM(xact_rollback), 0)::text FROM pg_stat_database
             UNION ALL
             SELECT 'blks_read', COALESCE(SUM(blks_read), 0)::text FROM pg_stat_database
             UNION ALL
             SELECT 'blks_hit', COALESCE(SUM(blks_hit), 0)::text FROM pg_stat_database
             UNION ALL
             SELECT 'tup_returned', COALESCE(SUM(tup_returned), 0)::text FROM pg_stat_database
             UNION ALL
             SELECT 'tup_fetched', COALESCE(SUM(tup_fetched), 0)::text FROM pg_stat_database
             UNION ALL
             SELECT 'tup_inserted', COALESCE(SUM(tup_inserted), 0)::text FROM pg_stat_database
             UNION ALL
             SELECT 'tup_updated', COALESCE(SUM(tup_updated), 0)::text FROM pg_stat_database
             UNION ALL
             SELECT 'tup_deleted', COALESCE(SUM(tup_deleted), 0)::text FROM pg_stat_database
             UNION ALL
             SELECT 'deadlocks', COALESCE(SUM(deadlocks), 0)::text FROM pg_stat_database
         ) s",
        &[],
    )?;
    Ok(rows
        .into_iter()
        .map(|it| (it.get::<_, String>(0), it.get::<_, String>(1)))
        .collect())
}

fn fetch_table_sizes(
    client: &mut Client,
    limit: usize,
) -> Result<Vec<PostgresTableSizeEntry>, postgres::Error> {
    let rows = client.query(
        "SELECT n.nspname AS table_schema,
                c.relname AS table_name,
                COALESCE(c.reltuples, 0)::bigint AS estimated_rows,
                COALESCE(pg_relation_size(c.oid), 0)::bigint AS data_length,
                COALESCE(pg_indexes_size(c.oid), 0)::bigint AS index_length,
                COALESCE(pg_total_relation_size(c.oid), 0)::bigint AS total_length
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind = 'r'
           AND n.nspname NOT IN ('pg_catalog', 'information_schema')
         ORDER BY total_length DESC
         LIMIT $1",
        &[&(limit as i64)],
    )?;
    Ok(rows.into_iter().map(parse_table_size_row).collect())
}

fn parse_table_size_row(row: Row) -> PostgresTableSizeEntry {
    PostgresTableSizeEntry {
        table_schema: row.get::<_, String>(0),
        table_name: row.get::<_, String>(1),
        estimated_rows: row.get::<_, i64>(2),
        data_length: row.get::<_, i64>(3),
        index_length: row.get::<_, i64>(4),
        total_length: row.get::<_, i64>(5),
    }
}

fn fetch_indexes(
    client: &mut Client,
    limit: usize,
) -> Result<Vec<PostgresIndexEntry>, postgres::Error> {
    let rows = client.query(
        "SELECT schemaname, tablename, indexname, indexdef
         FROM pg_indexes
         WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
         ORDER BY schemaname, tablename, indexname
         LIMIT $1",
        &[&(limit as i64)],
    )?;
    Ok(rows
        .into_iter()
        .map(|it| PostgresIndexEntry {
            table_schema: it.get::<_, String>(0),
            table_name: it.get::<_, String>(1),
            index_name: it.get::<_, String>(2),
            index_def: it.get::<_, String>(3),
        })
        .collect())
}

fn fetch_replication_status(
    client: &mut Client,
) -> Result<BTreeMap<String, String>, postgres::Error> {
    let rows = client.query(
        "SELECT metric, value FROM (
             SELECT 'is_in_recovery' AS metric, pg_is_in_recovery()::text AS value
             UNION ALL
             SELECT 'replication_clients', COALESCE(COUNT(*), 0)::text FROM pg_stat_replication
             UNION ALL
             SELECT 'wal_receiver_status',
                    COALESCE((SELECT status FROM pg_stat_wal_receiver LIMIT 1), '')
         ) s",
        &[],
    )?;
    Ok(rows
        .into_iter()
        .map(|it| (it.get::<_, String>(0), it.get::<_, String>(1)))
        .collect())
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|it| it.as_millis())
        .unwrap_or(0)
}
