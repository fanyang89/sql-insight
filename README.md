# SQL Insight

Through SQLs, we turn EXPLAIN into action.

## What It Is

`sql-insight` is a MySQL observability MVP focused on **slow SQL attribution**.
Current implementation delivers:

- **MySQL 5.x / 8.x**: Level 0 + Level 1
- **PostgreSQL 14+**: Level 0 + Level 1

## Current MVP Scope (Level 0)

- MySQL read-only collection:
  - `SHOW GLOBAL STATUS`
  - `SHOW VARIABLES`
  - `INFORMATION_SCHEMA.TABLES` (table size)
  - `INFORMATION_SCHEMA.STATISTICS` (index metadata)
  - `SHOW REPLICA STATUS` / fallback `SHOW SLAVE STATUS`
- OS metrics collection:
  - `/proc/stat`, `/proc/meminfo`, `/proc/loadavg`
  - Optional commands: `vmstat`, `iostat`, `sar` (if installed)
- Capability-based level negotiation:
  - Determines the highest acceptable collection level and emits downgrade reasons.

## Current MVP Scope (Level 1)

- Slow log hot-switch collection:
  - `SET GLOBAL slow_query_log = ON`
  - configurable `long_query_time`
  - windowed capture with optional restore of original settings
- PostgreSQL statement-log hot-switch collection:
  - `ALTER SYSTEM SET log_min_duration_statement`
  - `SELECT pg_reload_conf()`
  - windowed capture with optional restore of original settings
- Slow log digest aggregation:
  - SQL fingerprint normalization
  - grouped counts, latency totals/averages, rows examined/sent
- Error log alert extraction:
  - `deadlock`
  - `crash recovery`
  - `purge`
  - `replication`

## PostgreSQL Support (Phase 1)

- Level 0 read-only collection:
  - `pg_stat_database` aggregated status counters
  - `pg_settings`
  - relation and index metadata (`pg_class`/`pg_namespace`/`pg_indexes`)
  - replication state (`pg_stat_replication`, `pg_stat_wal_receiver`, `pg_is_in_recovery()`)
- Level 1 lightweight diagnostics:
  - statement log window capture + digest aggregation
  - error log alert extraction (`deadlock`, `crash_recovery`, `purge/vacuum`, `replication`)

## Project Layout

- `src/main.rs`: CLI entrypoint (`clap`) + runtime logs (`tracing`)
- `src/level0.rs`: Level 0 collectors and report schema
- `src/collection.rs`: level negotiation and task mapping
- `src/pipeline.rs`: unified collection contract + scheduler utilities
- `src/lib.rs`: module exports

## Quick Start

```bash
cargo run -- --help
```

Run Level 1 collection:

```bash
MYSQL_URL='mysql://user:pass@127.0.0.1:3306' cargo run -- --collect-level level1 --output pretty-json -v
```

Run Level 0 only:

```bash
cargo run -- --collect-level level0 --output json
```

Run in scheduler mode (2 cycles):

```bash
cargo run -- --run-mode daemon --max-cycles 2 --interval-secs 30 --collect-level level0 --output json
```

Run PostgreSQL Level 0:

```bash
POSTGRES_URL='postgres://user:pass@127.0.0.1:5432/postgres' cargo run -- --engine postgres --collect-level level0 --output pretty-json -v
```

Run PostgreSQL Level 1:

```bash
POSTGRES_URL='postgres://user:pass@127.0.0.1:5432/postgres' cargo run -- --engine postgres --collect-level level1 --slow-log-path /var/log/postgresql/postgresql.log --error-log-path /var/log/postgresql/postgresql.log --output pretty-json -v
```

Without `MYSQL_URL`, the tool skips MySQL collection and keeps available local OS metrics.

## CLI Options

- Scheduler:
  - `--run-mode once|daemon` (env `RUN_MODE`, default `once`)
  - `--interval-secs` / `--jitter-pct`
  - `--timeout-secs` / `--retry-times` / `--retry-backoff-ms`
  - `--max-cycles` (optional)
- `--mysql-url` (or env `MYSQL_URL`)
- `--postgres-url` (or env `POSTGRES_URL`)
- `--engine mysql|postgres` (or env `DB_ENGINE`, default `mysql`)
- `--collect-level level0|level1` (default `level1`)
- `--table-limit` (or env `LEVEL0_TABLE_LIMIT`, default `200`)
- `--index-limit` (or env `LEVEL0_INDEX_LIMIT`, default `500`)
- `--slow-log-window-secs` (env `LEVEL1_SLOW_LOG_WINDOW_SECS`, default `30`)
- `--slow-log-long-query-time-secs` (env `LEVEL1_LONG_QUERY_TIME_SECS`, default `0.2`)
- `--slow-log-path` (env `LEVEL1_SLOW_LOG_PATH`)
- `--error-log-path` (env `LEVEL1_ERROR_LOG_PATH`)
- `--max-slow-log-bytes` / `--max-error-log-bytes` / `--max-error-log-lines`
- `--no-slow-log-hot-switch` / `--no-restore-slow-log-settings`
- `--output json|pretty-json` (default `pretty-json`)
- `-v/--verbose` (`info` -> `debug` -> `trace`)

## Unified Data Contract

Collector output now uses a unified record envelope (JSON):

- `contract_version`, `run_id`, `cycle`
- `engine`, `requested_level`, `selected_level`
- `schedule` (mode/interval/timeout/retry policy)
- `window` (start/end/duration)
- `attempts` (per-attempt status and error)
- `source_status` (per-source success flags)
- `warnings`, `status`, `error`
- `payload` (engine-specific collection report)

## Development

```bash
cargo fmt
cargo test
cargo clippy --all-targets --all-features
```

## Compatibility Testing

Run the multi-version database matrix test (MySQL 5.7/8.0 + PostgreSQL 14/16):

```bash
bash tests/scripts/run-matrix.sh
```

Run smoke matrix (PR fast gate):

```bash
MATRIX_PROFILE=smoke bash tests/scripts/run-matrix.sh
```

The script validates:

- collector compatibility by database/version
- Level 0 capability extraction
- MySQL Level 1 slow-log digest and error-log alert collection
- PostgreSQL Level 1 statement-log digest and error-log alert collection

Set `KEEP_CONTAINERS=1` to keep containers running after the script exits.

## Contract Regression Gate

Validate matrix outputs against the `v1` JSON schema and golden summary:

```bash
bash tests/scripts/check-contract.sh
```

Use `CONTRACT_PROFILE=smoke` for PR smoke outputs.

Schema file:

- `contracts/unified-record-v1.schema.json`

Golden summary baseline:

- `tests/contracts/golden/matrix-summary.full.json`
- `tests/contracts/golden/matrix-summary.smoke.json`

CI applies this in two layers:

- PR gate: unit tests + parser regression + smoke matrix + contract gate + budget gate
- Nightly gate: unit tests + full matrix + contract gate + budget gate + resilience gate

## Budget & Resilience Gates

Check runtime and payload budgets on collected matrix outputs:

```bash
bash tests/scripts/check-budget.sh
```

Run resilience scenarios (timeout/retry and forced Level 1 downgrade):

```bash
bash tests/scripts/run-resilience.sh
```

Nightly CI additionally runs budget and resilience gates after full matrix.

## Roadmap

- Level 2: `performance_schema` and `sys` enhanced diagnostics (MySQL), `pg_stat_statements` path (PostgreSQL)
- Level 3: expert-mode short-window deep sampling (`tcpdump` / `perf` / `strace`)
