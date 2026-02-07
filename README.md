# SQL Insight

Through SQLs, we turn EXPLAIN into action.

## What It Is

`sql-insight` is a MySQL observability MVP focused on **slow SQL attribution**.
Current implementation delivers **Level 0 (zero-intrusion, read-only)** collection for strict production environments.

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

## Project Layout

- `src/main.rs`: CLI entrypoint (`clap`) + runtime logs (`tracing`)
- `src/level0.rs`: Level 0 collectors and report schema
- `src/collection.rs`: level negotiation and task mapping
- `src/lib.rs`: module exports

## Quick Start

```bash
cargo run -- --help
```

Run Level 0 collection:

```bash
MYSQL_URL='mysql://user:pass@127.0.0.1:3306' cargo run -- --output pretty-json -v
```

Without `MYSQL_URL`, the tool skips MySQL and collects OS metrics only.

## CLI Options

- `--mysql-url` (or env `MYSQL_URL`)
- `--table-limit` (or env `LEVEL0_TABLE_LIMIT`, default `200`)
- `--index-limit` (or env `LEVEL0_INDEX_LIMIT`, default `500`)
- `--output json|pretty-json` (default `pretty-json`)
- `-v/--verbose` (`info` -> `debug` -> `trace`)

## Development

```bash
cargo fmt
cargo test
cargo clippy --all-targets --all-features
```

## Roadmap

- Level 1: slow log + error log ingestion with windowed activation
- Level 2: `performance_schema` and `sys` enhanced diagnostics
- Level 3: expert-mode short-window deep sampling (`tcpdump` / `perf` / `strace`)
