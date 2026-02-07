#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export RESULTS_DIR="${ROOT_DIR}/tests/artifacts/resilience/results"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

KEEP_CONTAINERS="${KEEP_CONTAINERS:-0}"

cleanup() {
  if [[ "${KEEP_CONTAINERS}" == "1" ]]; then
    echo "[cleanup] KEEP_CONTAINERS=1, skip docker compose down"
  else
    echo "[cleanup] docker compose down -v"
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "[setup] ensure directories"
ensure_test_dirs

echo "[setup] docker compose up"
compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d mysql80 postgres16

wait_for_service_healthy mysql80
wait_for_service_healthy postgres16

echo "[setup] build collector binary"
cargo build --quiet --manifest-path "${ROOT_DIR}/Cargo.toml"

run_mysql_timeout_retry_case() {
  local output="${RESULTS_DIR}/mysql_timeout_retry.json"

  echo "[case] mysql timeout retry"
  run_collector "${output}" \
    --engine mysql \
    --collect-level level1 \
    --run-mode once \
    --mysql-url "mysql://root:root@127.0.0.1:3308/app" \
    --slow-log-window-secs 5 \
    --timeout-secs 1 \
    --retry-times 2 \
    --retry-backoff-ms 100 \
    --output json

  assert_jq "${output}" '.status == "failed"' "mysql timeout status failed"
  assert_jq "${output}" '(.attempts | length) == 3' "mysql timeout retried 3 attempts"
  assert_jq "${output}" '([.attempts[].status] | all(. == "failed"))' "mysql timeout all attempts failed"
  assert_jq "${output}" '(.error | contains("timed out")) == true' "mysql timeout error surfaced"
}

run_mysql_downgrade_case() {
  local output="${RESULTS_DIR}/mysql_level1_downgrade.json"

  echo "[case] mysql level1 downgrade"
  run_collector "${output}" \
    --engine mysql \
    --collect-level level1 \
    --run-mode once \
    --mysql-url "mysql://root:root@127.0.0.1:3308/app" \
    --slow-log-window-secs 1 \
    --no-slow-log-hot-switch \
    --slow-log-path "/tmp/not-found-slow.log" \
    --error-log-path "/tmp/not-found-error.log" \
    --output json

  assert_jq "${output}" '.status == "ok"' "mysql downgrade record status"
  assert_jq "${output}" '.selected_level == "Level 0"' "mysql downgraded to level 0"
  assert_jq "${output}" '(.payload.downgrade_reasons | join(" ") | contains("cannot hot-enable slow log")) == true' "mysql downgrade reason hot switch"
  assert_jq "${output}" '(.payload.downgrade_reasons | join(" ") | contains("cannot collect slow log for digest aggregation")) == true' "mysql downgrade reason slow log"
  assert_jq "${output}" '(.payload.downgrade_reasons | join(" ") | contains("cannot collect error log")) == true' "mysql downgrade reason error log"
}

run_postgres_downgrade_case() {
  local output="${RESULTS_DIR}/postgres_level1_downgrade.json"

  echo "[case] postgres level1 downgrade"
  run_collector "${output}" \
    --engine postgres \
    --collect-level level1 \
    --run-mode once \
    --postgres-url "postgres://postgres:postgres@127.0.0.1:5434/app" \
    --slow-log-window-secs 1 \
    --no-slow-log-hot-switch \
    --slow-log-path "/tmp/not-found-statement.log" \
    --error-log-path "/tmp/not-found-error.log" \
    --output json

  assert_jq "${output}" '.status == "ok"' "postgres downgrade record status"
  assert_jq "${output}" '.selected_level == "Level 0"' "postgres downgraded to level 0"
  assert_jq "${output}" '(.payload.downgrade_reasons | join(" ") | contains("cannot hot-enable log_min_duration_statement")) == true' "postgres downgrade reason hot switch"
  assert_jq "${output}" '(.payload.downgrade_reasons | join(" ") | contains("cannot collect statement log for digest aggregation")) == true' "postgres downgrade reason statement log"
  assert_jq "${output}" '(.payload.downgrade_reasons | join(" ") | contains("cannot collect PostgreSQL log alerts")) == true' "postgres downgrade reason error log"
}

run_mysql_timeout_retry_case
run_mysql_downgrade_case
run_postgres_downgrade_case

echo "[result] all resilience cases passed"
