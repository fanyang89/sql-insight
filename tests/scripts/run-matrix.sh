#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
compose up -d mysql57 mysql80 postgres14 postgres16

wait_for_service_healthy mysql57
wait_for_service_healthy mysql80
wait_for_service_healthy postgres14
wait_for_service_healthy postgres16

echo "[setup] build collector binary"
cargo build --quiet --manifest-path "${ROOT_DIR}/Cargo.toml"

run_mysql_level0_case() {
  local tag="$1"
  local port="$2"
  local output="${RESULTS_DIR}/mysql_${tag}_level0.json"

  echo "[case] mysql ${tag} level0"
  run_collector "${output}" \
    --engine mysql \
    --collect-level level0 \
    --run-mode once \
    --mysql-url "mysql://root:root@127.0.0.1:${port}/app" \
    --output json

  assert_jq "${output}" '.status == "ok"' "mysql ${tag} level0 record status"
  assert_jq "${output}" '.selected_level == "Level 0"' "mysql ${tag} selected level 0"
  assert_jq "${output}" '.payload.level0.capability.mysql_status_access == true' "mysql ${tag} status access"
  assert_jq "${output}" '.payload.level0.capability.mysql_variables_access == true' "mysql ${tag} variables access"
  assert_jq "${output}" '.payload.level0.capability.information_schema_access == true' "mysql ${tag} information_schema access"
}

run_mysql_level1_case() {
  local service="$1"
  local tag="$2"
  local port="$3"
  local log_dir="${ROOT_DIR}/tests/artifacts/${tag}"
  local output="${RESULTS_DIR}/mysql_${tag}_level1.json"

  echo "[case] mysql ${tag} level1"
  inject_mysql_error_alerts "${service}"

  (
    sleep 1
    run_mysql_workload "${service}"
  ) &
  local workload_pid=$!

  run_collector "${output}" \
    --engine mysql \
    --collect-level level1 \
    --run-mode once \
    --mysql-url "mysql://root:root@127.0.0.1:${port}/app" \
    --slow-log-window-secs 5 \
    --slow-log-long-query-time-secs 0.1 \
    --slow-log-path "${log_dir}/slow.log" \
    --error-log-path "${log_dir}/error.log" \
    --output json
  wait "${workload_pid}"

  assert_jq "${output}" '.status == "ok"' "mysql ${tag} level1 record status"
  assert_jq "${output}" '.selected_level == "Level 1"' "mysql ${tag} selected level 1"
  assert_jq "${output}" '.payload.level1 != null' "mysql ${tag} level1 payload present"
  assert_jq "${output}" '.payload.level1.capability.can_enable_slow_log_hot_switch == true' "mysql ${tag} hot-switch capability"
  assert_jq "${output}" '.payload.level1.capability.can_read_slow_log == true' "mysql ${tag} slow log readable"
  assert_jq "${output}" '.payload.level1.capability.can_read_error_log == true' "mysql ${tag} error log readable"
  assert_jq "${output}" '.payload.level1.slow_log.digest_count >= 1' "mysql ${tag} slow log digest generated"
  assert_jq "${output}" '([.payload.level1.error_log.alerts[].category] | index("deadlock")) != null' "mysql ${tag} deadlock alert detected"
}

run_postgres_level0_case() {
  local service="$1"
  local tag="$2"
  local port="$3"
  local output="${RESULTS_DIR}/postgres_${tag}_level0.json"

  echo "[case] postgres ${tag} level0"
  compose exec -T "${service}" psql -U postgres -d app -c "SELECT COUNT(*) FROM orders;" >/dev/null

  run_collector "${output}" \
    --engine postgres \
    --collect-level level0 \
    --run-mode once \
    --postgres-url "postgres://postgres:postgres@127.0.0.1:${port}/app" \
    --output json

  assert_jq "${output}" '.status == "ok"' "postgres ${tag} level0 record status"
  assert_jq "${output}" '.selected_level == "Level 0"' "postgres ${tag} selected level 0"
  assert_jq "${output}" '.payload.postgres_level0.capability.has_status_access == true' "postgres ${tag} status access"
  assert_jq "${output}" '.payload.postgres_level0.capability.has_settings_access == true' "postgres ${tag} settings access"
  assert_jq "${output}" '.payload.postgres_level0.capability.has_storage_access == true' "postgres ${tag} storage access"
}

run_postgres_level1_downgrade_case() {
  local tag="$1"
  local port="$2"
  local output="${RESULTS_DIR}/postgres_${tag}_level1_downgrade.json"

  echo "[case] postgres ${tag} level1 downgrade"
  run_collector "${output}" \
    --engine postgres \
    --collect-level level1 \
    --run-mode once \
    --postgres-url "postgres://postgres:postgres@127.0.0.1:${port}/app" \
    --output json

  assert_jq "${output}" '.status == "ok"' "postgres ${tag} downgrade record status"
  assert_jq "${output}" '.selected_level == "Level 0"' "postgres ${tag} downgraded to level 0"
  assert_jq "${output}" '(.payload.downgrade_reasons | join(" ") | contains("postgres Level 1 is not implemented yet")) == true' "postgres ${tag} downgrade reason emitted"
}

run_mysql_level0_case "5_7" "3307"
run_mysql_level1_case "mysql57" "mysql57" "3307"
run_mysql_level0_case "8_0" "3308"
run_mysql_level1_case "mysql80" "mysql80" "3308"

run_postgres_level0_case "postgres14" "14" "5433"
run_postgres_level0_case "postgres16" "16" "5434"
run_postgres_level1_downgrade_case "16" "5434"

echo "[result] all matrix cases passed"
