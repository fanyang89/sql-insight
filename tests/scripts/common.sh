#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/tests/compose/docker-compose.yml"
RESULTS_DIR="${ROOT_DIR}/tests/artifacts/results"
BIN_PATH="${ROOT_DIR}/target/debug/sql-insight"

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

wait_for_service_healthy() {
  local service="$1"
  local timeout_secs="${2:-180}"
  local start
  start="$(date +%s)"

  while true; do
    local container_id
    container_id="$(compose ps -q "${service}")"
    if [[ -n "${container_id}" ]]; then
      local health
      health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}")"
      if [[ "${health}" == "healthy" || "${health}" == "running" ]]; then
        echo "[wait] ${service} is ${health}"
        return 0
      fi
    fi

    if (( "$(date +%s)" - start > timeout_secs )); then
      echo "[wait] timeout waiting for ${service} to become healthy"
      compose ps
      return 1
    fi
    sleep 2
  done
}

ensure_test_dirs() {
  local uid gid
  uid="$(id -u)"
  gid="$(id -g)"

  mkdir -p "${ROOT_DIR}/tests/artifacts"
  # Bind-mounted files can become unwritable on host; repair ownership via a short-lived container.
  docker run --rm -v "${ROOT_DIR}/tests/artifacts:/artifacts" alpine:3.20 sh -lc "
    rm -rf /artifacts/mysql57 /artifacts/mysql80 /artifacts/postgres14 /artifacts/postgres16
    rm -rf /artifacts/results
    mkdir -p /artifacts/mysql57 /artifacts/mysql80 /artifacts/postgres14 /artifacts/postgres16 /artifacts/results
    : > /artifacts/mysql57/slow.log
    : > /artifacts/mysql57/error.log
    : > /artifacts/mysql80/slow.log
    : > /artifacts/mysql80/error.log
    : > /artifacts/postgres14/postgresql.log
    : > /artifacts/postgres16/postgresql.log
    chown -R ${uid}:${gid} /artifacts
    chmod -R 0777 /artifacts
  " >/dev/null
}

assert_jq() {
  local file="$1"
  local expr="$2"
  local message="$3"
  if jq -e "${expr}" "${file}" >/dev/null; then
    echo "[assert] PASS: ${message}"
  else
    echo "[assert] FAIL: ${message}"
    echo "[assert] jq expr: ${expr}"
    echo "[assert] file: ${file}"
    return 1
  fi
}

run_collector() {
  local output_file="$1"
  shift
  "${BIN_PATH}" "$@" >"${output_file}"
}

run_mysql_workload() {
  local service="$1"
  for i in 1 2 3 4; do
    compose exec -T "${service}" mysql -uroot -proot -e "USE app; SELECT SLEEP(0.35); SELECT COUNT(*) FROM orders WHERE user_id=${i};" >/dev/null
  done
}

inject_mysql_error_alerts() {
  local service="$1"
  compose exec -T "${service}" sh -lc "echo 'InnoDB: Deadlock found when trying to get lock' >> /var/log/mysql/error.log"
  compose exec -T "${service}" sh -lc "echo 'InnoDB: Starting crash recovery from checkpoint' >> /var/log/mysql/error.log"
  compose exec -T "${service}" sh -lc "echo 'replication applier thread stopped with error' >> /var/log/mysql/error.log"
}

run_postgres_workload() {
  local service="$1"
  for i in 1 2 3 4; do
    compose exec -T "${service}" psql -U postgres -d app -c "SELECT pg_sleep(0.35); SELECT COUNT(*) FROM orders WHERE user_id=${i};" >/dev/null
  done
}

inject_postgres_error_alerts() {
  local service="$1"
  compose exec -T "${service}" sh -lc "echo 'ERROR: deadlock detected' >> /var/log/postgresql/postgresql.log"
  compose exec -T "${service}" sh -lc "echo 'LOG: database system is starting up' >> /var/log/postgresql/postgresql.log"
  compose exec -T "${service}" sh -lc "echo 'LOG: wal receiver process terminated' >> /var/log/postgresql/postgresql.log"
}
