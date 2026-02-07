#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULTS_DIR="${CONTRACT_RESULTS_DIR:-${ROOT_DIR}/tests/artifacts/results}"

LEVEL0_MAX_DURATION_MS="${LEVEL0_MAX_DURATION_MS:-5000}"
LEVEL1_MAX_DURATION_MS="${LEVEL1_MAX_DURATION_MS:-15000}"
MAX_FILE_BYTES="${MAX_FILE_BYTES:-8000000}"

if [[ ! -d "${RESULTS_DIR}" ]]; then
  echo "[budget] missing results dir: ${RESULTS_DIR}"
  exit 1
fi

files=("${RESULTS_DIR}"/*.json)
if [[ ! -e "${files[0]}" ]]; then
  echo "[budget] no json files under ${RESULTS_DIR}"
  exit 1
fi

for file in "${files[@]}"; do
  name="$(basename "${file}")"
  status="$(jq -r '.status' "${file}")"
  duration_ms="$(jq -r '.window.duration_ms // 0' "${file}")"
  size_bytes="$(wc -c <"${file}")"

  if [[ "${status}" != "ok" ]]; then
    echo "[budget] FAIL ${name}: status=${status}"
    exit 1
  fi

  max_duration_ms="${LEVEL0_MAX_DURATION_MS}"
  if [[ "${name}" == *"_level1.json" ]]; then
    max_duration_ms="${LEVEL1_MAX_DURATION_MS}"
  fi

  if (( duration_ms > max_duration_ms )); then
    echo "[budget] FAIL ${name}: duration_ms=${duration_ms} > ${max_duration_ms}"
    exit 1
  fi
  if (( size_bytes > MAX_FILE_BYTES )); then
    echo "[budget] FAIL ${name}: bytes=${size_bytes} > ${MAX_FILE_BYTES}"
    exit 1
  fi

  echo "[budget] PASS ${name}: duration_ms=${duration_ms} bytes=${size_bytes}"
done

echo "[budget] all budget checks passed"
