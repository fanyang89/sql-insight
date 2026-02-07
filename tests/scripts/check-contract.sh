#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULTS_DIR="${CONTRACT_RESULTS_DIR:-${ROOT_DIR}/tests/artifacts/results}"
CONTRACT_PROFILE="${CONTRACT_PROFILE:-full}"
GOLDEN_FILE="${CONTRACT_GOLDEN_FILE:-${ROOT_DIR}/tests/contracts/golden/matrix-summary.${CONTRACT_PROFILE}.json}"

CONTRACT_RESULTS_DIR="${RESULTS_DIR}" \
CONTRACT_REQUIRE_RESULTS=1 \
CONTRACT_GOLDEN_FILE="${GOLDEN_FILE}" \
cargo test --quiet --test contract_schema --manifest-path "${ROOT_DIR}/Cargo.toml"
