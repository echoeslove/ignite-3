#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <run-id>" >&2
    exit 2
fi

RUN_ID="$1"
CLUSTER_STARTED=false

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM

    if [[ "${CLUSTER_STARTED}" == "true" ]]; then
        "${SCRIPT_DIR}/02-init-3node-cluster.sh" stop >/dev/null 2>&1 || true
    fi

    exit "${exit_code}"
}
trap cleanup EXIT INT TERM

"${SCRIPT_DIR}/02-init-3node-cluster.sh" start
CLUSTER_STARTED=true

IGNITE_SAFETIME_REPRO_RUN_ID="${RUN_ID}" \
    "${SCRIPT_DIR}/07-reproduce-safetime-dirty-page-write-amplification.sh"

"${SCRIPT_DIR}/02-init-3node-cluster.sh" stop
CLUSTER_STARTED=false

