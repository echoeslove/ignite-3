#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VALIDATION_ROOT="${IGNITE_VALIDATION_ROOT:-${REPO_ROOT}/build/job-classloader-cache-standalone}"
ARTIFACT_MANIFEST="${VALIDATION_ROOT}/artifacts/build-manifest.env"
RUNTIME_ROOT="${VALIDATION_ROOT}/runtime"
CLI_STATE_HOME="${VALIDATION_ROOT}/cli-home"
UNIT_ID="${IGNITE_COMPUTE_UNIT_ID:-classloader-marker}"
UNIT_VERSION="${IGNITE_COMPUTE_UNIT_VERSION:-1.0.8}"
REST_URL="http://127.0.0.1:10300"
ACTION="${1:-deploy}"

if [[ ! -f "${ARTIFACT_MANIFEST}" ]]; then
    echo "ERROR: Build manifest not found. Run 01-build-ignite.sh first." >&2
    exit 1
fi

# shellcheck disable=SC1090
source "${ARTIFACT_MANIFEST}"

CLI_HOME="$(find "${RUNTIME_ROOT}/dist" -maxdepth 1 -type d -name 'ignite3-cli-*' | head -1)"
if [[ -z "${CLI_HOME}" || ! -x "${CLI_HOME}/bin/ignite3" ]]; then
    echo "ERROR: CLI distribution not found. Run 02-init-3node-cluster.sh start first." >&2
    exit 1
fi

run_cli() {
    HOME="${CLI_STATE_HOME}" IGNITE3_OPTS="-Duser.home=${CLI_STATE_HOME}" "${CLI_HOME}/bin/ignite3" "$@"
}

wait_for_status() {
    local expected="$1"
    for _ in $(seq 1 120); do
        local json
        json="$(curl -fsS "${REST_URL}/management/v1/deployment/cluster/units/${UNIT_ID}?version=${UNIT_VERSION}" 2>/dev/null || true)"
        if [[ "${expected}" == "ABSENT" && "${json}" == "[]" ]]; then
            return 0
        fi
        if [[ "${expected}" == "DEPLOYED" && -n "${json}" ]] && JSON="${json}" python3 -c \
            'import json,os,sys; data=json.loads(os.environ["JSON"]); statuses=[v["status"] for u in data for v in u.get("versionToStatus",[])]; sys.exit(0 if "DEPLOYED" in statuses else 1)' 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "ERROR: Unit ${UNIT_ID}:${UNIT_VERSION} did not reach ${expected}" >&2
    return 1
}

deploy() {
    run_cli cluster unit deploy "${UNIT_ID}" --version "${UNIT_VERSION}" \
        --path "${COMPUTE_UNIT_JAR}" --nodes ALL --url "${REST_URL}"
    wait_for_status DEPLOYED

    for port in 10300 10301 10302; do
        local json
        json="$(curl -fsS "http://127.0.0.1:${port}/management/v1/deployment/node/units/${UNIT_ID}?version=${UNIT_VERSION}")"
        JSON="${json}" python3 -c \
            'import json,os,sys; data=json.loads(os.environ["JSON"]); statuses=[v["status"] for u in data for v in u.get("versionToStatus",[])]; sys.exit(0 if "DEPLOYED" in statuses else 1)'
    done
    echo "DEPLOY_RESULT=PASS unit=${UNIT_ID}:${UNIT_VERSION} nodes=ALL"
}

undeploy() {
    run_cli cluster unit undeploy "${UNIT_ID}" --version "${UNIT_VERSION}" --url "${REST_URL}"
    wait_for_status ABSENT
    echo "UNDEPLOY_RESULT=PASS unit=${UNIT_ID}:${UNIT_VERSION}"
}

case "${ACTION}" in
    deploy)
        deploy
        ;;
    undeploy)
        undeploy
        ;;
    redeploy)
        if curl -fsS "${REST_URL}/management/v1/deployment/cluster/units/${UNIT_ID}?version=${UNIT_VERSION}" | grep -q 'DEPLOYED'; then
            undeploy
        fi
        deploy
        ;;
    status)
        run_cli cluster unit list --plain --url "${REST_URL}" "${UNIT_ID}" --version "${UNIT_VERSION}"
        ;;
    *)
        echo "Usage: $0 deploy|undeploy|redeploy|status" >&2
        exit 2
        ;;
esac
