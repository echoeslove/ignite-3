#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VALIDATION_ROOT="${IGNITE_VALIDATION_ROOT:-${REPO_ROOT}/build/job-classloader-cache-standalone}"
RUNTIME_ROOT="${VALIDATION_ROOT}/runtime"
CLI_STATE_HOME="${VALIDATION_ROOT}/cli-home"
LOG_DIR="${VALIDATION_ROOT}/logs"
RESULT_DIR="${VALIDATION_ROOT}/results"
RUN_LOG="${LOG_DIR}/05-stability-test.log"
METASPACE_CSV="${RESULT_DIR}/metaspace.csv"
HARNESS_CLASSES="${RESULT_DIR}/harness-classes"
UNIT_ID="${IGNITE_COMPUTE_UNIT_ID:-classloader-marker}"
UNIT_VERSION="${IGNITE_COMPUTE_UNIT_VERSION:-1.0.8}"
NEXT_UNIT_VERSION="${IGNITE_COMPUTE_UNIT_NEXT_VERSION:-1.0.9}"
ADDRESSES="127.0.0.1:10800,127.0.0.1:10801,127.0.0.1:10802"
REST_URL="http://127.0.0.1:10300"
STRESS_BATCHES="${IGNITE_STRESS_BATCHES:-10}"
STRESS_ITERATIONS="${IGNITE_STRESS_ITERATIONS_PER_BATCH:-1000}"
STRESS_PARALLELISM="${IGNITE_STRESS_PARALLELISM:-16}"
NEW_VERSION_PARALLELISM="${IGNITE_NEW_VERSION_PARALLELISM:-1}"
PLATEAU_LIMIT_KB="${IGNITE_METASPACE_PLATEAU_LIMIT_KB:-2048}"

mkdir -p "${LOG_DIR}" "${RESULT_DIR}" "${HARNESS_CLASSES}"
: > "${RUN_LOG}"
exec > >(tee -a "${RUN_LOG}") 2>&1

JAVA_MAJOR="$(java -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.version =/{split($2, parts, "."); print parts[1]; exit}')"
if [[ "${JAVA_MAJOR}" != "25" ]]; then
    echo "ERROR: JDK 25 is required, current java.version=${JAVA_MAJOR}" >&2
    exit 1
fi

CLI_HOME="$(find "${RUNTIME_ROOT}/dist" -maxdepth 1 -type d -name 'ignite3-cli-*' | head -1)"
if [[ -z "${CLI_HOME}" || ! -x "${CLI_HOME}/bin/ignite3" ]]; then
    echo "ERROR: CLI distribution not found." >&2
    exit 1
fi

run_cli() {
    HOME="${CLI_STATE_HOME}" IGNITE3_OPTS="-Duser.home=${CLI_STATE_HOME}" "${CLI_HOME}/bin/ignite3" "$@"
}

run_probe() {
    java -cp "${CLI_HOME}/lib/*:${HARNESS_CLASSES}" StandaloneComputeProbe "$@"
}

node_pid() {
    tr -d '[:space:]' < "${RUNTIME_ROOT}/nodes/node$1/run/java.pid"
}

metaspace_used_kb() {
    local pid="$1"
    local nmt_summary
    nmt_summary="$(jcmd "${pid}" VM.native_memory summary)"
    NMT_SUMMARY="${nmt_summary}" python3 -c '
import os,re,sys
text=os.environ["NMT_SUMMARY"]
match=re.search(r"\(\s*Metadata:\s*\).*?\(\s*used=([0-9]+)KB\)", text, re.S)
if not match:
    print(text, file=sys.stderr)
    raise SystemExit("Metaspace used value not found")
print(match.group(1))
'
}

sample_metaspace() {
    local phase="$1"
    local batch="$2"

    for index in 1 2 3; do
        local pid used
        pid="$(node_pid "${index}")"
        if ! kill -0 "${pid}" 2>/dev/null; then
            echo "ERROR: node${index} PID ${pid} is not alive" >&2
            return 1
        fi
        jcmd "${pid}" GC.run >/dev/null
        used="$(metaspace_used_kb "${pid}")"
        printf '%s,%s,node%d,%s,%s\n' "$(date -Iseconds)" "${phase}" "${index}" "${batch}" "${used}" >> "${METASPACE_CSV}"
        echo "METASPACE phase=${phase} batch=${batch} node=node${index} usedKB=${used}"
    done
}

wait_for_topology() {
    local expected="$1"
    local topology_url="${2:-${REST_URL}}"
    for _ in $(seq 1 120); do
        local json
        json="$(curl -fsS "${topology_url}/management/v1/cluster/topology/logical" 2>/dev/null || true)"
        if [[ -n "${json}" ]] && JSON="${json}" EXPECTED="${expected}" python3 -c \
            'import json,os,sys; sys.exit(0 if len(json.loads(os.environ["JSON"]))==int(os.environ["EXPECTED"]) else 1)' 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "ERROR: logical topology did not reach ${expected}" >&2
    return 1
}

extract_marker() {
    sed -n 's/.*firstMarker=\([^ ]*\).*/\1/p' | tail -1
}

assert_unit_deployed_on_every_node() {
    local version="$1"

    for port in 10300 10301 10302; do
        local json
        json="$(curl -fsS "http://127.0.0.1:${port}/management/v1/deployment/node/units/${UNIT_ID}?version=${version}")"
        JSON="${json}" python3 -c \
            'import json,os,sys; data=json.loads(os.environ["JSON"]); statuses=[v["status"] for u in data for v in u.get("versionToStatus",[])]; sys.exit(0 if "DEPLOYED" in statuses else 1)'
    done
}

wait_for_unit_deployed_on_every_node() {
    local version="$1"

    for _ in $(seq 1 120); do
        if assert_unit_deployed_on_every_node "${version}" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done

    echo "ERROR: Unit ${UNIT_ID}:${version} was not restored on every node" >&2
    return 1
}

echo 'timestamp,phase,node,batch,used_kb' > "${METASPACE_CSV}"

echo "[1/11] Preflight: three independent ZIP-deployed nodes and JDK 25"
"${SCRIPT_DIR}/02-init-3node-cluster.sh" status
for index in 1 2 3; do
    pid="$(node_pid "${index}")"
    jcmd "${pid}" VM.version | grep -Eq 'version 25([. ]|$)'
    jcmd "${pid}" VM.native_memory summary > "${RESULT_DIR}/node${index}-nmt-before.txt"
done

echo "[2/11] Compile the standalone thin-client probe from the ZIP client libraries"
javac -cp "${CLI_HOME}/lib/*" -d "${HARNESS_CLASSES}" "${SCRIPT_DIR}/StandaloneComputeProbe.java"

echo "[3/11] Negative path: missing unit must fail, then the valid unit must still execute"
run_probe missing-unit "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 404.0.0

echo "[4/11] A/B behavior: disabled cache creates a loader per sequential execution"
run_cli node config update --url "${REST_URL}" 'ignite.compute.classLoaderCacheMaxIdleEntries=0'
DISABLED_RESULT="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 100 1 100)"
echo "${DISABLED_RESULT}"

echo "[5/11] Enable the repaired cache, warm up, and run sustained load with Metaspace sampling"
run_cli node config update --url "${REST_URL}" 'ignite.compute.classLoaderCacheMaxIdleEntries=16'
run_cli node config update --url "${REST_URL}" 'ignite.compute.classLoaderCacheIdleTimeoutMillis=600000'
run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 200 16 1
sample_metaspace enabled-warmup 0

: > "${RESULT_DIR}/stress-markers.txt"
for batch in $(seq 1 "${STRESS_BATCHES}"); do
    result="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" \
        "${STRESS_ITERATIONS}" "${STRESS_PARALLELISM}" 1)"
    echo "${result}"
    echo "${result}" | extract_marker >> "${RESULT_DIR}/stress-markers.txt"
    sample_metaspace enabled-stress "${batch}"
done

STRESS_MARKER_COUNT="$(sort -u "${RESULT_DIR}/stress-markers.txt" | wc -l | tr -d '[:space:]')"
if [[ "${STRESS_MARKER_COUNT}" != "1" ]]; then
    echo "ERROR: Cached workload used ${STRESS_MARKER_COUNT} class-loader markers across batches" >&2
    exit 1
fi

echo "[6/11] Publish both versions, roll all nodes, then verify the new version reaches a Metaspace plateau"
OLD_VERSION_MARKER="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 1 1 1 | extract_marker)"
IGNITE_COMPUTE_UNIT_VERSION="${NEXT_UNIT_VERSION}" "${SCRIPT_DIR}/03-deploy-compute-unit.sh" deploy
assert_unit_deployed_on_every_node "${UNIT_VERSION}"
assert_unit_deployed_on_every_node "${NEXT_UNIT_VERSION}"

for index in 1 2 3; do
    if [[ "${index}" == "1" ]]; then
        topology_url="http://127.0.0.1:10301"
    else
        topology_url="${REST_URL}"
    fi

    "${SCRIPT_DIR}/02-init-3node-cluster.sh" stop-node "${index}"
    wait_for_topology 2 "${topology_url}"
    "${SCRIPT_DIR}/02-init-3node-cluster.sh" start-node "${index}"
    wait_for_topology 3 "${topology_url}"
    wait_for_unit_deployed_on_every_node "${UNIT_VERSION}"
    wait_for_unit_deployed_on_every_node "${NEXT_UNIT_VERSION}"
done

CONTROL_VERSION_RESULT="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 1000 1 1)"
echo "${CONTROL_VERSION_RESULT}"
CONTROL_VERSION_MARKER="$(echo "${CONTROL_VERSION_RESULT}" | extract_marker)"

if [[ "${CONTROL_VERSION_MARKER}" == "${OLD_VERSION_MARKER}" ]]; then
    echo "ERROR: Rolling restart did not replace ${UNIT_VERSION}'s class loader marker" >&2
    exit 1
fi
echo "ROLLING_RESTART_RESULT=PASS oldBefore=${OLD_VERSION_MARKER} oldAfter=${CONTROL_VERSION_MARKER}"

NEW_VERSION_WARMUP="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${NEXT_UNIT_VERSION}" 200 1 1)"
echo "${NEW_VERSION_WARMUP}"
NEW_VERSION_MARKER="$(echo "${NEW_VERSION_WARMUP}" | extract_marker)"
LATEST_MARKER="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" latest 1 1 1 | extract_marker)"
OLD_VERSION_MARKER_AFTER_NEW_REQUEST="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 1 1 1 | extract_marker)"

if [[ "${NEW_VERSION_MARKER}" == "${CONTROL_VERSION_MARKER}" ]]; then
    echo "ERROR: New unit version reused the old version's class loader marker" >&2
    exit 1
fi
if [[ "${LATEST_MARKER}" != "${NEW_VERSION_MARKER}" ]]; then
    echo "ERROR: latest did not resolve to ${NEXT_UNIT_VERSION}" >&2
    exit 1
fi
if [[ "${OLD_VERSION_MARKER_AFTER_NEW_REQUEST}" != "${CONTROL_VERSION_MARKER}" ]]; then
    echo "ERROR: Requesting ${NEXT_UNIT_VERSION} replaced the cached marker for ${UNIT_VERSION}" >&2
    exit 1
fi

echo 'batch,marker' > "${RESULT_DIR}/new-version-markers.csv"
sample_metaspace new-version-warmup 0
for batch in $(seq 1 "${STRESS_BATCHES}"); do
    result="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${NEXT_UNIT_VERSION}" \
        "${STRESS_ITERATIONS}" "${NEW_VERSION_PARALLELISM}" 1)"
    echo "${result}"
    marker="$(echo "${result}" | extract_marker)"
    printf '%s,%s\n' "${batch}" "${marker}" >> "${RESULT_DIR}/new-version-markers.csv"
    sample_metaspace new-version-stress "${batch}"
done

NEW_VERSION_MARKER_COUNT="$(tail -n +2 "${RESULT_DIR}/new-version-markers.csv" | cut -d, -f2 | sort -u | wc -l | tr -d '[:space:]')"
if [[ "${NEW_VERSION_MARKER_COUNT}" != "1" ]]; then
    echo "ERROR: ${NEXT_UNIT_VERSION} used ${NEW_VERSION_MARKER_COUNT} class-loader markers across batches" >&2
    exit 1
fi
if ! tail -n +2 "${RESULT_DIR}/new-version-markers.csv" | cut -d, -f2 | grep -Fxq "${NEW_VERSION_MARKER}"; then
    echo "ERROR: ${NEXT_UNIT_VERSION} pressure marker differs from its warmup marker" >&2
    exit 1
fi
echo "NEW_VERSION_RESULT=PASS old=${UNIT_VERSION}:${CONTROL_VERSION_MARKER} new=${NEXT_UNIT_VERSION}:${NEW_VERSION_MARKER} latest=${LATEST_MARKER}"

echo "[7/11] TTL expiry: an idle loader expires and a later execution gets a new marker"
run_cli node config update --url "${REST_URL}" 'ignite.compute.classLoaderCacheIdleTimeoutMillis=2000'
TTL_BEFORE="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 1 1 1 | extract_marker)"
sleep 4
TTL_AFTER="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 1 1 1 | extract_marker)"
if [[ "${TTL_BEFORE}" == "${TTL_AFTER}" ]]; then
    echo "ERROR: Class-loader marker did not change after TTL expiry" >&2
    exit 1
fi
run_cli node config update --url "${REST_URL}" 'ignite.compute.classLoaderCacheIdleTimeoutMillis=600000'
echo "TTL_RESULT=PASS before=${TTL_BEFORE} after=${TTL_AFTER}"

echo "[8/11] Undeploy/redeploy invalidation: stale loader cannot be reused"
MARKER_BEFORE_REDEPLOY="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 1 1 1 | extract_marker)"
"${SCRIPT_DIR}/03-deploy-compute-unit.sh" undeploy
if run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 1 1 1 >/dev/null 2>&1; then
    echo "ERROR: Compute execution unexpectedly succeeded after undeploy" >&2
    exit 1
fi
"${SCRIPT_DIR}/03-deploy-compute-unit.sh" deploy
MARKER_AFTER_REDEPLOY="$(run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 1 1 1 | extract_marker)"
if [[ "${MARKER_BEFORE_REDEPLOY}" == "${MARKER_AFTER_REDEPLOY}" ]]; then
    echo "ERROR: Class-loader marker was reused after undeploy/redeploy" >&2
    exit 1
fi
echo "REDEPLOY_RESULT=PASS before=${MARKER_BEFORE_REDEPLOY} after=${MARKER_AFTER_REDEPLOY}"

echo "[9/11] Three-node execution and node3 outage/recovery"
for index in 1 2 3; do
    run_probe workload "${ADDRESSES}" "node${index}" "${UNIT_ID}" "${UNIT_VERSION}" 100 16 1
done
"${SCRIPT_DIR}/02-init-3node-cluster.sh" stop-node 3
wait_for_topology 2
run_probe workload "127.0.0.1:10800,127.0.0.1:10801" node1 "${UNIT_ID}" "${UNIT_VERSION}" 100 16 1
SQL_DURING_OUTAGE="$(run_cli sql --plain --jdbc-url jdbc:ignite:thin://127.0.0.1:10800 \
    'SELECT COUNT(*), SUM(id) FROM CLASSLOADER_VALIDATION;')"
echo "${SQL_DURING_OUTAGE}"
echo "${SQL_DURING_OUTAGE}" | grep -q '1000'
"${SCRIPT_DIR}/02-init-3node-cluster.sh" start-node 3
wait_for_topology 3
run_probe workload "${ADDRESSES}" node3 "${UNIT_ID}" "${UNIT_VERSION}" 100 16 1

echo "[10/11] Stability gate: Metaspace plateau and final process health"
sample_metaspace final 999
python3 - "${METASPACE_CSV}" "${PLATEAU_LIMIT_KB}" <<'PY'
import csv
import sys

csv_path = sys.argv[1]
limit_kb = int(sys.argv[2])
with open(csv_path, newline="") as stream:
    rows = list(csv.DictReader(stream))

node1 = [int(row["used_kb"]) for row in rows if row["node"] == "node1" and row["phase"] == "enabled-stress"]
if len(node1) < 4:
    raise SystemExit("Not enough node1 Metaspace samples")

tail = node1[-4:]
spread = max(tail) - min(tail)
print(f"METASPACE_PLATEAU tail={tail} spreadKB={spread} limitKB={limit_kb}")
if spread > limit_kb:
    raise SystemExit(f"Metaspace did not plateau: spread {spread} KB exceeds {limit_kb} KB")

new_version = [int(row["used_kb"]) for row in rows if row["node"] == "node1" and row["phase"] == "new-version-stress"]
if len(new_version) < 8:
    raise SystemExit("Not enough new-version Metaspace samples")

positive_growth = max(0, new_version[-1] - new_version[0])
new_tail = new_version[-4:]
new_spread = max(new_tail) - min(new_tail)
print(
    f"NEW_VERSION_METASPACE samples={new_version} positiveGrowthKB={positive_growth} "
    f"tail={new_tail} spreadKB={new_spread} limitKB={limit_kb}"
)
if positive_growth > limit_kb:
    raise SystemExit(f"New-version Metaspace kept growing: {positive_growth} KB exceeds {limit_kb} KB")
if new_spread > limit_kb:
    raise SystemExit(f"New-version Metaspace did not plateau: spread {new_spread} KB exceeds {limit_kb} KB")
PY

"${SCRIPT_DIR}/02-init-3node-cluster.sh" status
for index in 1 2 3; do
    pid="$(node_pid "${index}")"
    jcmd "${pid}" VM.native_memory summary > "${RESULT_DIR}/node${index}-nmt-after.txt"
done

echo "[11/11] Fatal-log gate"
FATAL_PATTERN='OutOfMemoryError|Critical failure|FATAL|SIGSEGV|Internal Error|hs_err_pid|AssertionError'
if grep -ERn "${FATAL_PATTERN}" "${RUNTIME_ROOT}/nodes"/node*/log/console*.log > "${RESULT_DIR}/fatal-log-matches.txt"; then
    cat "${RESULT_DIR}/fatal-log-matches.txt" >&2
    echo "ERROR: Fatal pattern found in node logs" >&2
    exit 1
fi

date -Iseconds > "${RESULT_DIR}/PASS"
echo "STABILITY_RESULT=PASS currentVersionJobs=$((STRESS_BATCHES * STRESS_ITERATIONS)) newVersionJobs=$((STRESS_BATCHES * STRESS_ITERATIONS)) markerCount=${STRESS_MARKER_COUNT} newVersionMarkerCount=${NEW_VERSION_MARKER_COUNT}"
