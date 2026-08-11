#!/usr/bin/env bash
set -euo pipefail

# Reproduces IGNITE-27453 on the disposable three-node standalone cluster.
# The harness node3 corresponds to the third production node (often named node2).
# Prerequisites:
#   01-build-ignite.sh
#   02-init-3node-cluster.sh start
# Run this script once on the pre-fix build and once on the fixed build with the
# same environment variables; compare results/safetime-dirty-page/<run-id>/summary.csv.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VALIDATION_ROOT="${IGNITE_VALIDATION_ROOT:-${REPO_ROOT}/build/job-classloader-cache-standalone}"
RUN_ID="${IGNITE_SAFETIME_REPRO_RUN_ID:-default}"
if [[ ! "${RUN_ID}" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "ERROR: IGNITE_SAFETIME_REPRO_RUN_ID contains unsupported characters: '${RUN_ID}'" >&2
    exit 2
fi
RUNTIME_ROOT="${VALIDATION_ROOT}/runtime"
CLI_STATE_HOME="${VALIDATION_ROOT}/cli-home"
LOG_DIR="${VALIDATION_ROOT}/logs"
RESULT_DIR="${VALIDATION_ROOT}/results/safetime-dirty-page/${RUN_ID}"
RUN_LOG="${LOG_DIR}/07-safetime-dirty-page-reproduction-${RUN_ID}.log"
SQL_FILE="${VALIDATION_ROOT}/safetime-dirty-page-reproduction.sql"
SUMMARY_CSV="${RESULT_DIR}/summary.csv"
CPU_CSV="${RESULT_DIR}/cpu.csv"
PORT_OFFSET="${IGNITE_PORT_OFFSET:-0}"
REST_URL="http://127.0.0.1:$((10300 + PORT_OFFSET))"
ZONE_NAME="SAFETIME_DIRTY_PAGE_REPRO_ZONE"
TABLE_PREFIX="SAFETIME_DIRTY_PAGE_REPRO_T"

TABLE_COUNT="${IGNITE_SAFETIME_REPRO_TABLES:-15}"
PARTITION_COUNT="${IGNITE_SAFETIME_REPRO_PARTITIONS:-32}"
REPLICA_COUNT="${IGNITE_SAFETIME_REPRO_REPLICAS:-3}"
REPRO_NODE_INDEX="${IGNITE_SAFETIME_REPRO_NODE_INDEX:-3}"
SAFE_TIME_INTERVAL_MS="${IGNITE_SAFETIME_REPRO_INTERVAL_MS:-100}"
CHECKPOINT_INTERVAL_MS="${IGNITE_SAFETIME_REPRO_CHECKPOINT_INTERVAL_MS:-10000}"
SETTLE_SECONDS="${IGNITE_SAFETIME_REPRO_SETTLE_SECONDS:-30}"
BASELINE_SECONDS="${IGNITE_SAFETIME_REPRO_BASELINE_SECONDS:-20}"
OFFLINE_SECONDS="${IGNITE_SAFETIME_REPRO_OFFLINE_SECONDS:-30}"
OBSERVE_SECONDS="${IGNITE_SAFETIME_REPRO_OBSERVE_SECONDS:-60}"
KEEP_FAST_INTERVALS="${IGNITE_SAFETIME_REPRO_KEEP_FAST_INTERVALS:-false}"

DEFAULT_SAFE_TIME_INTERVAL_MS=1000
DEFAULT_CHECKPOINT_INTERVAL_MS=180000
DEFAULT_CHECKPOINT_DEVIATION_PERCENT=40
CONFIG_CHANGED=false
REPRO_NODE_STOPPED=false
CPU_SAMPLER_PID=""

mkdir -p "${LOG_DIR}" "${RESULT_DIR}"

if [[ "${IGNITE_SAFETIME_REPRO_LOGGING_ACTIVE:-false}" != "true" ]]; then
    : > "${RUN_LOG}"
    set +e
    IGNITE_SAFETIME_REPRO_LOGGING_ACTIVE=true "${BASH_SOURCE[0]}" "$@" 2>&1 | tee -a "${RUN_LOG}"
    script_status=${PIPESTATUS[0]}
    exit "${script_status}"
fi

require_positive_integer() {
    local name="$1"
    local value="$2"

    if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
        echo "ERROR: ${name} must be a positive integer, got '${value}'" >&2
        exit 2
    fi
}

for pair in \
    "TABLE_COUNT:${TABLE_COUNT}" \
    "PARTITION_COUNT:${PARTITION_COUNT}" \
    "REPLICA_COUNT:${REPLICA_COUNT}" \
    "SAFE_TIME_INTERVAL_MS:${SAFE_TIME_INTERVAL_MS}" \
    "CHECKPOINT_INTERVAL_MS:${CHECKPOINT_INTERVAL_MS}" \
    "SETTLE_SECONDS:${SETTLE_SECONDS}" \
    "BASELINE_SECONDS:${BASELINE_SECONDS}" \
    "OFFLINE_SECONDS:${OFFLINE_SECONDS}" \
    "OBSERVE_SECONDS:${OBSERVE_SECONDS}"; do
    require_positive_integer "${pair%%:*}" "${pair#*:}"
done

if [[ "${REPRO_NODE_INDEX}" != "2" && "${REPRO_NODE_INDEX}" != "3" ]]; then
    echo "ERROR: IGNITE_SAFETIME_REPRO_NODE_INDEX must be 2 or 3 so node1 remains available for control." >&2
    exit 2
fi

CLI_HOME="$(find "${RUNTIME_ROOT}/dist" -maxdepth 1 -type d -name 'ignite3-cli-*' | head -1)"
if [[ -z "${CLI_HOME}" || ! -x "${CLI_HOME}/bin/ignite3" ]]; then
    echo "ERROR: CLI distribution not found. Run 01-build-ignite.sh and 02-init-3node-cluster.sh start first." >&2
    exit 1
fi

run_cli() {
    HOME="${CLI_STATE_HOME}" IGNITE3_OPTS="-Duser.home=${CLI_STATE_HOME}" "${CLI_HOME}/bin/ignite3" "$@"
}

wait_for_topology() {
    local expected="$1"

    for _ in $(seq 1 120); do
        local json
        json="$(curl -fsS "${REST_URL}/management/v1/cluster/topology/logical" 2>/dev/null || true)"
        if [[ -n "${json}" ]] && JSON="${json}" EXPECTED="${expected}" python3 -c \
            'import json,os,sys; sys.exit(0 if len(json.loads(os.environ["JSON"])) == int(os.environ["EXPECTED"]) else 1)' \
            2>/dev/null; then
            return 0
        fi
        sleep 1
    done

    echo "ERROR: Logical topology did not reach ${expected} nodes" >&2
    return 1
}

node_log_dir() {
    echo "${RUNTIME_ROOT}/nodes/node$1/log"
}

node_pid() {
    local file="${RUNTIME_ROOT}/nodes/node$1/run/java.pid"
    [[ -f "${file}" ]] && tr -d '[:space:]' < "${file}"
}

set_test_intervals() {
    CONFIG_CHANGED=true

    run_cli cluster config update --url "${REST_URL}" \
        "ignite.replication.idleSafeTimePropagationDurationMillis=${SAFE_TIME_INTERVAL_MS}"

    for index in 1 2 3; do
        local url="http://127.0.0.1:$((10299 + PORT_OFFSET + index))"
        run_cli node config update --url "${url}" \
            "ignite.storage.engines.aipersist.checkpoint.intervalMillis=${CHECKPOINT_INTERVAL_MS}"
        run_cli node config update --url "${url}" \
            'ignite.storage.engines.aipersist.checkpoint.intervalDeviationPercent=0'
    done

}

restore_default_intervals() {
    if [[ "${CONFIG_CHANGED}" != "true" || "${KEEP_FAST_INTERVALS}" == "true" ]]; then
        return
    fi

    echo "Restoring harness default SafeTime/checkpoint intervals"
    run_cli cluster config update --url "${REST_URL}" \
        "ignite.replication.idleSafeTimePropagationDurationMillis=${DEFAULT_SAFE_TIME_INTERVAL_MS}" >/dev/null || true

    for index in 1 2 3; do
        local url="http://127.0.0.1:$((10299 + PORT_OFFSET + index))"
        run_cli node config update --url "${url}" \
            "ignite.storage.engines.aipersist.checkpoint.intervalMillis=${DEFAULT_CHECKPOINT_INTERVAL_MS}" >/dev/null || true
        run_cli node config update --url "${url}" \
            "ignite.storage.engines.aipersist.checkpoint.intervalDeviationPercent=${DEFAULT_CHECKPOINT_DEVIATION_PERCENT}" >/dev/null || true
    done
}

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM

    if [[ -n "${CPU_SAMPLER_PID}" ]]; then
        kill "${CPU_SAMPLER_PID}" >/dev/null 2>&1 || true
    fi

    if [[ "${REPRO_NODE_STOPPED}" == "true" ]]; then
        "${SCRIPT_DIR}/02-init-3node-cluster.sh" start-node "${REPRO_NODE_INDEX}" >/dev/null 2>&1 || true
    fi

    restore_default_intervals
    exit "${exit_code}"
}
trap cleanup EXIT INT TERM

capture_offsets() {
    local phase="$1"

    for index in 1 2 3; do
        date '+%Y-%m-%d %H:%M:%S' > "${RESULT_DIR}/${phase}-node${index}.offset"
    done
}

collect_since_offsets() {
    local phase="$1"

    for index in 1 2 3; do
        local log_dir offset output
        log_dir="$(node_log_dir "${index}")"
        offset="$(< "${RESULT_DIR}/${phase}-node${index}.offset")"
        output="${RESULT_DIR}/${phase}-node${index}.log"
        python3 - "${log_dir}" "${offset}" "${output}" <<'PY'
import pathlib
import sys

log_dir = pathlib.Path(sys.argv[1])
start = sys.argv[2]
output = pathlib.Path(sys.argv[3])
entries = []
for log_path in log_dir.glob("ignite3db-*.log"):
    for line in log_path.read_text(errors="replace").splitlines(keepends=True):
        if len(line) >= 19 and line[:19] >= start:
            entries.append((line[:23], line))
entries.sort(key=lambda entry: entry[0])
output.write_text("".join(line for _, line in entries))
PY
    done
}

sample_cpu() {
    local phase="$1"
    local duration="$2"
    local end_epoch=$(( $(date +%s) + duration ))

    while [[ "$(date +%s)" -lt "${end_epoch}" ]]; do
        for index in 1 2 3; do
            local pid sample
            pid="$(node_pid "${index}" || true)"
            if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
                sample="$(ps -p "${pid}" -o %cpu= -o rss= | awk '{$1=$1; print}')"
                if [[ -n "${sample}" ]]; then
                    printf '%s,%s,node%d,%s,%s,%s\n' \
                        "$(date -Iseconds)" "${phase}" "${index}" "${pid}" ${sample} >> "${CPU_CSV}"
                fi
            fi
        done
        sleep 1
    done
}

generate_schema() {
    : > "${SQL_FILE}"

    for index in $(seq 1 "${TABLE_COUNT}"); do
        printf 'DROP TABLE IF EXISTS %s%02d;\n' "${TABLE_PREFIX}" "${index}" >> "${SQL_FILE}"
    done

    printf 'DROP ZONE IF EXISTS %s;\n' "${ZONE_NAME}" >> "${SQL_FILE}"
    printf "CREATE ZONE %s WITH STORAGE_PROFILES='default', PARTITIONS=%d, REPLICAS=%d;\n" \
        "${ZONE_NAME}" "${PARTITION_COUNT}" "${REPLICA_COUNT}" >> "${SQL_FILE}"

    for index in $(seq 1 "${TABLE_COUNT}"); do
        printf 'CREATE TABLE %s%02d (id BIGINT PRIMARY KEY, val VARCHAR(64)) ZONE %s;\n' \
            "${TABLE_PREFIX}" "${index}" "${ZONE_NAME}" >> "${SQL_FILE}"
        printf "INSERT INTO %s%02d (id, val) VALUES (1, 'seed');\n" \
            "${TABLE_PREFIX}" "${index}" >> "${SQL_FILE}"
    done
}

summarize_results() {
    python3 - "${RESULT_DIR}" "${SUMMARY_CSV}" "${CPU_CSV}" "${TABLE_COUNT}" "${PARTITION_COUNT}" "${REPRO_NODE_INDEX}" <<'PY'
import csv
import pathlib
import re
import statistics
import sys

result_dir = pathlib.Path(sys.argv[1])
summary_path = pathlib.Path(sys.argv[2])
cpu_path = pathlib.Path(sys.argv[3])
table_count = int(sys.argv[4])
partition_count = int(sys.argv[5])
restarted_node = f"node{int(sys.argv[6])}"

checkpoint = re.compile(r"checkpoint started .*pages=(\d+), reason='([^']+)'", re.IGNORECASE)
skipped_checkpoint = re.compile(r"skipping checkpoint \(no pages were modified\)", re.IGNORECASE)
compaction_start = re.compile(r"starting new compaction round .*files=(\d+)", re.IGNORECASE)
compaction_finish = re.compile(r"compaction round finished .*pages=(\d+), duration=(\d+)ms", re.IGNORECASE)

expected_files = table_count * partition_count
large_write_threshold = max(1, int(expected_files * 0.8))

rows = []
for phase in ("baseline", "restart"):
    for node in range(1, 4):
        text = (result_dir / f"{phase}-node{node}.log").read_text(errors="replace")
        checkpoints = checkpoint.findall(text)
        skipped_checkpoints = skipped_checkpoint.findall(text)
        starts = [int(value) for value in compaction_start.findall(text)]
        finishes = [(int(pages), int(duration)) for pages, duration in compaction_finish.findall(text)]
        rows.append({
            "phase": phase,
            "node": f"node{node}",
            "checkpoint_count": len(checkpoints),
            "checkpoint_pages": sum(int(pages) for pages, _ in checkpoints),
            "large_checkpoint_count": sum(int(pages) >= large_write_threshold for pages, _ in checkpoints),
            "skipped_checkpoint_count": len(skipped_checkpoints),
            "too_many_dirty_count": sum(reason.lower() == "too many dirty pages" for _, reason in checkpoints),
            "compaction_start_count": len(starts),
            "max_compaction_files": max(starts, default=0),
            "compaction_finish_count": len(finishes),
            "compaction_pages": sum(pages for pages, _ in finishes),
            "compaction_duration_ms": sum(duration for _, duration in finishes),
        })

with summary_path.open("w", newline="") as stream:
    writer = csv.DictWriter(stream, fieldnames=rows[0])
    writer.writeheader()
    writer.writerows(rows)

cpu_by_phase_node = {}
if cpu_path.exists():
    with cpu_path.open(newline="") as stream:
        for row in csv.DictReader(stream):
            cpu_by_phase_node.setdefault((row["phase"], row["node"]), []).append(float(row["cpu_percent"]))

for row in rows:
    values = cpu_by_phase_node.get((row["phase"], row["node"]), [])
    avg_cpu = statistics.fmean(values) if values else 0.0
    max_cpu = max(values, default=0.0)
    print(
        "SAFETIME_REPRO_SAMPLE "
        f"phase={row['phase']} node={row['node']} checkpoints={row['checkpoint_count']} "
        f"checkpointPages={row['checkpoint_pages']} largeCheckpoints={row['large_checkpoint_count']} "
        f"skippedCheckpoints={row['skipped_checkpoint_count']} maxCompactionFiles={row['max_compaction_files']} "
        f"compactionPages={row['compaction_pages']} avgCpu={avg_cpu:.1f} maxCpu={max_cpu:.1f}"
    )

restarted_node_row = next(row for row in rows if row["phase"] == "restart" and row["node"] == restarted_node)
recurring_round_threshold = 3
observed = restarted_node_row["large_checkpoint_count"] >= recurring_round_threshold
print(
    f"SAFETIME_DIRTY_PAGE_WRITE_AMPLIFICATION={'OBSERVED' if observed else 'NOT_OBSERVED'} "
    f"restartedNode={restarted_node} expectedTablePartitionFiles={expected_files} "
    f"largeWriteThresholdPages={large_write_threshold} recurringRoundThreshold={recurring_round_threshold} "
    f"observedLargeCheckpointRounds={restarted_node_row['large_checkpoint_count']} "
    f"skippedCheckpoints={restarted_node_row['skipped_checkpoint_count']}"
)
PY
}

echo "[1/8] Verify the disposable three-node harness"
"${SCRIPT_DIR}/02-init-3node-cluster.sh" status
wait_for_topology 3

for index in 1 2 3; do
    log_dir="$(node_log_dir "${index}")"
    if grep -q 'Zone based replication: false' "${log_dir}"/ignite3db-*.log; then
        echo "ERROR: node${index} has Zone based replication disabled; this scenario requires colocation." >&2
        exit 1
    fi
done

echo "[2/8] Accelerate SafeTime and PageMemory checkpoint intervals"
set_test_intervals

echo "[3/8] Create ${TABLE_COUNT} aipersist tables × ${PARTITION_COUNT} partitions × ${REPLICA_COUNT} replicas"
generate_schema
run_cli sql --jdbc-url "jdbc:ignite:thin://127.0.0.1:$((10800 + PORT_OFFSET))" --file "${SQL_FILE}"

echo "[4/8] Let DDL/seed writes drain for ${SETTLE_SECONDS}s"
sleep "${SETTLE_SECONDS}"

echo 'timestamp,phase,node,pid,cpu_percent,rss_kb' > "${CPU_CSV}"
echo "[5/8] Capture an idle baseline for ${BASELINE_SECONDS}s"
capture_offsets baseline
sample_cpu baseline "${BASELINE_SECONDS}"
collect_since_offsets baseline

echo "[6/8] Stop node${REPRO_NODE_INDEX} for ${OFFLINE_SECONDS}s so SafeTime Raft commands accumulate"
capture_offsets restart
"${SCRIPT_DIR}/02-init-3node-cluster.sh" stop-node "${REPRO_NODE_INDEX}"
REPRO_NODE_STOPPED=true
wait_for_topology 2
sleep "${OFFLINE_SECONDS}"

echo "[7/8] Restart node${REPRO_NODE_INDEX} with its original work directory and observe catch-up for ${OBSERVE_SECONDS}s"
sample_cpu restart "${OBSERVE_SECONDS}" &
CPU_SAMPLER_PID=$!
"${SCRIPT_DIR}/02-init-3node-cluster.sh" start-node "${REPRO_NODE_INDEX}"
REPRO_NODE_STOPPED=false
wait "${CPU_SAMPLER_PID}"
CPU_SAMPLER_PID=""
collect_since_offsets restart

echo '[8/8] Summarize checkpoint, compaction, and CPU evidence'
summarize_results

echo "RESULT_DIR=${RESULT_DIR}"
echo "SUMMARY_CSV=${SUMMARY_CSV}"
echo "CPU_CSV=${CPU_CSV}"
echo "RUN_LOG=${RUN_LOG}"
