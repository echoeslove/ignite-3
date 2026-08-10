#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VALIDATION_ROOT="${IGNITE_VALIDATION_ROOT:-${REPO_ROOT}/build/job-classloader-cache-baseline}"
RUNTIME_ROOT="${VALIDATION_ROOT}/runtime"
RESULT_DIR="${VALIDATION_ROOT}/results"
LOG_DIR="${VALIDATION_ROOT}/logs"
CLI_HOME="$(find "${RUNTIME_ROOT}/dist" -maxdepth 1 -type d -name 'ignite3-cli-*' | head -1)"
CLI_STATE_HOME="${VALIDATION_ROOT}/cli-home"
HARNESS_CLASSES="${RESULT_DIR}/harness-classes"
METASPACE_CSV="${RESULT_DIR}/baseline-metaspace.csv"
RUN_LOG="${LOG_DIR}/baseline-reproduction.log"
ADDRESSES="127.0.0.1:10800,127.0.0.1:10801,127.0.0.1:10802"
UNIT_ID="${IGNITE_COMPUTE_UNIT_ID:-classloader-marker}"
UNIT_VERSION="${IGNITE_COMPUTE_UNIT_VERSION:-1.0.0}"
BATCHES="${IGNITE_BASELINE_BATCHES:-8}"
ITERATIONS="${IGNITE_BASELINE_ITERATIONS_PER_BATCH:-500}"

if [[ -z "${CLI_HOME}" || ! -d "${CLI_HOME}/lib" ]]; then
    echo "ERROR: Baseline CLI distribution not found. Initialize the baseline cluster first." >&2
    exit 1
fi

mkdir -p "${RESULT_DIR}" "${LOG_DIR}" "${HARNESS_CLASSES}"
: > "${RUN_LOG}"
exec > >(tee -a "${RUN_LOG}") 2>&1

JAVA_MAJOR="$(java -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.version =/{split($2, parts, "."); print parts[1]; exit}')"
if [[ "${JAVA_MAJOR}" != "25" ]]; then
    echo "ERROR: JDK 25 is required, current java.version=${JAVA_MAJOR}" >&2
    exit 1
fi

node_pid() {
    tr -d '[:space:]' < "${RUNTIME_ROOT}/nodes/node$1/run/java.pid"
}

run_probe() {
    java -cp "${CLI_HOME}/lib/*:${HARNESS_CLASSES}" StandaloneComputeProbe "$@"
}

metaspace_used_kb() {
    local pid="$1"
    local nmt_summary
    nmt_summary="$(jcmd "${pid}" VM.native_memory summary)"
    NMT_SUMMARY="${nmt_summary}" python3 -c '
import os,re,sys
match=re.search(r"\(\s*Metadata:\s*\).*?\(\s*used=([0-9]+)KB\)", os.environ["NMT_SUMMARY"], re.S)
if not match:
    raise SystemExit("Metaspace used value not found")
print(match.group(1))
'
}

sample_node1() {
    local phase="$1"
    local batch="$2"
    local pid used
    pid="$(node_pid 1)"
    jcmd "${pid}" GC.run >/dev/null
    used="$(metaspace_used_kb "${pid}")"
    printf '%s,%s,%s,%s\n' "$(date -Iseconds)" "${phase}" "${batch}" "${used}" >> "${METASPACE_CSV}"
    echo "BASELINE_METASPACE phase=${phase} batch=${batch} usedKB=${used}"
}

echo 'timestamp,phase,batch,used_kb' > "${METASPACE_CSV}"

for index in 1 2 3; do
    pid="$(node_pid "${index}")"
    kill -0 "${pid}"
    jcmd "${pid}" VM.version | grep -Eq 'version 25([. ]|$)'
done

javac -cp "${CLI_HOME}/lib/*" -d "${HARNESS_CLASSES}" "${SCRIPT_DIR}/StandaloneComputeProbe.java"

echo "[1/3] Warm up the original server"
run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" 100 1 100
sample_node1 warmup 0

echo "[2/3] Run sequential batches; the original implementation must create one marker per job"
for batch in $(seq 1 "${BATCHES}"); do
    run_probe workload "${ADDRESSES}" node1 "${UNIT_ID}" "${UNIT_VERSION}" "${ITERATIONS}" 1 "${ITERATIONS}"
    sample_node1 stress "${batch}"
done

echo "[3/3] Evaluate reproduction evidence"
python3 - "${METASPACE_CSV}" <<'PY'
import csv
import sys

with open(sys.argv[1], newline="") as stream:
    rows = list(csv.DictReader(stream))

stress = [int(row["used_kb"]) for row in rows if row["phase"] == "stress"]
if len(stress) < 4:
    raise SystemExit("Not enough baseline Metaspace samples")

growth = stress[-1] - stress[0]
positive_steps = sum(right > left for left, right in zip(stress, stress[1:]))
print(f"BASELINE_METASPACE_TREND samples={stress} growthKB={growth} positiveSteps={positive_steps}")

if growth <= 0:
    raise SystemExit("Original-version Metaspace growth was not reproduced")
PY

for index in 1 2 3; do
    pid="$(node_pid "${index}")"
    kill -0 "${pid}"
done

date -Iseconds > "${RESULT_DIR}/REPRODUCED"
echo "ORIGINAL_METASPACE_REPRODUCTION=PASS jobs=$((BATCHES * ITERATIONS))"
