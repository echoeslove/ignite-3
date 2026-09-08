#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VALIDATION_ROOT="${IGNITE_VALIDATION_ROOT:-${REPO_ROOT}/build/job-classloader-cache-standalone}"
ARTIFACT_MANIFEST="${VALIDATION_ROOT}/artifacts/build-manifest.env"
RUNTIME_ROOT="${VALIDATION_ROOT}/runtime"
CLI_STATE_HOME="${VALIDATION_ROOT}/cli-home"
CLUSTER_NAME="job-classloader-cache-validation"
ACTION="${1:-start}"
USE_LAUNCHCTL="${IGNITE_USE_LAUNCHCTL:-true}"
PORT_OFFSET="${IGNITE_PORT_OFFSET:-0}"
NODE_HEAP="${IGNITE_NODE_HEAP:-1g}"
NODE_COUNT="${IGNITE_NODE_COUNT:-3}"

if ! [[ "${NODE_COUNT}" =~ ^[0-9]+$ ]] || [[ "${NODE_COUNT}" -lt 3 ]]; then
    echo "ERROR: IGNITE_NODE_COUNT must be an integer of at least 3" >&2
    exit 2
fi

if [[ ! -f "${ARTIFACT_MANIFEST}" ]]; then
    echo "ERROR: Build manifest not found. Run 01-build-ignite.sh first." >&2
    exit 1
fi

# shellcheck disable=SC1090
source "${ARTIFACT_MANIFEST}"

JAVA_HOME="${JAVA_HOME:-$(java -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.home =/{print $2; exit}')}"
EXPECTED_RUNTIME_JAVA_MAJOR="${IGNITE_RUNTIME_JAVA_MAJOR:-25}"

if [[ ! -x "${JAVA_HOME}/bin/java" || ! -x "${JAVA_HOME}/bin/jcmd" ]]; then
    echo "ERROR: Runtime JDK is invalid: ${JAVA_HOME}" >&2
    exit 1
fi

ACTUAL_RUNTIME_JAVA_MAJOR="$("${JAVA_HOME}/bin/java" -XshowSettings:properties -version 2>&1 \
    | awk -F'= ' '/java.version =/{split($2, parts, "."); print parts[1]; exit}')"
if [[ "${ACTUAL_RUNTIME_JAVA_MAJOR}" != "${EXPECTED_RUNTIME_JAVA_MAJOR}" ]]; then
    echo "ERROR: JDK ${EXPECTED_RUNTIME_JAVA_MAJOR} is required, current java.version=${ACTUAL_RUNTIME_JAVA_MAJOR}" >&2
    exit 1
fi

node_rest_port() {
    echo $((10299 + PORT_OFFSET + $1))
}

node_client_port() {
    echo $((10799 + PORT_OFFSET + $1))
}

node_network_port() {
    echo $((3343 + PORT_OFFSET + $1))
}

node_dir() {
    echo "${RUNTIME_ROOT}/nodes/node$1"
}

pid_file() {
    echo "$(node_dir "$1")/run/java.pid"
}

launch_label() {
    echo "org.apache.ignite.validation.jobclassloader.node$1"
}

java_pid_for_name() {
    local name="$1"
    "${JAVA_HOME}/bin/jcmd" -l 2>/dev/null | awk -v needle="--node-name ${name}" 'index($0, needle) {print $1; exit}'
}

wait_for_node_rest() {
    local index="$1"
    local port
    port="$(node_rest_port "${index}")"

    for _ in $(seq 1 120); do
        if curl -fsS "http://127.0.0.1:${port}/management/v1/node/state" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "ERROR: node${index} REST endpoint did not become ready" >&2
    tail -100 "$(node_dir "${index}")/log/console.log" >&2 || true
    return 1
}

wait_for_logical_topology() {
    local expected="$1"

    for _ in $(seq 1 120); do
        local json
        json="$(curl -fsS "http://127.0.0.1:$(node_rest_port 1)/management/v1/cluster/topology/logical" 2>/dev/null || true)"
        if [[ -n "${json}" ]] && JSON="${json}" EXPECTED="${expected}" python3 -c \
            'import json,os,sys; data=json.loads(os.environ["JSON"]); sys.exit(0 if len(data)==int(os.environ["EXPECTED"]) else 1)' 2>/dev/null; then
            return 0
        fi
        sleep 1
    done

    echo "ERROR: logical topology did not reach ${expected} nodes" >&2
    return 1
}

stop_node() {
    local index="$1"
    local file
    file="$(pid_file "${index}")"

    if [[ ! -f "${file}" ]]; then
        if [[ "$(uname -s)" == "Darwin" && "${USE_LAUNCHCTL}" == "true" ]]; then
            launchctl remove "$(launch_label "${index}")" >/dev/null 2>&1 || true
        fi
        echo "node${index}: no PID file"
        return 0
    fi

    local pid command
    pid="$(tr -d '[:space:]' < "${file}")"

    if ! kill -0 "${pid}" 2>/dev/null; then
        if [[ "$(uname -s)" == "Darwin" && "${USE_LAUNCHCTL}" == "true" ]]; then
            launchctl remove "$(launch_label "${index}")" >/dev/null 2>&1 || true
        fi
        echo "node${index}: stale PID ${pid}"
        return 0
    fi

    command="$(ps -p "${pid}" -o command=)"
    if [[ "${command}" != *"--node-name node${index}"* ]]; then
        echo "ERROR: Refusing to stop PID ${pid}; it is not node${index}: ${command}" >&2
        return 1
    fi

    if [[ "$(uname -s)" == "Darwin" && "${USE_LAUNCHCTL}" == "true" ]]; then
        launchctl remove "$(launch_label "${index}")"
    else
        kill "${pid}"
    fi
    for _ in $(seq 1 30); do
        if ! kill -0 "${pid}" 2>/dev/null; then
            echo "node${index}: stopped"
            return 0
        fi
        sleep 1
    done

    command="$(ps -p "${pid}" -o command= 2>/dev/null || true)"
    if [[ "${command}" == *"--node-name node${index}"* ]]; then
        kill -9 "${pid}"
        echo "node${index}: force-stopped after graceful timeout"
    fi
}

stop_all() {
    for index in $(seq 1 "${NODE_COUNT}"); do
        stop_node "${index}"
    done
}

start_node() {
    local index="$1"
    local dir port pid
    dir="$(node_dir "${index}")"
    port="$(node_rest_port "${index}")"

    if [[ ! -x "${dir}/bin/ignite3db" ]]; then
        echo "ERROR: node${index} distribution is missing: ${dir}" >&2
        return 1
    fi

    pid="$(java_pid_for_name "node${index}" || true)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
        echo "node${index}: already running as PID ${pid}"
        printf '%s\n' "${pid}" > "$(pid_file "${index}")"
        return 0
    fi

    mkdir -p "${dir}/run" "${dir}/log"
    if [[ "$(uname -s)" == "Darwin" && "${USE_LAUNCHCTL}" == "true" ]]; then
        local label
        label="$(launch_label "${index}")"
        launchctl remove "${label}" >/dev/null 2>&1 || true
        launchctl submit -l "${label}" \
            -o "${dir}/log/console.log" \
            -e "${dir}/log/console-error.log" \
            -- /usr/bin/env JAVA_HOME="${JAVA_HOME}" "${dir}/bin/ignite3db"
    else
        (
            cd "${dir}"
            JAVA_HOME="${JAVA_HOME}" nohup "${dir}/bin/ignite3db" > "${dir}/log/console.log" 2>&1 &
            printf '%s\n' "$!" > "${dir}/run/launcher.pid"
        )
    fi

    for _ in $(seq 1 60); do
        pid="$(java_pid_for_name "node${index}" || true)"
        if [[ -n "${pid}" ]]; then
            printf '%s\n' "${pid}" > "$(pid_file "${index}")"
            break
        fi
        sleep 1
    done

    if [[ -z "${pid:-}" ]]; then
        echo "ERROR: Java PID for node${index} was not found" >&2
        tail -100 "${dir}/log/console.log" >&2 || true
        return 1
    fi

    wait_for_node_rest "${index}"
    echo "node${index}: ready on REST ${port}, PID ${pid}"
}

start_nodes() {
    if [[ "$#" -eq 0 ]]; then
        echo "ERROR: at least one node index is required" >&2
        return 2
    fi

    for index in "$@"; do
        start_node "${index}"
    done

    wait_for_logical_topology "${NODE_COUNT}"
    show_status
}

write_node_configuration() {
    local index="$1"
    local dir network_port client_port rest_port vars_file
    dir="$(node_dir "${index}")"
    network_port="$(node_network_port "${index}")"
    client_port="$(node_client_port "${index}")"
    rest_port="$(node_rest_port "${index}")"
    vars_file="${dir}/etc/vars.env"

    sed -i.bak \
        -e "s/^NODE_NAME=.*/NODE_NAME=node${index}/" \
        -e "s/^JVM_MAX_MEM=.*/JVM_MAX_MEM=\"${NODE_HEAP}\"/" \
        -e "s/^JVM_MIN_MEM=.*/JVM_MIN_MEM=\"${NODE_HEAP}\"/" \
        -e 's/^JVM_G1HeapRegionSize=.*/JVM_G1HeapRegionSize="4M"/' \
        -e "s|^IGNITE3_EXTRA_JVM_ARGS=.*|IGNITE3_EXTRA_JVM_ARGS=\"-XX:NativeMemoryTracking=summary -XX:+UnlockDiagnosticVMOptions -XX:+ClassUnloadingWithConcurrentMark -Xlog:class+unload=info -Xlog:gc*:file=${dir}/log/gc.log:time,uptime,level,tags:filecount=5,filesize=20M\"|" \
        "${vars_file}"

    local node_finder_entries=""
    local peer
    for peer in $(seq 1 "${NODE_COUNT}"); do
        node_finder_entries+="            \"127.0.0.1:$(node_network_port "${peer}")\","
    done
    node_finder_entries="${node_finder_entries%,}"

    if [[ "${IGNITE_BASELINE_NO_CACHE:-false}" == "true" ]]; then
        cat > "${dir}/etc/ignite-config.conf" <<EOF
ignite {
    network {
        port: ${network_port}
        nodeFinder.netClusterNodes: [
${node_finder_entries}
        ]
    }
    clientConnector.port: ${client_port}
    rest.port: ${rest_port}
}
EOF
    else
        cat > "${dir}/etc/ignite-config.conf" <<EOF
ignite {
    network {
        port: ${network_port}
        nodeFinder.netClusterNodes: [
${node_finder_entries}
        ]
    }
    clientConnector.port: ${client_port}
    rest.port: ${rest_port}
    compute {
        classLoaderCacheMaxIdleEntries: 16
        classLoaderCacheIdleTimeoutMillis: 600000
    }
}
EOF
    fi
}

prepare_runtime() {
    if [[ -d "${RUNTIME_ROOT}" ]]; then
        stop_all
        local backup="${RUNTIME_ROOT}.previous.$(date +%Y%m%d%H%M%S)"
        mv "${RUNTIME_ROOT}" "${backup}"
        echo "Previous runtime preserved at ${backup}"
    fi

    mkdir -p "${RUNTIME_ROOT}/dist" "${RUNTIME_ROOT}/nodes" "${CLI_STATE_HOME}"
    unzip -q "${IGNITE_ZIP}" -d "${RUNTIME_ROOT}/dist"

    local db_template
    db_template="$(find "${RUNTIME_ROOT}/dist" -maxdepth 1 -type d -name 'ignite3-db-*' | head -1)"
    if [[ -z "${db_template}" ]]; then
        echo "ERROR: DB distribution was not found in ${IGNITE_ZIP}" >&2
        exit 1
    fi

    for index in $(seq 1 "${NODE_COUNT}"); do
        cp -R "${db_template}" "$(node_dir "${index}")"
        write_node_configuration "${index}"
    done
}

cli_home() {
    find "${RUNTIME_ROOT}/dist" -maxdepth 1 -type d -name 'ignite3-cli-*' | head -1
}

run_cli() {
    local home
    home="$(cli_home)"
    HOME="${CLI_STATE_HOME}" IGNITE3_OPTS="-Duser.home=${CLI_STATE_HOME}" "${home}/bin/ignite3" "$@"
}

show_status() {
    local failures=0
    for index in $(seq 1 "${NODE_COUNT}"); do
        local file pid state version
        file="$(pid_file "${index}")"
        pid="$(test -f "${file}" && tr -d '[:space:]' < "${file}" || true)"
        if [[ -z "${pid}" ]] || ! kill -0 "${pid}" 2>/dev/null; then
            echo "node${index}: DOWN"
            failures=$((failures + 1))
            continue
        fi
        state="$(curl -fsS "http://127.0.0.1:$(node_rest_port "${index}")/management/v1/node/state")"
        version="$(curl -fsS "http://127.0.0.1:$(node_rest_port "${index}")/management/v1/node/version")"
        echo "node${index}: UP pid=${pid} state=${state} version=${version}"
    done

    if [[ "${failures}" -ne 0 ]]; then
        return 1
    fi

    local topology
    topology="$(curl -fsS "http://127.0.0.1:$(node_rest_port 1)/management/v1/cluster/topology/logical")"
    echo "logicalTopology=${topology}"
    JSON="${topology}" EXPECTED="${NODE_COUNT}" python3 -c 'import json,os,sys; data=json.loads(os.environ["JSON"]); sys.exit(0 if len(data)==int(os.environ["EXPECTED"]) else 1)'
}

case "${ACTION}" in
    start)
        prepare_runtime
        for index in $(seq 1 "${NODE_COUNT}"); do
            start_node "${index}"
        done
        sleep 2
        run_cli cluster init --url "http://127.0.0.1:$(node_rest_port 1)" --name "${CLUSTER_NAME}" \
            --cluster-management-group node1,node2,node3 \
            --metastorage-group node1,node2,node3
        wait_for_logical_topology "${NODE_COUNT}"
        show_status
        ;;
    start-existing)
        for index in $(seq 1 "${NODE_COUNT}"); do
            start_node "${index}"
        done
        wait_for_logical_topology "${NODE_COUNT}"
        show_status
        ;;
    status)
        show_status
        ;;
    stop)
        stop_all
        ;;
    stop-node)
        stop_node "${2:?node index is required}"
        ;;
    start-node)
        start_node "${2:?node index is required}"
        wait_for_logical_topology "${NODE_COUNT}"
        ;;
    start-nodes)
        shift
        start_nodes "$@"
        ;;
    restart-node)
        stop_node "${2:?node index is required}"
        start_node "$2"
        wait_for_logical_topology "${NODE_COUNT}"
        ;;
    *)
        echo "Usage: $0 start|start-existing|status|stop|stop-node N|start-node N|start-nodes N...|restart-node N" >&2
        exit 2
        ;;
esac
