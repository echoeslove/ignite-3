#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VALIDATION_ROOT="${IGNITE_VALIDATION_ROOT:-${REPO_ROOT}/build/job-classloader-cache-standalone}"
RUNTIME_ROOT="${VALIDATION_ROOT}/runtime"
CLI_STATE_HOME="${VALIDATION_ROOT}/cli-home"
SQL_FILE="${VALIDATION_ROOT}/mock-data.sql"
SQL_LOG="${VALIDATION_ROOT}/logs/04-mock-data.log"
ROW_COUNT="${IGNITE_MOCK_ROW_COUNT:-1000}"

CLI_HOME="$(find "${RUNTIME_ROOT}/dist" -maxdepth 1 -type d -name 'ignite3-cli-*' | head -1)"
if [[ -z "${CLI_HOME}" || ! -x "${CLI_HOME}/bin/ignite3" ]]; then
    echo "ERROR: CLI distribution not found. Run 02-init-3node-cluster.sh start first." >&2
    exit 1
fi

mkdir -p "${VALIDATION_ROOT}/logs"

{
    echo 'DROP TABLE IF EXISTS CLASSLOADER_VALIDATION;'
    echo 'DROP ZONE IF EXISTS CLASSLOADER_VALIDATION_ZONE;'
    echo "CREATE ZONE CLASSLOADER_VALIDATION_ZONE WITH STORAGE_PROFILES='default', PARTITIONS=12, REPLICAS=3;"
    echo 'CREATE TABLE CLASSLOADER_VALIDATION (id INT PRIMARY KEY, payload VARCHAR(128), category INT) ZONE CLASSLOADER_VALIDATION_ZONE;'

    for start in $(seq 1 100 "${ROW_COUNT}"); do
        end=$((start + 99))
        if [[ "${end}" -gt "${ROW_COUNT}" ]]; then
            end="${ROW_COUNT}"
        fi

        printf 'INSERT INTO CLASSLOADER_VALIDATION (id, payload, category) VALUES '
        separator=''
        for id in $(seq "${start}" "${end}"); do
            printf "%s(%d, 'payload-%06d', %d)" "${separator}" "${id}" "${id}" "$((id % 10))"
            separator=','
        done
        echo ';'
    done

    echo 'SELECT COUNT(*) AS ROW_COUNT, SUM(id) AS ID_SUM, COUNT(DISTINCT category) AS CATEGORY_COUNT FROM CLASSLOADER_VALIDATION;'
} > "${SQL_FILE}"

HOME="${CLI_STATE_HOME}" IGNITE3_OPTS="-Duser.home=${CLI_STATE_HOME}" \
    "${CLI_HOME}/bin/ignite3" sql --jdbc-url jdbc:ignite:thin://127.0.0.1:10800 --file "${SQL_FILE}" \
    | tee "${SQL_LOG}"

EXPECTED_SUM=$((ROW_COUNT * (ROW_COUNT + 1) / 2))
VERIFY_OUTPUT="$(HOME="${CLI_STATE_HOME}" IGNITE3_OPTS="-Duser.home=${CLI_STATE_HOME}" \
    "${CLI_HOME}/bin/ignite3" sql --plain --jdbc-url jdbc:ignite:thin://127.0.0.1:10800 \
    'SELECT COUNT(*), SUM(id), COUNT(DISTINCT category) FROM CLASSLOADER_VALIDATION;')"

echo "${VERIFY_OUTPUT}" | tee -a "${SQL_LOG}"
read -r ACTUAL_COUNT ACTUAL_SUM ACTUAL_CATEGORIES <<< "$(echo "${VERIFY_OUTPUT}" | tail -1)"
if [[ "${ACTUAL_COUNT}" != "${ROW_COUNT}" || "${ACTUAL_SUM}" != "${EXPECTED_SUM}" || "${ACTUAL_CATEGORIES}" != "10" ]]; then
    echo "ERROR: Mock data verification failed: ${VERIFY_OUTPUT}" >&2
    exit 1
fi

echo "MOCK_DATA_RESULT=PASS rows=${ROW_COUNT} idSum=${EXPECTED_SUM} categories=10"
