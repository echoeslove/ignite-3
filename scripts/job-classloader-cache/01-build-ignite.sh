#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VALIDATION_ROOT="${IGNITE_VALIDATION_ROOT:-${REPO_ROOT}/build/job-classloader-cache-standalone}"
ARTIFACT_DIR="${VALIDATION_ROOT}/artifacts"
LOG_DIR="${VALIDATION_ROOT}/logs"

mkdir -p "${ARTIFACT_DIR}" "${LOG_DIR}"

JAVA_VERSION_OUTPUT="$(java -version 2>&1)"
JAVA_MAJOR="$(java -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.version =/{split($2, parts, "."); print parts[1]; exit}')"

if [[ "${JAVA_MAJOR}" != "25" ]]; then
    echo "ERROR: JDK 25 is required, current java.version=${JAVA_MAJOR}" >&2
    exit 1
fi

cd "${REPO_ROOT}"

GRADLE_ARGS=(
    -Dorg.gradle.internal.instrumentation.agent=false
    --configure-on-demand
)

echo "[1/3] Running changed-area unit tests with JDK 25"
./gradlew "${GRADLE_ARGS[@]}" :ignite-compute:cleanTest :ignite-compute:test \
    --tests org.apache.ignite.internal.compute.loader.JobClassLoaderCacheTest \
    --tests org.apache.ignite.internal.compute.loader.JobContextManagerTest \
    --tests org.apache.ignite.internal.compute.executor.platform.dotnet.DotNetComputeExecutorTest \
    | tee "${LOG_DIR}/01-unit-tests.log"

echo "[2/3] Building compute unit and the official DB+CLI ZIP"
./gradlew "${GRADLE_ARGS[@]}" :ignite-compute:jobsJar :packaging:allDistZip \
    | tee "${LOG_DIR}/01-build.log"

PROJECT_VERSION="$(awk -F'=' '/^projectVersion[[:space:]]*=/{gsub(/[[:space:]]/, "", $2); print $2; exit}' gradle.properties)"
IGNITE_ZIP="${REPO_ROOT}/packaging/build/distributions/ignite3-${PROJECT_VERSION}.zip"
COMPUTE_UNIT_JAR="${REPO_ROOT}/modules/compute/build/libs/ignite-integration-test-jobs-1.0-SNAPSHOT.jar"

for artifact in "${IGNITE_ZIP}" "${COMPUTE_UNIT_JAR}"; do
    if [[ ! -s "${artifact}" ]]; then
        echo "ERROR: Expected artifact is missing or empty: ${artifact}" >&2
        exit 1
    fi
done

echo "[3/3] Verifying that the ZIP contains the repaired compute implementation"
rm -rf "${ARTIFACT_DIR}/inspect"
mkdir -p "${ARTIFACT_DIR}/inspect"
unzip -q "${IGNITE_ZIP}" 'ignite3-db-*/lib/ignite-compute-*.jar' -d "${ARTIFACT_DIR}/inspect"
PACKAGED_COMPUTE_JAR="$(find "${ARTIFACT_DIR}/inspect" -name 'ignite-compute-*.jar' -type f | head -1)"

if [[ -z "${PACKAGED_COMPUTE_JAR}" ]]; then
    echo "ERROR: Packaged ignite-compute JAR not found in ${IGNITE_ZIP}" >&2
    exit 1
fi

javap -classpath "${PACKAGED_COMPUTE_JAR}" -p org.apache.ignite.internal.compute.loader.JobClassLoaderCache \
    > "${ARTIFACT_DIR}/packaged-job-classloader-cache.javap"
grep -q 'updateMaxIdleEntries' "${ARTIFACT_DIR}/packaged-job-classloader-cache.javap"
grep -q 'updateIdleTimeoutMillis' "${ARTIFACT_DIR}/packaged-job-classloader-cache.javap"

ZIP_SHA256="$(shasum -a 256 "${IGNITE_ZIP}" | awk '{print $1}')"
UNIT_SHA256="$(shasum -a 256 "${COMPUTE_UNIT_JAR}" | awk '{print $1}')"
SOURCE_DIFF_SHA256="$(git diff HEAD --binary | shasum -a 256 | awk '{print $1}')"
GIT_HEAD="$(git rev-parse HEAD)"

{
    printf 'PROJECT_VERSION=%q\n' "${PROJECT_VERSION}"
    printf 'IGNITE_ZIP=%q\n' "${IGNITE_ZIP}"
    printf 'COMPUTE_UNIT_JAR=%q\n' "${COMPUTE_UNIT_JAR}"
    printf 'PACKAGED_COMPUTE_JAR=%q\n' "${PACKAGED_COMPUTE_JAR}"
    printf 'ZIP_SHA256=%q\n' "${ZIP_SHA256}"
    printf 'UNIT_SHA256=%q\n' "${UNIT_SHA256}"
    printf 'SOURCE_DIFF_SHA256=%q\n' "${SOURCE_DIFF_SHA256}"
    printf 'GIT_HEAD=%q\n' "${GIT_HEAD}"
    printf 'JAVA_MAJOR=%q\n' "${JAVA_MAJOR}"
} > "${ARTIFACT_DIR}/build-manifest.env"

printf '%s\n' "${JAVA_VERSION_OUTPUT}" > "${ARTIFACT_DIR}/java-version.txt"
git status --short > "${ARTIFACT_DIR}/git-status.txt"

echo "BUILD_RESULT=PASS"
echo "IGNITE_ZIP=${IGNITE_ZIP}"
echo "ZIP_SHA256=${ZIP_SHA256}"
echo "COMPUTE_UNIT_JAR=${COMPUTE_UNIT_JAR}"
echo "UNIT_SHA256=${UNIT_SHA256}"
