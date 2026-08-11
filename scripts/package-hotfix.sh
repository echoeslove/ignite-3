#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <package-version>" >&2
    echo "Example: $0 3.1.0-hotfix1" >&2
    exit 1
fi

PACKAGE_VERSION="$1"
BASE_VERSION="$(awk -F'=' '/^projectVersion[[:space:]]*=/{gsub(/[[:space:]]/, "", $2); print $2; exit}' \
    "${REPO_ROOT}/gradle.properties")"

if [[ -z "${BASE_VERSION}" ]]; then
    echo "ERROR: projectVersion was not found in gradle.properties" >&2
    exit 1
fi

PACKAGE_VERSION_PATTERN="^${BASE_VERSION//./\.}-hotfix[1-9][0-9]*$"
if [[ ! "${PACKAGE_VERSION}" =~ ${PACKAGE_VERSION_PATTERN} ]]; then
    echo "ERROR: package version must match ${BASE_VERSION}-hotfix<N>, where N starts at 1" >&2
    exit 1
fi

SOURCE_ZIP="${REPO_ROOT}/packaging/build/distributions/ignite3-${BASE_VERSION}.zip"
OUTPUT_DIR="${REPO_ROOT}/build/hotfix-distributions"
TARGET_NAME="ignite3-${PACKAGE_VERSION}.zip"
TARGET_ZIP="${OUTPUT_DIR}/${TARGET_NAME}"
CHECKSUM_FILE="${TARGET_ZIP}.sha256"

if [[ -e "${TARGET_ZIP}" || -e "${CHECKSUM_FILE}" ]]; then
    echo "ERROR: Hotfix artifact already exists and will not be overwritten: ${TARGET_ZIP}" >&2
    exit 1
fi

cd "${REPO_ROOT}"

echo "[1/3] Building Ignite ${BASE_VERSION} distribution"
if [[ "${IGNITE_HOTFIX_USE_EXISTING_DIST:-false}" == "true" ]]; then
    echo "Using the existing distribution built by the preceding verified Gradle invocation"
else
    ./gradlew \
        :packaging:clean \
        :packaging-cli:clean \
        :packaging-db:clean \
        :packaging:allDistZip \
        -PprojectVersion="${BASE_VERSION}"
fi

if [[ ! -s "${SOURCE_ZIP}" ]]; then
    echo "ERROR: Expected distribution was not generated: ${SOURCE_ZIP}" >&2
    exit 1
fi

echo "[2/3] Verifying ZIP integrity and directory layout"
unzip -tq "${SOURCE_ZIP}"

for required_dir in "ignite3-db-${BASE_VERSION}/" "ignite3-cli-${BASE_VERSION}/"; do
    if ! unzip -Z1 "${SOURCE_ZIP}" | awk -v required="${required_dir}" \
        '$0 == required { found = 1 } END { exit !found }'; then
        echo "ERROR: Required directory is missing from ZIP: ${required_dir}" >&2
        exit 1
    fi
done

echo "[3/3] Publishing immutable Hotfix artifact"
mkdir -p "${OUTPUT_DIR}"
cp "${SOURCE_ZIP}" "${TARGET_ZIP}"

(
    cd "${OUTPUT_DIR}"
    shasum -a 256 "${TARGET_NAME}" > "${TARGET_NAME}.sha256"
)

echo "PACKAGE_RESULT=PASS"
echo "BASE_VERSION=${BASE_VERSION}"
echo "PACKAGE_VERSION=${PACKAGE_VERSION}"
echo "ARTIFACT=${TARGET_ZIP}"
echo "CHECKSUM=${CHECKSUM_FILE}"
