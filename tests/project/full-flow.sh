#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_TEST_CHILD="${PROJECT_TEST_CHILD:-0}"
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fixtures/local-stack.sh"
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fixtures/certificates.sh"

project_test_require_base_tools
project_test_require_cmd openssl

CENTRAL_TOKEN="project-test-token"
export PROJECT_TEST_SECRET_VALUES="${CENTRAL_TOKEN}"
CENTRAL_PORT="$(project_test_allocate_port)"
CENTRAL_URL="http://127.0.0.1:${CENTRAL_PORT}"
SOURCE_AGENT_PORT="$(project_test_allocate_port)"
SOURCE_CONTROL_PORT="$(project_test_allocate_port)"
SOURCE_PEER_PORT="$(project_test_allocate_port)"
TARGET_AGENT_PORT="$(project_test_allocate_port)"
TARGET_CONTROL_PORT="$(project_test_allocate_port)"
TARGET_PEER_PORT="$(project_test_allocate_port)"

AUTHORITY_DIR="${PROJECT_TEST_TEMP_ROOT}/authority"
SOURCE_VOLUME="${PROJECT_TEST_TEMP_ROOT}/volume-source"
TARGET_VOLUME="${PROJECT_TEST_TEMP_ROOT}/volume-target"
SOURCE_STATE_DIR="${PROJECT_TEST_TEMP_ROOT}/agent-state-source"
TARGET_STATE_DIR="${PROJECT_TEST_TEMP_ROOT}/agent-state-target"
LOCAL_REPOSITORY="${PROJECT_TEST_TEMP_ROOT}/local-repository"
LOCAL_EXPORT="${PROJECT_TEST_TEMP_ROOT}/local-export"

mkdir -p "${AUTHORITY_DIR}" "${LOCAL_REPOSITORY}"
mkdir -p \
  "${PROJECT_TEST_REPORT_ROOT}/central" \
  "${PROJECT_TEST_REPORT_ROOT}/gateway-source" \
  "${PROJECT_TEST_REPORT_ROOT}/gateway-target" \
  "${PROJECT_TEST_REPORT_ROOT}/agent-source" \
  "${PROJECT_TEST_REPORT_ROOT}/agent-target"
export PROJECT_TEST_PROCESS_LOG_ROOT="${PROJECT_TEST_REPORT_ROOT}"
project_test_create_volume_fixture "${SOURCE_VOLUME}" volume-source
project_test_create_volume_fixture "${TARGET_VOLUME}" volume-target
project_test_write_enrollment_keyring "${PROJECT_TEST_TEMP_ROOT}/enrollment-keyring.json"

fixture_setup() {
  local certificate_dir="${PROJECT_TEST_TEMP_ROOT}/certificates"
  mkdir -p "${certificate_dir}"
  project_test_generate_ca "${certificate_dir}" gateway-bootstrap-ca
  project_test_generate_leaf "${certificate_dir}" gateway-source 'IP:127.0.0.1' gateway-bootstrap-ca
  project_test_generate_leaf "${certificate_dir}" gateway-target 'IP:127.0.0.1' gateway-bootstrap-ca
  project_test_generate_ca "${certificate_dir}" transfer-ca
  project_test_generate_leaf "${certificate_dir}" transfer-source 'IP:127.0.0.1' transfer-ca
  project_test_generate_leaf "${certificate_dir}" transfer-target 'IP:127.0.0.1' transfer-ca
  project_test_write_agent_config \
    "${PROJECT_TEST_REPORT_ROOT}/agent-source/agent.yaml" \
    "http://127.0.0.1:${SOURCE_AGENT_PORT}/" \
    "${certificate_dir}/gateway-bootstrap-ca.pem" project-test edge-project-test \
    volume-source local-source "${SOURCE_VOLUME}" "${SOURCE_STATE_DIR}" \
    project-test-source "${PROJECT_TEST_TEMP_ROOT}/bootstrap-source-token" source-pvc
  project_test_write_agent_config \
    "${PROJECT_TEST_REPORT_ROOT}/agent-target/agent.yaml" \
    "http://127.0.0.1:${TARGET_AGENT_PORT}/" \
    "${certificate_dir}/gateway-bootstrap-ca.pem" project-test edge-project-test \
    volume-target local-target "${TARGET_VOLUME}" "${TARGET_STATE_DIR}" \
    project-test-target "${PROJECT_TEST_TEMP_ROOT}/bootstrap-target-token" target-pvc
  printf '%s\n' \
    '{"status":"not-started","reason":"real Agent enrollment fixture is external; see real-agent-flow.json"}' \
    >"${PROJECT_TEST_REPORT_ROOT}/agent-source/config-summary.json"
  cp "${PROJECT_TEST_REPORT_ROOT}/agent-source/config-summary.json" \
    "${PROJECT_TEST_REPORT_ROOT}/agent-target/config-summary.json"
}
project_test_run_step fixture-setup fixture_setup

project_test_run_step build-binaries cargo build --locked \
  -p neoengram --bin neoengram \
  -p neoengram-central --bin neoengram-central \
  -p neoengram-gateway --bin neoengram-gateway \
  -p neoengram-agent --bin neoengram-agent

project_test_start_central "127.0.0.1:${CENTRAL_PORT}" "${AUTHORITY_DIR}" \
  "${PROJECT_TEST_TEMP_ROOT}/enrollment-keyring.json" "${CENTRAL_TOKEN}" >/dev/null
project_test_wait_http "${CENTRAL_URL}/health/live"
project_test_wait_http "${CENTRAL_URL}/health/ready"

project_test_start_gateway gateway-source edge-project-test pool-project-test replica-source \
  "${SOURCE_AGENT_PORT}" "${SOURCE_CONTROL_PORT}" "${SOURCE_PEER_PORT}" "${CENTRAL_URL}" >/dev/null
project_test_start_gateway gateway-target edge-project-test pool-project-test replica-target \
  "${TARGET_AGENT_PORT}" "${TARGET_CONTROL_PORT}" "${TARGET_PEER_PORT}" "${CENTRAL_URL}" >/dev/null
project_test_wait_http "http://127.0.0.1:${SOURCE_AGENT_PORT}/health/live"
project_test_wait_http "http://127.0.0.1:${TARGET_AGENT_PORT}/health/live"

central_api_smoke() {
  set -Eeuo pipefail
  version="$(project_test_api_post "${CENTRAL_URL}" /api/system/version/query "{}" "${CENTRAL_TOKEN}")"
  project_test_assert_json "${version}" ".api_version == 1" "Central API version contract failed"
  tenants="$(project_test_api_post "${CENTRAL_URL}" /api/tenant/list/query "{\"page_size\":10}" "${CENTRAL_TOKEN}")"
  project_test_assert_json "${tenants}" ".items | any(.tenant_id == \"project-test\")" "Development tenant was not seeded"
  project="$(project_test_api_post "${CENTRAL_URL}" /api/project/create "{\"tenant_id\":\"project-test\",\"project_id\":\"project-flow\",\"display_name\":\"Project test flow\",\"description\":null}" "${CENTRAL_TOKEN}")"
  project_test_assert_json "${project}" ".project.project_id == \"project-flow\"" "Project creation failed"
  artifact="$(project_test_api_post "${CENTRAL_URL}" /api/artifact/create "{\"tenant_id\":\"project-test\",\"project_id\":\"project-flow\",\"artifact_id\":\"artifact-flow\",\"display_name\":\"Replication flow\",\"description\":null,\"initialization\":{\"mode\":\"empty\"}}" "${CENTRAL_TOKEN}")"
  project_test_assert_json "${artifact}" ".artifact.artifact_id == \"artifact-flow\"" "Artifact creation failed"
}
project_test_run_step central-api-smoke central_api_smoke

local_commit_flow() {
  set -Eeuo pipefail
  cli="${PROJECT_TEST_ROOT}/target/debug/neoengram"
  mkdir -p "${LOCAL_REPOSITORY}/dataset/subdir"
  printf "project flow source\\n" >"${LOCAL_REPOSITORY}/dataset/source.txt"
  printf "second object\\n" >"${LOCAL_REPOSITORY}/dataset/subdir/second.txt"
  commit_output="$(
    set -Eeuo pipefail
    cd "${LOCAL_REPOSITORY}"
    "${cli}" init --chunking whole-file
    "${cli}" add dataset
    "${cli}" commit -m "project flow commit"
    "${cli}" fsck
  )"
  printf '%s\n' "${commit_output}" >"${PROJECT_TEST_REPORT_ROOT}/commit-cli-output.log"
  commit_id="$(sed -n 's/^Committed //p' <<<"${commit_output}" | head -1)"
  [[ "${commit_id}" =~ ^[0-9a-f]{64}$ ]] || {
    project_test_log "CLI did not return a canonical Commit ID"
    return 1
  }
  (cd "${LOCAL_REPOSITORY}" && "${cli}" export HEAD "${LOCAL_EXPORT}" --mode copy)
  cmp "${LOCAL_REPOSITORY}/dataset/source.txt" "${LOCAL_EXPORT}/dataset/source.txt"
  cmp "${LOCAL_REPOSITORY}/dataset/subdir/second.txt" "${LOCAL_EXPORT}/dataset/subdir/second.txt"
  project_test_sha256_file "${LOCAL_REPOSITORY}/dataset/source.txt" >"${PROJECT_TEST_REPORT_ROOT}/source-file-digest.txt"
  project_test_sha256_file "${LOCAL_EXPORT}/dataset/source.txt" >>"${PROJECT_TEST_REPORT_ROOT}/source-file-digest.txt"
  project_test_sha256_file "${LOCAL_REPOSITORY}/dataset/subdir/second.txt" >>"${PROJECT_TEST_REPORT_ROOT}/source-file-digest.txt"
  project_test_sha256_file "${LOCAL_EXPORT}/dataset/subdir/second.txt" >>"${PROJECT_TEST_REPORT_ROOT}/source-file-digest.txt"
  file_count="$(find "${LOCAL_EXPORT}" -type f | wc -l | tr -d ' ')"
  byte_count="$(find "${LOCAL_EXPORT}" -type f -exec wc -c {} + | awk 'END {print $1 + 0}')"
  jq -n --arg commit_id "${commit_id}" --argjson object_count "${file_count}" \
    --argjson bytes "${byte_count}" '{commit_id:$commit_id,object_count:$object_count,bytes:$bytes}' \
    >"${PROJECT_TEST_REPORT_ROOT}/commit-observation.json"
}
project_test_run_step local-commit-flow local_commit_flow

project_test_run_step placement-replication-contract cargo test --locked \
  -p neoengram-central --test placement_replication --test placement_service
project_test_run_step agent-replication-contract cargo test --locked \
  -p neoengram-agent --lib replication

cp -a "${LOCAL_EXPORT}/." "${SOURCE_VOLUME}/playgrounds/commit-source/"
mkdir -p "${TARGET_VOLUME}/playgrounds/commit-target"
cp -a "${SOURCE_VOLUME}/playgrounds/commit-source/." "${TARGET_VOLUME}/playgrounds/commit-target/"
volume_copy_integrity() {
  set -Eeuo pipefail
  local source_root="${SOURCE_VOLUME}/playgrounds/commit-source"
  local target_root="${TARGET_VOLUME}/playgrounds/commit-target"
  local source_manifest="${PROJECT_TEST_TEMP_ROOT}/source-digests.txt"
  local target_manifest="${PROJECT_TEST_TEMP_ROOT}/target-digests.txt"
  local relative path
  : >"${source_manifest}"
  : >"${target_manifest}"
  diff -ru "${source_root}" "${target_root}"
  while IFS= read -r -d '' path; do
    relative="${path#${source_root}/}"
    printf '%s  %s\n' "$(project_test_sha256_file "${path}")" "${relative}" >>"${source_manifest}"
  done < <(find "${source_root}" -type f -print0 | sort -z)
  while IFS= read -r -d '' path; do
    relative="${path#${target_root}/}"
    printf '%s  %s\n' "$(project_test_sha256_file "${path}")" "${relative}" >>"${target_manifest}"
  done < <(find "${target_root}" -type f -print0 | sort -z)
  source_digest="$(project_test_sha256_file "${source_manifest}")"
  target_digest="$(project_test_sha256_file "${target_manifest}")"
  [[ "${source_digest}" == "${target_digest}" ]]
  printf "{\"source_digest\":\"%s\",\"target_digest\":\"%s\",\"source_unchanged\":true}\n" "${source_digest}" "${target_digest}" >"${PROJECT_TEST_REPORT_ROOT}/volume-integrity.json"
}
project_test_run_step volume-copy-integrity volume_copy_integrity

run_real_agent_flow() {
  set +e
  bash -c "${PROJECT_TEST_REAL_FLOW_COMMAND}" 2>&1 | project_test_redact_stream
  local rc="${PIPESTATUS[0]}"
  set -e
  return "${rc}"
}

if [[ -n "${PROJECT_TEST_REAL_FLOW_COMMAND:-}" ]]; then
  project_test_run_step real-agent-flow run_real_agent_flow
else
  project_test_skip_step real-agent-flow 'PROJECT_TEST_REAL_FLOW_COMMAND is not configured; isolated contract flow completed'
  printf '%s\n' '{"status":"skipped","reason":"PROJECT_TEST_REAL_FLOW_COMMAND is not configured","source_volume":"isolated temporary volume","target_volume":"isolated temporary volume"}' >"${PROJECT_TEST_REPORT_ROOT}/real-agent-flow.json"
fi

project_test_log "isolated full-flow contract passed; process-level Agent transfer requires PROJECT_TEST_REAL_FLOW_COMMAND"
