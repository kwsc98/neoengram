#!/usr/bin/env bash
set -Eeuo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib/common.sh"

project_test_write_enrollment_keyring() {
  local path="$1"
  local key
  key="$(printf 'project-test-enrollment-key-32-bytes!' | head -c 32 | base64 | tr '+/' '-_' | tr -d '=\n')"
  umask 077
  printf '{"version":1,"active_key_id":"project-test-key","keys":{"project-test-key":"%s"}}\n' "${key}" >"${path}"
  chmod 600 "${path}"
}

project_test_start_central() {
  local bind="$1" authority_dir="$2" keyring="$3" token="${4:-local-development-token}"
  local binary="${NEOENGRAM_CENTRAL_BIN:-${PROJECT_TEST_ROOT}/target/debug/neoengram-central}"
  [[ -x "${binary}" ]] || binary="$(command -v neoengram-central 2>/dev/null || true)"
  [[ -n "${binary}" && -x "${binary}" ]] || project_test_die "neoengram-central binary is not built"
  project_test_start_process central "${binary}" \
    --bind "${bind}" --authority-dir "${authority_dir}" \
    --development --development-token "${token}" --development-tenants project-test
}

project_test_start_gateway() {
  local name="$1" edge_cluster="$2" pool="$3" replica="$4" agent_port="$5" control_port="$6" peer_port="$7" central_url="$8"
  local binary="${NEOENGRAM_GATEWAY_BIN:-${PROJECT_TEST_ROOT}/target/debug/neoengram-gateway}"
  [[ -x "${binary}" ]] || binary="$(command -v neoengram-gateway 2>/dev/null || true)"
  [[ -n "${binary}" && -x "${binary}" ]] || project_test_die "neoengram-gateway binary is not built"
  project_test_start_process "${name}" "${binary}" \
    --edge-cluster-id "${edge_cluster}" --gateway-pool-id "${pool}" \
    --gateway-replica-id "${replica}" --agent-listen "127.0.0.1:${agent_port}" \
    --control-listen "127.0.0.1:${control_port}" --peer-listen "127.0.0.1:${peer_port}" \
    --central-upstream "${central_url}/" --log 'neoengram_gateway=warn'
}

project_test_create_volume_fixture() {
  local root="$1" volume_marker="$2"
  mkdir -p "${root}/objects" "${root}/playgrounds"
  printf '%s\n' "${volume_marker}" >"${root}/.neoengram-volume-marker"
  chmod 700 "${root}"
}

project_test_write_agent_config() {
  local path="$1" gateway_endpoint="$2" trust_bundle="$3" tenant_id="$4"
  local edge_cluster_id="$5" storage_volume_id="$6" region="$7" mount_path="$8"
  local state_dir="$9" token_id="${10}" bootstrap_token_file="${11}" pvc_claim="${12}"
  mkdir -p "$(dirname "${path}")" "${state_dir}" "$(dirname "${bootstrap_token_file}")"
  printf '%s\n' "project-test-bootstrap-${storage_volume_id}" >"${bootstrap_token_file}"
  chmod 600 "${bootstrap_token_file}"
  cat >"${path}" <<EOF
schema_version: 1
wire_version: 1
gateway_endpoint: ${gateway_endpoint}
trust_bundle_file: ${trust_bundle}
replication:
  enabled: false
tenant_id: ${tenant_id}
edge_cluster_id: ${edge_cluster_id}
storage_volume_id: ${storage_volume_id}
volume_descriptor_digest: 0000000000000000000000000000000000000000000000000000000000000000
region: ${region}
storage:
  backend_type: pvc
  access_mode: read_write_many
  mount_path: ${mount_path}
  state_dir: ${state_dir}
  marker_file: ${mount_path}/.neoengram-volume-marker
  expected_volume_marker: ${storage_volume_id}
  pvc_reference:
    namespace: project-test
    claim_name: ${pvc_claim}
registration:
  approval_required: true
  token_id: ${token_id}
  bootstrap_token_file: ${bootstrap_token_file}
session:
  heartbeat_interval_seconds: 10
  reconnect_max_delay_seconds: 30
logging:
  format: json
  level: info
EOF
  chmod 600 "${path}"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  project_test_require_base_tools
  project_test_write_enrollment_keyring "${PROJECT_TEST_TEMP_ROOT}/enrollment-keyring.json"
  project_test_create_volume_fixture "${PROJECT_TEST_TEMP_ROOT}/volume-source" volume-source
  project_test_create_volume_fixture "${PROJECT_TEST_TEMP_ROOT}/volume-target" volume-target
  project_test_log "local stack fixture prepared under ${PROJECT_TEST_TEMP_ROOT}"
fi
