#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd bash
project_test_require_cmd rg

run_dev_stack_checks() {
  local script="${PROJECT_TEST_ROOT}/scripts/dev-stack.sh"
  local volume_1="${PROJECT_TEST_TEMP_ROOT}/dev-stack-volume-1"
  local volume_2="${PROJECT_TEST_TEMP_ROOT}/dev-stack-volume-2"
  local volume_3="${PROJECT_TEST_TEMP_ROOT}/dev-stack-volume-3"
  local data_dir="${PROJECT_TEST_TEMP_ROOT}/dev-stack-data"
  local output

  [[ -x "${script}" ]] || project_test_die "dev-stack script is not executable"
  bash -n "${script}"
  output="$(bash "${script}" --dry-run \
    --gateways 3 --disk "${volume_1}" --disk "${volume_2}" --disk "${volume_3}" \
    --data-dir "${data_dir}" --central-port 19080 --base-port 19100)"
  printf '%s\n' "${output}" >"${PROJECT_TEST_REPORT_ROOT}/dev-stack-dry-run.txt"
  rg -F 'pool: pool-local (edge cluster edge-local, replicas 3)' <<<"${output}"
  rg -F 'gateway-2: agent=http://127.0.0.1:19103' <<<"${output}"
  rg -F "disk-1: gateway-1 -> ${volume_1} (Agent/Volume volume-local-1)" <<<"${output}"
  rg -F "disk-2: gateway-2 -> ${volume_2} (Agent/Volume volume-local-2)" <<<"${output}"
  rg -F "disk-3: gateway-3 -> ${volume_3} (Agent/Volume volume-local-3)" <<<"${output}"
  [[ ! -e "${volume_1}" && ! -e "${volume_2}" && ! -e "${volume_3}" && ! -e "${data_dir}" ]] || {
    project_test_die "dev-stack dry-run changed the filesystem"
  }

  output="$(bash "${script}" --dry-run --gateways 2 \
    --disk "${volume_1}" --disk "${volume_2}" --disk "${volume_3}" \
    --disk-gateway 2 --volume-id custom-a --volume-id custom-b --volume-id custom-c)"
  rg -F "disk-1: gateway-2 -> ${volume_1} (Agent/Volume custom-a)" <<<"${output}"
  rg -F "disk-2: gateway-2 -> ${volume_2} (Agent/Volume custom-b)" <<<"${output}"
  rg -F "disk-3: gateway-2 -> ${volume_3} (Agent/Volume custom-c)" <<<"${output}"

  if bash "${script}" --dry-run --gateways 2 --disk-gateway 3 >/dev/null 2>&1; then
    project_test_die "dev-stack accepted a disk gateway outside the pool"
  fi

  local saved_dir="${PROJECT_TEST_TEMP_ROOT}/dev-stack-saved"
  local saved_volume="${PROJECT_TEST_TEMP_ROOT}/volume with # marker"
  mkdir -p "${saved_dir}"
  printf '%s\n' \
    'topology_version=1' \
    'central_url=http://127.0.0.1:19280' \
    'central_port=19280' \
    'base_port=19300' \
    'pool_id=saved-pool' \
    'edge_cluster_id=saved-edge' \
    'tenant_id=saved-tenant' \
    'volume_id=saved-volume' \
    'region=saved-region' \
    'display_name=Saved local stack' \
    'gateways=4' \
    'disk_gateway=3' \
    "disk_path=${saved_volume}" \
    >"${saved_dir}/stack.info"
  output="$(bash "${script}" --dry-run --data-dir "${saved_dir}")"
  rg -F 'central: http://127.0.0.1:19280' <<<"${output}"
  rg -F 'pool: saved-pool (edge cluster saved-edge, replicas 4)' <<<"${output}"
  rg -F "disk: gateway-3 -> ${saved_volume} (Agent/Volume)" <<<"${output}"

  local saved_multi_dir="${PROJECT_TEST_TEMP_ROOT}/dev-stack-saved-multi"
  local saved_multi_1="${PROJECT_TEST_TEMP_ROOT}/saved multi volume 1"
  local saved_multi_2="${PROJECT_TEST_TEMP_ROOT}/saved multi volume 2"
  local saved_multi_3="${PROJECT_TEST_TEMP_ROOT}/saved multi volume 3"
  mkdir -p "${saved_multi_dir}"
  printf '%s\n' \
    'topology_version=2' \
    'central_url=http://127.0.0.1:19480' \
    'central_port=19480' \
    'base_port=19500' \
    'pool_id=saved-multi-pool' \
    'edge_cluster_id=saved-multi-edge' \
    'tenant_id=saved-multi-tenant' \
    'region=saved-multi-region' \
    'display_name=Saved multi stack' \
    'gateways=3' \
    'disk_count=3' \
    'disk_1_gateway=1' \
    "disk_1_path=${saved_multi_1}" \
    'disk_1_volume_id=saved-multi-1' \
    'disk_2_gateway=3' \
    "disk_2_path=${saved_multi_2}" \
    'disk_2_volume_id=saved-multi-2' \
    'disk_3_gateway=2' \
    "disk_3_path=${saved_multi_3}" \
    'disk_3_volume_id=saved-multi-3' \
    >"${saved_multi_dir}/stack.info"
  output="$(bash "${script}" --dry-run --data-dir "${saved_multi_dir}")"
  rg -F 'central: http://127.0.0.1:19480' <<<"${output}"
  rg -F 'pool: saved-multi-pool (edge cluster saved-multi-edge, replicas 3)' <<<"${output}"
  rg -F "disk-1: gateway-1 -> ${saved_multi_1} (Agent/Volume saved-multi-1)" <<<"${output}"
  rg -F "disk-2: gateway-3 -> ${saved_multi_2} (Agent/Volume saved-multi-2)" <<<"${output}"
  rg -F "disk-3: gateway-2 -> ${saved_multi_3} (Agent/Volume saved-multi-3)" <<<"${output}"
}

main() {
  project_test_run_step dev-stack-dry-run run_dev_stack_checks
}

main "$@"
