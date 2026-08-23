#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd bash
project_test_require_cmd rg
project_test_require_cmd jq

main() {
  project_test_run_step agent-manifest-check \
    bash "${PROJECT_TEST_ROOT}/deploy/kubernetes/agent/check-manifests.sh" || return $?
  project_test_run_step gateway-manifest-check \
    bash "${PROJECT_TEST_ROOT}/deploy/kubernetes/gateway/check-manifests.sh"
}

main "$@"
