#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd cargo
project_test_require_cmd jq

main() {
  local suite="${PROJECT_TEST_GATEWAY_SUITE:-all}"
  case "${suite}" in
    all|network|network_e2e)
      project_test_run_step gateway-network-e2e \
        cargo test --locked -p neoengram-gateway --test network_e2e
      ;;
    *)
      project_test_die "unknown Gateway suite '${suite}' (expected all or network_e2e)"
      ;;
  esac
}

main "$@"
