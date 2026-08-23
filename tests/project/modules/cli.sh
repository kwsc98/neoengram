#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd cargo
project_test_require_cmd jq

main() {
  local suite="${PROJECT_TEST_CLI_SUITE:-all}"
  case "${suite}" in
    all)
      project_test_run_step cli-all-targets \
        cargo test --locked -p neoengram --all-targets
      ;;
    workflow|concurrency|recovery|integrity)
      project_test_run_step "cli-${suite}" \
        cargo test --locked -p neoengram --test "${suite}"
      ;;
    workspace)
      project_test_run_step cli-workspace \
        cargo test --locked -p neoengram --test workspaces
      ;;
    *)
      project_test_die "unknown CLI suite '${suite}' (expected all, workflow, concurrency, recovery, integrity, or workspace)"
      ;;
  esac
}

main "$@"
