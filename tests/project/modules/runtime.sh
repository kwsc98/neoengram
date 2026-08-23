#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd cargo
project_test_require_cmd jq

main() {
  local suite="${PROJECT_TEST_RUNTIME_SUITE:-all}"
  case "${suite}" in
    all)
      # This includes the object backend unit tests and prepare-add integration
      # target, along with every other runtime target.
      project_test_run_step runtime-all-targets \
        cargo test --locked -p neoengram-runtime --all-targets
      ;;
    object_backend)
      project_test_run_step runtime-object-backend \
        cargo test --locked -p neoengram-runtime --lib object_backend
      ;;
    prepare_add)
      project_test_run_step runtime-prepare-add \
        cargo test --locked -p neoengram-runtime --test prepare_add
      ;;
    *)
      project_test_die "unknown runtime suite '${suite}' (expected all, object_backend, or prepare_add)"
      ;;
  esac
}

main "$@"
