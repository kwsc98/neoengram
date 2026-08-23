#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd cargo
project_test_require_cmd jq

run_domain_target() {
  local target="$1"
  project_test_run_step "domain-${target}" \
    cargo test --locked -p neoengram-domain --test "${target}"
}

main() {
  local suite="${PROJECT_TEST_DOMAIN_SUITE:-all}"
  local targets

  case "${suite}" in
    all)
      # Keep the four protocol contract targets explicit so a failure identifies
      # the affected contract rather than only reporting one aggregate command.
      targets=(envelope public_api schema_golden wire_golden)
      ;;
    envelope|public_api|schema_golden|wire_golden)
      targets=("${suite}")
      ;;
    *)
      project_test_die "unknown domain suite '${suite}' (expected all or a domain test target)"
      ;;
  esac

  local target
  for target in "${targets[@]}"; do
    run_domain_target "${target}" || return $?
  done
}

main "$@"
