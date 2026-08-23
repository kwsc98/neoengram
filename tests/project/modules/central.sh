#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd cargo
project_test_require_cmd jq

run_central_target() {
  local target="$1"
  project_test_run_step "central-${target}" \
    cargo test --locked -p neoengram-central --test "${target}"
}

main() {
  local suite="${PROJECT_TEST_CENTRAL_SUITE:-all}"
  local targets

  case "${suite}" in
    all)
      # All integration targets cover HTTP, authorization, lifecycle, placement,
      # replication, and workspace behavior in one Cargo invocation.
      project_test_run_step central-all-integration \
        cargo test --locked -p neoengram-central --tests
      return $?
      ;;
    http)
      targets=(http_server catalog_http security_acceptance)
      ;;
    permissions|permission)
      targets=(security_acceptance control_catalog catalog_http)
      ;;
    lifecycle)
      targets=(lifecycle_reports workspace_commit workspace_materialization playground_precommit)
      ;;
    placement)
      targets=(placement_service placement_replication)
      ;;
    replication)
      targets=(placement_replication)
      ;;
    workspace)
      targets=(workspace_commit workspace_materialization playground_precommit precommit_repository)
      ;;
    *)
      project_test_die "unknown Central suite '${suite}' (expected all, http, permissions, lifecycle, placement, replication, or workspace)"
      ;;
  esac

  local target
  for target in "${targets[@]}"; do
    run_central_target "${target}" || return $?
  done
}

main "$@"
