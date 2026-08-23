#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd cargo
project_test_require_cmd jq

run_agent_target() {
  local target="$1"
  project_test_run_step "agent-${target}" \
    cargo test --locked -p neoengram-agent --test "${target}"
}

main() {
  local suite="${PROJECT_TEST_AGENT_SUITE:-all}"
  # The dispatcher uses a positional flag for the explicitly privileged mount
  # probe. Keep the environment override useful for focused local runs while
  # accepting that public command form as well.
  case "${1:-}" in
    --mount-probe|mount-probe)
      suite=mount-probe
      ;;
    "")
      ;;
    *)
      project_test_die "unknown agent option '$1'"
      ;;
  esac
  local targets

  case "${suite}" in
    all)
      # Ignored real-mount tests are intentionally excluded by the normal Cargo
      # invocation. They are available through the explicit mount-probe suite.
      project_test_run_step agent-all-targets \
        cargo test --locked -p neoengram-agent --all-targets
      ;;
    state|state_machine)
      targets=(state_machine)
      ;;
    persistence|persistent_adapters)
      targets=(persistent_adapters)
      ;;
    central|central_managed|central_managed_add)
      targets=(central_managed_add)
      ;;
    mount-probe)
      if [[ "$(uname -s)" != "Linux" ]]; then
        project_test_die "mount-probe requires Linux"
      fi
      [[ -n "${NEOENGRAM_REAL_MOUNT_PROBE_ROOT:-}" ]] || \
        project_test_die "mount-probe requires NEOENGRAM_REAL_MOUNT_PROBE_ROOT"
      project_test_run_step agent-real-mount-probe \
        env NEOENGRAM_REAL_MOUNT_PROBE_ROOT="${NEOENGRAM_REAL_MOUNT_PROBE_ROOT}" \
        cargo test --locked -p neoengram-agent \
          --test linux_mount_probe detects_real_mount_boundary_and_read_write_capabilities \
          -- --ignored --exact
      ;;
    *)
      project_test_die "unknown agent suite '${suite}' (expected all, state_machine, persistent_adapters, central_managed_add, or mount-probe)"
      ;;
  esac

  if [[ "${suite}" != "all" && "${suite}" != "mount-probe" ]]; then
    local target
    for target in "${targets[@]}"; do
      run_agent_target "${target}" || return $?
    done
  fi
}

main "$@"
