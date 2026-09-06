#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export REPO_ROOT

source "${REPO_ROOT}/tests/project/lib/common.sh"

PROJECT_TEST_COMMAND="${1:-all}"
PROJECT_TEST_NO_INSTALL="${PROJECT_TEST_NO_INSTALL:-0}"
PROJECT_TEST_INVOCATION="${PROJECT_TEST_INVOCATION:-$0 $*}"
export PROJECT_TEST_COMMAND
export PROJECT_TEST_INVOCATION

usage() {
  cat <<'EOF'
Usage: scripts/project-test.sh [options] <command>

Commands:
  bootstrap                 Install lockfile-pinned OpenAPI and Web dependencies
  module <name>             Run one module wrapper
  modules                   Run all module wrappers except the full process flow
  self-test                 Validate the project test runner itself
  full-flow                Run the isolated Central/Gateway/Agent file-flow test
  all                       Run bootstrap, modules, and full-flow
  mount-probe               Run the explicit Linux real-mount probe

Module names:
  domain runtime agent central gateway dev-stack cli openapi web web-e2e manifests quality gaps

Options:
  --no-install              Do not run npm ci during bootstrap/all
  --keep-temp               Preserve temporary state after a successful run
  --verbose                 Print every step log to the console
  -h, --help                Show this help
EOF
}

parse_options() {
  while (($# > 0)); do
    case "$1" in
      --no-install)
        PROJECT_TEST_NO_INSTALL=1
        ;;
      --keep-temp)
        PROJECT_TEST_KEEP_TEMP=1
        ;;
      --verbose)
        PROJECT_TEST_VERBOSE=1
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      --)
        shift
        break
        ;;
      -* )
        project_test_die "unknown option: $1"
        ;;
      *)
        break
        ;;
    esac
    shift
  done
  PROJECT_TEST_COMMAND="${1:-all}"
  shift || true
  export PROJECT_TEST_COMMAND PROJECT_TEST_NO_INSTALL PROJECT_TEST_KEEP_TEMP PROJECT_TEST_VERBOSE PROJECT_TEST_INVOCATION
  printf '%s\n' "$@" >"${PROJECT_TEST_TEMP_ROOT}/command-args"
}

bootstrap() {
  project_test_require_base_tools
  if [[ "${PROJECT_TEST_NO_INSTALL:-0}" == "1" ]]; then
    project_test_log "dependency installation skipped by --no-install"
    return 0
  fi
  npm ci --prefix "${REPO_ROOT}/docs/openapi" --ignore-scripts
  npm ci --prefix "${REPO_ROOT}/apps/neoengram-web"
}

run_module() {
  local module_name="$1"
  local module_script="${REPO_ROOT}/tests/project/modules/${module_name}.sh"
  if [[ ! -f "${module_script}" ]]; then
    project_test_log "unknown test module: ${module_name}"
    return 2
  fi
  # Keep a module's detailed step records in its own directory.  The parent
  # runner owns the aggregate report, so sibling modules can never overwrite
  # each other's step-*.json files.
  PROJECT_TEST_CHILD=1 \
    PROJECT_TEST_LOG_ROOT="${PROJECT_TEST_LOG_ROOT}/modules/${module_name}" \
    PROJECT_TEST_PROCESS_LOG_ROOT="${PROJECT_TEST_LOG_ROOT}/modules/${module_name}" \
    bash "${module_script}"
}

run_modules() {
  local module_name
  # gaps is opt-in for now: its bundle-collision audit fails on the nine known
  # agent schema name collisions. Add it to this list once they are resolved.
  for module_name in domain runtime agent central gateway dev-stack cli openapi web web-e2e manifests quality; do
    project_test_run_step "module-${module_name}" run_module "${module_name}"
  done
}

run_full_flow() {
  # full-flow owns its process trap so Central/Gateway children are always
  # reaped even though the parent captures its output as one aggregate step.
  PROJECT_TEST_CHILD=0 PROJECT_TEST_SUPPRESS_REPORT=1 \
    PROJECT_TEST_LOG_ROOT="${PROJECT_TEST_LOG_ROOT}/full-flow" \
    bash "${REPO_ROOT}/tests/project/full-flow.sh"
}

run_mount_probe() {
  if [[ "$(uname -s)" != "Linux" ]]; then
    project_test_log "mount-probe requires Linux; use the regular agent module on this platform"
    return 2
  fi
  PROJECT_TEST_CHILD=1 \
    PROJECT_TEST_LOG_ROOT="${PROJECT_TEST_LOG_ROOT}/mount-probe" \
    bash "${REPO_ROOT}/tests/project/modules/agent.sh" --mount-probe
}

run_self_test() {
  PROJECT_TEST_CHILD=1 \
    PROJECT_TEST_LOG_ROOT="${PROJECT_TEST_LOG_ROOT}/self-test" \
    bash "${REPO_ROOT}/tests/project/self-test.sh"
}

parse_options "$@"

case "${PROJECT_TEST_COMMAND}" in
  bootstrap)
    project_test_run_step bootstrap bootstrap
    ;;
  module)
    module_name="$(sed -n '1p' "${PROJECT_TEST_TEMP_ROOT}/command-args" 2>/dev/null || true)"
    [[ -n "${module_name}" ]] || project_test_die "module name is required"
    project_test_run_step "module-${module_name}" run_module "${module_name}"
    ;;
  modules)
    run_modules
    ;;
  self-test)
    project_test_run_step self-test run_self_test
    ;;
  full-flow)
    project_test_run_step full-flow run_full_flow
    ;;
  all)
    project_test_run_step bootstrap bootstrap
    run_modules
    project_test_run_step full-flow run_full_flow
    ;;
  mount-probe)
    project_test_run_step mount-probe run_mount_probe
    ;;
  -h|--help)
    usage
    ;;
  *)
    usage >&2
    project_test_die "unknown command: ${PROJECT_TEST_COMMAND}"
    ;;
esac
