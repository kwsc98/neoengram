#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd bash
project_test_require_cmd cargo
project_test_require_cmd jq
project_test_require_cmd rg

run_script_syntax_checks() {
  local path
  local -a scripts=()
  while IFS= read -r path; do
    scripts+=("${path}")
  done < <(
    find "${PROJECT_TEST_ROOT}/scripts" "${PROJECT_TEST_ROOT}/tests/project" \
      -type f -name '*.sh' -print 2>/dev/null | sort
  )
  # Also cover the repository's policy scripts, which are part of the CI surface.
  scripts+=(
    "${PROJECT_TEST_ROOT}/.github/check-architecture.sh"
    "${PROJECT_TEST_ROOT}/deploy/kubernetes/agent/check-manifests.sh"
    "${PROJECT_TEST_ROOT}/deploy/kubernetes/gateway/check-manifests.sh"
  )
  bash -n "${scripts[@]}"
}

main() {
  project_test_run_step rustfmt cargo fmt --all -- --check || return $?
  project_test_run_step architecture \
    bash "${PROJECT_TEST_ROOT}/.github/check-architecture.sh" || return $?
  project_test_run_step clippy \
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings || return $?
  project_test_run_step rustdoc \
    env RUSTDOCFLAGS='-D warnings' \
    cargo doc --workspace --all-features --no-deps --locked || return $?
  project_test_run_step shell-syntax run_script_syntax_checks
}

main "$@"
