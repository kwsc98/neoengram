#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd node
project_test_require_cmd npm
project_test_require_cmd jq

web_npm() {
  (cd "${PROJECT_TEST_ROOT}/apps/neoengram-web" && npm "$@")
}

main() {
  if [[ "${PROJECT_TEST_NO_INSTALL:-0}" != "1" ]]; then
    project_test_run_step web-install \
      web_npm ci || return $?
  else
    project_test_skip_step web-install '--no-install'
  fi

  project_test_run_step web-format-check web_npm run format:check || return $?
  project_test_run_step web-lint web_npm run lint || return $?
  project_test_run_step web-typecheck web_npm run typecheck || return $?
  project_test_run_step web-api-check web_npm run api:check || return $?
  project_test_run_step web-unit-tests web_npm test || return $?
  project_test_run_step web-build web_npm run build || return $?
}

main "$@"
