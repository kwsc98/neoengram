#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd node
project_test_require_cmd npm
project_test_require_cmd npx
project_test_require_cmd jq

web_npm() {
  (cd "${PROJECT_TEST_ROOT}/apps/neoengram-web" && npm "$@")
}

web_npx() {
  (cd "${PROJECT_TEST_ROOT}/apps/neoengram-web" && npx "$@")
}

main() {
  if [[ "${PROJECT_TEST_NO_INSTALL:-0}" != "1" ]]; then
    project_test_run_step web-e2e-install \
      web_npm ci || return $?
    # Install only the browser used by the existing desktop and mobile projects;
    # --with-deps is intentionally left to CI, where the runner is Linux.
    project_test_run_step web-e2e-browser \
      web_npx playwright install chromium || return $?
  else
    project_test_skip_step web-e2e-install '--no-install (browser installation skipped too)'
    project_test_skip_step web-e2e-browser '--no-install'
  fi

  mkdir -p "${PROJECT_TEST_TEMP_ROOT}/web-e2e"
  project_test_run_step web-e2e \
    web_npm run test:e2e -- \
      --output "${PROJECT_TEST_TEMP_ROOT}/web-e2e/test-results" \
      --reporter=list
}

main "$@"
