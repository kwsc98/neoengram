#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${module_dir}/../lib/common.sh"

project_test_require_cmd node
project_test_require_cmd npm
project_test_require_cmd jq

openapi_npm() {
  (cd "${PROJECT_TEST_ROOT}/docs/openapi" && npm "$@")
}

main() {
  if [[ "${PROJECT_TEST_NO_INSTALL:-0}" != "1" ]]; then
    project_test_run_step openapi-install \
      openapi_npm ci --ignore-scripts || return $?
  else
    project_test_skip_step openapi-install '--no-install'
  fi

  project_test_run_step openapi-lint openapi_npm run lint || return $?
  project_test_run_step openapi-bundle openapi_npm run bundle || return $?
  project_test_run_step openapi-contract openapi_npm run test:contract || return $?
}

main "$@"
