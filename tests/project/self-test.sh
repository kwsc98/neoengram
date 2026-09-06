#!/usr/bin/env bash
set -Eeuo pipefail

module_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "${module_dir}/lib/common.sh"

project_test_require_cmd bash
project_test_require_cmd jq
project_test_require_cmd mktemp
project_test_require_cmd python3

check_script_syntax() {
  local path
  local -a scripts=()
  while IFS= read -r path; do
    scripts+=("${path}")
  done < <(find "${PROJECT_TEST_ROOT}/scripts" "${PROJECT_TEST_ROOT}/tests/project" \
    -type f -name '*.sh' -print 2>/dev/null | sort)
  [[ "${#scripts[@]}" -gt 0 ]] || {
    project_test_log "no project test scripts found"
    return 1
  }
  bash -n "${scripts[@]}"
}

check_expected_modules() {
  local module
  for module in domain runtime agent central gateway dev-stack cli openapi web web-e2e manifests quality; do
    [[ -x "${PROJECT_TEST_ROOT}/tests/project/modules/${module}.sh" ]] || {
      project_test_log "module is missing or not executable: ${module}.sh"
      return 1
    }
  done
}

check_common_helpers() {
  local first_port second_port
  first_port="$(project_test_allocate_port)"
  second_port="$(project_test_allocate_port)"
  [[ "${first_port}" =~ ^[0-9]+$ && "${second_port}" =~ ^[0-9]+$ ]] || return 1
  [[ "${first_port}" -ge 1 && "${first_port}" -le 65535 ]] || return 1
  [[ "${second_port}" -ge 1 && "${second_port}" -le 65535 ]] || return 1
  python3 - "${first_port}" <<'PY'
import socket
import sys

port = int(sys.argv[1])
sock = socket.socket()
try:
    sock.bind(("127.0.0.1", port))
finally:
    sock.close()
PY

  local document='{"status":"ready","count":2}'
  project_test_assert_json "${document}" '.status == "ready" and .count == 2'
}

check_secret_redaction() {
  local redacted
  PROJECT_TEST_SECRET_VALUES='known-secret'
  redacted="$(project_test_redact_secrets 'Authorization: Bearer bearer-secret --development-token dev-secret --secret known-secret')"
  [[ "${redacted}" != *bearer-secret* ]] || return 1
  [[ "${redacted}" != *dev-secret* ]] || return 1
  [[ "${redacted}" != *known-secret* ]] || return 1
  [[ "${redacted}" == *'[REDACTED]'* ]] || return 1
  redacted="$(printf '%s\n' '-----BEGIN PRIVATE KEY-----' 'private-key-body' '-----END PRIVATE KEY-----' | project_test_redact_stream)"
  [[ "${redacted}" != *private-key-body* ]] || return 1
}

check_dispatch() {
  local output
  output="$(env \
    -u PROJECT_TEST_RUN_ID \
    -u PROJECT_TEST_REPORT_ROOT \
    -u PROJECT_TEST_LOG_ROOT \
    -u PROJECT_TEST_TEMP_ROOT \
    -u PROJECT_TEST_PROCESS_LOG_ROOT \
    -u PROJECT_TEST_INVOCATION \
    PROJECT_TEST_KEEP_TEMP=0 PROJECT_TEST_CHILD=0 PROJECT_TEST_NO_INSTALL=1 \
    bash "${PROJECT_TEST_ROOT}/scripts/project-test.sh" module domain 2>&1)" || {
    project_test_log "module dispatch failed"
    return 1
  }
  [[ "${output}" == *"PASS module-domain"* ]]
}

check_process_cleanup() {
  local pid
  PROJECT_TEST_PIDS=()
  project_test_start_process self-test-sleep sleep 60 >/dev/null
  pid="${PROJECT_TEST_PIDS[${#PROJECT_TEST_PIDS[@]}-1]}"
  project_test_stop_processes
  ! kill -0 "${pid}" 2>/dev/null
}

check_temp_cleanup() {
  local temp_root report_root common_path
  temp_root="$(mktemp -d "${TMPDIR:-/tmp}/neoengram-project-test.self.XXXXXX")"
  printf 'readonly\n' >"${temp_root}/readonly.txt"
  chmod 444 "${temp_root}/readonly.txt"
  report_root="${PROJECT_TEST_ROOT}/target/project-test/self-test-cleanup-$$"
  common_path="${PROJECT_TEST_ROOT}/tests/project/lib/common.sh"
  PROJECT_TEST_CHILD=0 \
  PROJECT_TEST_TEMP_ROOT="${temp_root}" \
  PROJECT_TEST_REPORT_ROOT="${report_root}" \
    PROJECT_TEST_LOG_ROOT="${report_root}/logs" \
    PROJECT_TEST_KEEP_TEMP=0 \
    PROJECT_TEST_COMMAND=self-test-cleanup \
    bash -c 'source "$1"; exit 0' _ "${common_path}" >/dev/null
  [[ ! -e "${temp_root}" ]]
}

check_report_shape() {
  local summary="${PROJECT_TEST_REPORT_ROOT}/summary.json"
  # Exercise the same finalizer used by standalone and aggregate runners. The
  # caller may still finalize again on EXIT with the actual process status.
  project_test_finalize_report 0
  [[ -f "${summary}" ]] || return 1
  jq -e 'has("run_id") and has("status") and has("steps")' "${summary}" >/dev/null
}

main() {
  project_test_run_step self-test-script-syntax check_script_syntax || return $?
  project_test_run_step self-test-module-layout check_expected_modules || return $?
  project_test_run_step self-test-common-helpers check_common_helpers || return $?
  project_test_run_step self-test-secret-redaction check_secret_redaction || return $?
  project_test_run_step self-test-dispatch check_dispatch || return $?
  project_test_run_step self-test-process-cleanup check_process_cleanup || return $?
  project_test_run_step self-test-temp-cleanup check_temp_cleanup || return $?
  # This step is intentionally last so all self-test records are included in
  # the report shape check.
  if [[ "${PROJECT_TEST_SELF_TEST_CHECK_REPORT:-0}" == "1" ]]; then
    project_test_run_step self-test-report-shape check_report_shape || return $?
  else
    project_test_log "SKIP self-test-report-shape (set PROJECT_TEST_SELF_TEST_CHECK_REPORT=1 in a completed runner)"
  fi
}

main "$@"
