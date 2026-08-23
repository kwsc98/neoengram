#!/usr/bin/env bash
set -Eeuo pipefail

if [[ "${PROJECT_TEST_COMMON_LOADED:-0}" == "1" ]]; then
  return 0
fi
PROJECT_TEST_COMMON_LOADED=1

PROJECT_TEST_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export PROJECT_TEST_ROOT

PROJECT_TEST_RUN_ID="${PROJECT_TEST_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
PROJECT_TEST_REPORT_ROOT="${PROJECT_TEST_REPORT_ROOT:-${PROJECT_TEST_ROOT}/target/project-test/${PROJECT_TEST_RUN_ID}}"
PROJECT_TEST_LOG_ROOT="${PROJECT_TEST_LOG_ROOT:-${PROJECT_TEST_REPORT_ROOT}/logs}"
if [[ -z "${PROJECT_TEST_TEMP_ROOT:-}" ]]; then
  PROJECT_TEST_TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/neoengram-project-test.XXXXXX")"
fi
mkdir -p "${PROJECT_TEST_TEMP_ROOT}"
# The CLI deliberately rejects paths whose ancestors are symlinks.  macOS
# commonly exposes TMPDIR through /var -> /private/var, so resolve the test
# root once before creating any volume or repository paths beneath it.
PROJECT_TEST_TEMP_ROOT="$(cd "${PROJECT_TEST_TEMP_ROOT}" && pwd -P)"
PROJECT_TEST_KEEP_TEMP="${PROJECT_TEST_KEEP_TEMP:-0}"
PROJECT_TEST_VERBOSE="${PROJECT_TEST_VERBOSE:-0}"
PROJECT_TEST_CHILD="${PROJECT_TEST_CHILD:-0}"
PROJECT_TEST_PROCESS_LOG_ROOT="${PROJECT_TEST_PROCESS_LOG_ROOT:-${PROJECT_TEST_LOG_ROOT}}"

export PROJECT_TEST_RUN_ID PROJECT_TEST_REPORT_ROOT PROJECT_TEST_LOG_ROOT
export PROJECT_TEST_TEMP_ROOT PROJECT_TEST_KEEP_TEMP PROJECT_TEST_VERBOSE
export PROJECT_TEST_PROCESS_LOG_ROOT

mkdir -p "${PROJECT_TEST_LOG_ROOT}"
mkdir -p \
  "${PROJECT_TEST_REPORT_ROOT}/central" \
  "${PROJECT_TEST_REPORT_ROOT}/gateway-source" \
  "${PROJECT_TEST_REPORT_ROOT}/gateway-target" \
  "${PROJECT_TEST_REPORT_ROOT}/agent-source" \
  "${PROJECT_TEST_REPORT_ROOT}/agent-target"

declare -a PROJECT_TEST_PIDS=()
PROJECT_TEST_STEP_INDEX=0
PROJECT_TEST_CLEANUP_FAILED=0

project_test_redact_secrets() {
  local value="$*"
  local secret

  # Redact values passed through the common development/authentication flags,
  # then redact any explicitly registered secret values.  Keep this helper
  # shell-only so it is also available to self-test and failure handlers.
  value="$(printf '%s' "${value}" | sed -E \
    -e 's/(Bearer[[:space:]]+)[^[:space:]]+/\1[REDACTED]/Ig' \
    -e 's/(--?(development-token|token|secret|password|private-key|key)[=[:space:]]+)[^[:space:]]+/\1[REDACTED]/Ig' \
    -e 's/([[:space:]](authorization|token|secret|password)[=:][[:space:]]*)[^[:space:],]+/\1[REDACTED]/Ig')"
  for secret in ${PROJECT_TEST_SECRET_VALUES:-}; do
    [[ -n "${secret}" ]] || continue
    value="${value//${secret}/[REDACTED]}"
  done
  printf '%s' "${value}"
}

project_test_redact_stream() {
  local line in_private_key=0
  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ "${line}" =~ -----BEGIN[[:space:]][^-]*PRIVATE[[:space:]]KEY----- ]]; then
      in_private_key=1
      printf '[REDACTED PRIVATE KEY]\n'
      continue
    fi
    if (( in_private_key )); then
      if [[ "${line}" =~ -----END[[:space:]][^-]*PRIVATE[[:space:]]KEY----- ]]; then
        in_private_key=0
      fi
      continue
    fi
    project_test_redact_secrets "${line}"
    printf '\n'
  done
}

project_test_redact_file() {
  local input="$1" output="$2"
  project_test_redact_stream <"${input}" >"${output}"
}

project_test_log() {
  local message
  message="$(project_test_redact_secrets "$*")"
  printf '[project-test] %s\n' "${message}"
}

project_test_die() {
  printf '[project-test] error: %s\n' "$*" >&2
  exit 2
}

project_test_require_cmd() {
  command -v "$1" >/dev/null 2>&1 || project_test_die "required command is missing: $1"
}

project_test_require_base_tools() {
  local command_name
  for command_name in bash cargo curl jq mktemp node npm python3; do
    project_test_require_cmd "${command_name}"
  done
}

project_test_sha256_file() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${path}" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${path}" | awk '{print $1}'
  else
    project_test_die "sha256sum or shasum is required"
  fi
}

project_test_record_step() {
  local name="$1" status="$2" started="$3" finished="$4" log_file="$5" detail="${6:-}" duration_ms="${7:-0}"
  jq -n \
    --arg name "${name}" \
    --arg status "${status}" \
    --arg started "${started}" \
    --arg finished "${finished}" \
    --arg log "${log_file}" \
    --arg detail "${detail}" \
    --argjson duration_ms "${duration_ms}" \
    '{name:$name,status:$status,started_at:$started,finished_at:$finished,duration_ms:$duration_ms,log:$log,detail:$detail,skip_reason:(if $status == "skipped" then $detail else null end)}' \
    > "${PROJECT_TEST_LOG_ROOT}/step-$(printf '%03d' "${PROJECT_TEST_STEP_INDEX}").json"
}

project_test_run_step() {
  local name="$1"
  shift
  PROJECT_TEST_STEP_INDEX=$((PROJECT_TEST_STEP_INDEX + 1))
  local log_file="${PROJECT_TEST_LOG_ROOT}/$(printf '%03d' "${PROJECT_TEST_STEP_INDEX}")-${name//[^A-Za-z0-9_.-]/_}.log"
  local raw_log="${log_file}.raw"
  local started finished status detail rc started_epoch finished_epoch duration_ms
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  started_epoch="$(date +%s)"
  project_test_log "START ${name}"
  if "$@" >"${raw_log}" 2>&1; then
    rc=0
    status=passed
    detail=""
  else
    rc=$?
    status=failed
    detail="exit ${rc}"
  fi
  project_test_redact_file "${raw_log}" "${log_file}"
  rm -f "${raw_log}"
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  finished_epoch="$(date +%s)"
  duration_ms=$(( (finished_epoch - started_epoch) * 1000 ))
  project_test_record_step "${name}" "${status}" "${started}" "${finished}" "${log_file}" "${detail}" "${duration_ms}"
  if [[ "${PROJECT_TEST_VERBOSE}" == "1" || "${rc}" != "0" ]]; then
    sed -n '1,240p' "${log_file}" || true
    if [[ "${rc}" != "0" ]]; then
      project_test_log "full log: ${log_file}"
    fi
  fi
  if [[ "${rc}" == "0" ]]; then
    project_test_log "PASS ${name}"
  else
    project_test_log "FAIL ${name}"
  fi
  return "${rc}"
}

project_test_skip_step() {
  local name="$1" reason="$2"
  PROJECT_TEST_STEP_INDEX=$((PROJECT_TEST_STEP_INDEX + 1))
  local log_file="${PROJECT_TEST_LOG_ROOT}/$(printf '%03d' "${PROJECT_TEST_STEP_INDEX}")-${name//[^A-Za-z0-9_.-]/_}.skip.log"
  local timestamp
  timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '[project-test] SKIP %s: %s\n' "${name}" "$(project_test_redact_secrets "${reason}")" >"${log_file}"
  project_test_record_step "${name}" skipped "${timestamp}" "${timestamp}" "${log_file}" "${reason}" 0
  project_test_log "SKIP ${name}: ${reason}"
}

project_test_register_pid() {
  PROJECT_TEST_PIDS+=("$1")
}

project_test_start_process() {
  local name="$1"
  shift
  local process_log_dir="${PROJECT_TEST_PROCESS_LOG_ROOT}/${name}"
  local log_file="${process_log_dir}/process.log"
  mkdir -p "${process_log_dir}"
  project_test_log "starting ${name}: $(project_test_redact_secrets "$*")"
  "$@" >"${log_file}" 2>&1 &
  local pid=$!
  project_test_register_pid "${pid}"
  printf '%s\n' "${pid}" > "${process_log_dir}/pid"
  sleep 0.1
  if ! kill -0 "${pid}" 2>/dev/null; then
    project_test_log "${name} exited during startup; log: ${log_file}"
    return 1
  fi
  printf '%s\n' "${pid}"
}

project_test_stop_processes() {
  local pid
  for pid in "${PROJECT_TEST_PIDS[@]:-}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
    fi
  done
  for pid in "${PROJECT_TEST_PIDS[@]:-}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      for _ in 1 2 3 4 5; do
        sleep 0.2
        kill -0 "${pid}" 2>/dev/null || break
      done
    fi
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill -KILL "${pid}" 2>/dev/null || PROJECT_TEST_CLEANUP_FAILED=1
    fi
    if [[ -n "${pid}" ]]; then
      wait "${pid}" 2>/dev/null || true
    fi
  done
}

project_test_redact_process_logs() {
  local path redacted
  while IFS= read -r -d '' path; do
    redacted="${path}.redacted"
    project_test_redact_file "${path}" "${redacted}" || {
      PROJECT_TEST_CLEANUP_FAILED=1
      continue
    }
    mv -f "${redacted}" "${path}" || PROJECT_TEST_CLEANUP_FAILED=1
  done < <(find "${PROJECT_TEST_PROCESS_LOG_ROOT}" -type f -name '*.log' -print0 2>/dev/null)
}

project_test_redact_raw_logs() {
  local path target
  while IFS= read -r -d '' path; do
    target="${path%.raw}"
    project_test_redact_file "${path}" "${target}" || {
      PROJECT_TEST_CLEANUP_FAILED=1
      continue
    }
    rm -f "${path}" || PROJECT_TEST_CLEANUP_FAILED=1
  done < <(find "${PROJECT_TEST_LOG_ROOT}" -type f -name '*.log.raw' -print0 2>/dev/null)
}

project_test_make_temp_removable() {
  local path
  while IFS= read -r -d '' path; do
    chmod u+w "${path}" 2>/dev/null || PROJECT_TEST_CLEANUP_FAILED=1
  done < <(find "${PROJECT_TEST_TEMP_ROOT}" -type f -print0 2>/dev/null)
  while IFS= read -r -d '' path; do
    chmod u+rwx "${path}" 2>/dev/null || PROJECT_TEST_CLEANUP_FAILED=1
  done < <(find "${PROJECT_TEST_TEMP_ROOT}" -type d -print0 2>/dev/null)
}

project_test_scrub_sensitive_temp() {
  local path
  # Failed runs preserve diagnostics, but do not retain generated credentials
  # or payload-bearing repositories in the temporary root.
  while IFS= read -r -d '' path; do
    rm -f "${path}" 2>/dev/null || PROJECT_TEST_CLEANUP_FAILED=1
  done < <(find "${PROJECT_TEST_TEMP_ROOT}" -type f \( \
    -name '*-key.pem' -o -name '*.key' -o -name '*bootstrap*token*' \
    -o -name 'enrollment-keyring.json' \) -print0 2>/dev/null)
  for path in \
    "${PROJECT_TEST_TEMP_ROOT}/local-repository" \
    "${PROJECT_TEST_TEMP_ROOT}/local-export" \
    "${PROJECT_TEST_TEMP_ROOT}/volume-source/objects" \
    "${PROJECT_TEST_TEMP_ROOT}/volume-source/playgrounds" \
    "${PROJECT_TEST_TEMP_ROOT}/volume-target/objects" \
    "${PROJECT_TEST_TEMP_ROOT}/volume-target/playgrounds" \
    "${PROJECT_TEST_TEMP_ROOT}/agent-state-source" \
    "${PROJECT_TEST_TEMP_ROOT}/agent-state-target"; do
    if [[ -e "${path}" ]]; then
      find "${path}" -mindepth 1 -exec rm -rf -- {} + 2>/dev/null || PROJECT_TEST_CLEANUP_FAILED=1
    fi
  done
}

project_test_wait_http() {
  local url="$1"
  local timeout_seconds="${2:-30}"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if curl --silent --show-error --fail --max-time 2 "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  project_test_log "HTTP endpoint did not become ready: ${url}"
  return 1
}

project_test_wait_file() {
  local path="$1"
  local timeout_seconds="${2:-30}"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    [[ -s "${path}" ]] && return 0
    sleep 0.25
  done
  project_test_log "file did not become ready: ${path}"
  return 1
}

project_test_wait_agent_health() {
  local binary="$1" state_dir="$2" mode="${3:-ready}" timeout_seconds="${4:-30}"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if "${binary}" health --state-dir "${state_dir}" --mode "${mode}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  project_test_log "Agent health did not become ${mode}: ${state_dir}"
  return 1
}

project_test_allocate_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

project_test_api_post() {
  local base_url="$1" path="$2" body="$3" token="${4:-local-development-token}"
  curl --silent --show-error --fail-with-body \
    -X POST "${base_url}${path}" \
    -H 'content-type: application/json' \
    -H 'NeoEngram-API-Version: 1' \
    -H "authorization: Bearer ${token}" \
    --data "${body}"
}

project_test_assert_json() {
  local document="$1" filter="$2" message="${3:-JSON assertion failed}"
  if ! jq -e "${filter}" >/dev/null <<<"${document}"; then
    project_test_log "${message}"
    project_test_log "document: ${document}"
    return 1
  fi
}

project_test_finalize_report() {
  local exit_status="$1"
  local steps_file="${PROJECT_TEST_REPORT_ROOT}/steps.json"
  local -a step_files=()
  while IFS= read -r -d '' step_file; do
    step_files+=("${step_file}")
  done < <(find "${PROJECT_TEST_LOG_ROOT}" -type f -name 'step-*.json' -print0 2>/dev/null | sort -z)
  if [[ "${#step_files[@]}" -gt 0 ]]; then
    jq -s '.' "${step_files[@]}" >"${steps_file}"
  else
    printf '[]\n' >"${steps_file}"
  fi
  jq -n \
    --arg run_id "${PROJECT_TEST_RUN_ID}" \
    --arg command "${PROJECT_TEST_COMMAND:-unknown}" \
    --arg invocation "${PROJECT_TEST_INVOCATION:-${PROJECT_TEST_COMMAND:-unknown}}" \
    --arg status "$([[ "${exit_status}" == "0" ]] && printf passed || printf failed)" \
    --arg started "${PROJECT_TEST_STARTED_AT:-}" \
    --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg temp_root "${PROJECT_TEST_TEMP_ROOT}" \
    --arg report_root "${PROJECT_TEST_REPORT_ROOT}" \
    --slurpfile steps "${steps_file}" \
    '{run_id:$run_id,command:$command,invocation:$invocation,status:$status,started_at:$started,finished_at:$finished,temp_root:$temp_root,report_root:$report_root,steps:$steps[0]}' \
    >"${PROJECT_TEST_REPORT_ROOT}/summary.json"
}

project_test_on_exit() {
  local exit_status=$?
  set +e
  project_test_stop_processes
  project_test_redact_process_logs
  project_test_redact_raw_logs
  if [[ "${PROJECT_TEST_SUPPRESS_REPORT:-0}" != "1" ]]; then
    project_test_finalize_report "${exit_status}"
  fi
  if [[ "${exit_status}" == "0" && "${PROJECT_TEST_CLEANUP_FAILED}" == "0" && "${PROJECT_TEST_KEEP_TEMP}" != "1" ]]; then
    project_test_make_temp_removable
    rm -rf "${PROJECT_TEST_TEMP_ROOT}" || PROJECT_TEST_CLEANUP_FAILED=1
  elif [[ "${exit_status}" != "0" ]]; then
    project_test_make_temp_removable
    project_test_scrub_sensitive_temp
  fi
  if [[ "${PROJECT_TEST_CLEANUP_FAILED}" != "0" ]]; then
    project_test_log "cleanup failed; preserving temporary state at ${PROJECT_TEST_TEMP_ROOT}"
    exit_status=1
  fi
  if [[ "${exit_status}" == "0" ]]; then
    project_test_log "report: ${PROJECT_TEST_REPORT_ROOT}/summary.json"
  else
    project_test_log "failure report: ${PROJECT_TEST_REPORT_ROOT}/summary.json"
    project_test_log "temporary state: ${PROJECT_TEST_TEMP_ROOT}"
  fi
  exit "${exit_status}"
}

PROJECT_TEST_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export PROJECT_TEST_STARTED_AT
if [[ "${PROJECT_TEST_CHILD}" != "1" ]]; then
  trap project_test_on_exit EXIT
fi
