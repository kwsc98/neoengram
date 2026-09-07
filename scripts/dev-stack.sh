#!/usr/bin/env bash
set -Eeuo pipefail

# Local-only process orchestrator. A Gateway never owns the disk: each --disk starts one
# volume-scoped Agent behind a selected Gateway and uses its development directory probe.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"

ACTION=start
GATEWAYS=1
DISK_PATHS=()
DISK_GATEWAYS=()
DATA_DIR="${REPO_ROOT}/target/dev-stack"
CENTRAL_PORT=8080
BASE_PORT=18080
CENTRAL_TOKEN=local-development-token
TENANT_ID=tenant-local
EDGE_CLUSTER_ID=edge-local
POOL_ID=pool-local
VOLUME_IDS=()
DEFAULT_VOLUME_ID=volume-local
REGION=local
DISPLAY_NAME="Local development GatewayPool"
WAIT_SECONDS=60
AUTO_APPROVE=0
NO_BUILD=0
REBUILD=0
DRY_RUN=0
STARTUP_IN_PROGRESS=0
GATEWAY_SOFTWARE_VERSION=
GATEWAY_CAPABILITIES_JSON='["agent-control-v1","commit_materialization_v2","peer-forward-v1","route-lease-v1"]'
WORKLOAD_TRUST_DOMAIN=development.neoengram.local
COMMAND_TRUST_BUNDLE=
TRANSFER_CA_CERT=
TRANSFER_PLANE_CONFIGURED=0
AGENT_IDS=()

# Track topology options so a stopped custom stack can restore the values saved in stack.info
# without preventing an explicit override for a deliberate new data directory.
GATEWAYS_EXPLICIT=0
DISKS_EXPLICIT=0
DISK_GATEWAYS_EXPLICIT=0
CENTRAL_PORT_EXPLICIT=0
BASE_PORT_EXPLICIT=0
TENANT_ID_EXPLICIT=0
EDGE_CLUSTER_ID_EXPLICIT=0
POOL_ID_EXPLICIT=0
VOLUME_IDS_EXPLICIT=0
REGION_EXPLICIT=0
DISPLAY_NAME_EXPLICIT=0

saved_topology_value() {
  local info="$1" wanted_key="$2" key value
  [[ -s "$info" ]] || return 1
  while IFS='=' read -r key value || [[ -n "$key" ]]; do
    [[ "$key" == "$wanted_key" ]] || continue
    printf '%s\n' "$value"
    return 0
  done <"$info"
  return 1
}

disk_count() {
  printf '%s\n' "${#DISK_PATHS[@]}"
}

disk_path_for() {
  local index="$1"
  printf '%s\n' "${DISK_PATHS[$((index - 1))]}"
}

disk_gateway_for() {
  local index="$1" count="${#DISK_PATHS[@]}" gateway_count="${#DISK_GATEWAYS[@]}"
  (( count > 0 )) || die "internal error: no disks configured"
  case "$gateway_count" in
    0) printf '%s\n' $(( ((index - 1) % GATEWAYS) + 1 )) ;;
    1) printf '%s\n' "${DISK_GATEWAYS[0]}" ;;
    *) printf '%s\n' "${DISK_GATEWAYS[$((index - 1))]}" ;;
  esac
}

volume_id_for() {
  local index="$1" count="${#DISK_PATHS[@]}" volume_count="${#VOLUME_IDS[@]}"
  (( count > 0 )) || die "internal error: no disks configured"
  if (( volume_count == 0 )); then
    if (( count == 1 )); then
      printf '%s\n' "$DEFAULT_VOLUME_ID"
    else
      printf '%s-%s\n' "$DEFAULT_VOLUME_ID" "$index"
    fi
  elif (( volume_count == 1 && count == 1 )); then
    printf '%s\n' "${VOLUME_IDS[0]}"
  else
    printf '%s\n' "${VOLUME_IDS[$((index - 1))]}"
  fi
}

usage() {
  cat <<'EOF'
Usage: scripts/dev-stack.sh [start] [options]
       scripts/dev-stack.sh stop [--data-dir PATH]
       scripts/dev-stack.sh status [--data-dir PATH]

Starts a loopback-only Central and a configurable number of Gateway processes. Each repeated
--disk starts one volume-scoped Agent; the Gateway itself never mounts or stores the disk.

Options:
  --gateways N             Number of Gateways in the local pool (default: 1)
  --disk PATH              Local directory; repeat for multiple independent Volumes
  --disk-gateway N         1-based Gateway for one/all --disk entries; repeat positionally
                           (default: round-robin across Gateways; one value applies to all)
  --data-dir PATH          Persistent process/config/log directory (default: target/dev-stack)
  --central-port PORT      Central HTTP port (default: 8080)
  --base-port PORT         First Gateway Agent port; local data-plane ports are allocated after the Gateway ports (default: 18080)
  --central-token TOKEN    Development Bearer token (default: local-development-token)
  --tenant ID              Development tenant (default: tenant-local)
  --edge-cluster ID        EdgeCluster ID (default: edge-local)
  --pool-id ID             GatewayPool ID (default: pool-local)
  --volume-id ID           StorageVolume ID; repeat positionally with --disk
                           (default: volume-local for one disk, volume-local-1..N otherwise)
  --region NAME            Region applied to all local Volumes (default: local)
  --display-name TEXT      GatewayPool display name
  --auto-approve           Approve all development Volume enrollments automatically
  --wait SECONDS           Startup/enrollment wait timeout (default: 60)
  --no-build               Require target/debug binaries instead of building them
  --rebuild                Rebuild the local Central/Gateway/Agent binaries before starting
  --dry-run                Print the topology without creating files or starting processes
  -h, --help               Show this help

Examples:
  bash scripts/dev-stack.sh --gateways 3
  bash scripts/dev-stack.sh --gateways 3 --disk "$PWD/dev/volume" --disk-gateway 2
  bash scripts/dev-stack.sh --gateways 3 --disk "$PWD/dev/volume-a" --disk "$PWD/dev/volume-b" --disk "$PWD/dev/volume-c" --auto-approve
  bash scripts/dev-stack.sh --gateways 2 --disk "$PWD/dev/a" --disk "$PWD/dev/b" --disk "$PWD/dev/c" --disk-gateway 2
  bash scripts/dev-stack.sh --auto-approve --disk "$PWD/dev/volume"
  bash scripts/dev-stack.sh stop
  bash scripts/dev-stack.sh status

This is a loopback development helper. It does not provide production TLS, HA, external workload
credentials, real PVC fencing, or cross-node transfer guarantees. Agent enrollment still uses the
normal Central approval state machine unless --auto-approve is explicitly supplied.
When a stopped stack is started again with the same --data-dir, the saved topology is restored;
pass --central-token again if the original stack used a non-default token. Use --rebuild after
changing local Rust sources when the existing target/debug binaries must be refreshed.
EOF
}

die() {
  printf 'dev-stack: %s\n' "$*"
  exit 2
}

log() {
  printf '[dev-stack] %s\n' "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

valid_uint() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

validate_port() {
  local name="$1" value="$2"
  valid_uint "$value" || die "${name} must be an integer between 1 and 65535"
  (( value >= 1 && value <= 65535 )) || die "${name} must be an integer between 1 and 65535"
}

validate_id() {
  local name="$1" value="$2"
  [[ "$value" =~ ^[a-z0-9][a-z0-9-]{0,62}$ ]] || die "${name} must match [a-z0-9][a-z0-9-]{0,62}"
}

validate_absolute_path() {
  local name="$1" value="$2"
  [[ "$value" == /* ]] || die "${name} must be an absolute path"
  [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] || die "${name} must not contain newlines"
  [[ "$value" != "/" ]] || die "${name} must not be the filesystem root"
  [[ "$value" != *"/../"* && "$value" != */.. && "$value" != *"/./"* && "$value" != */. ]] || {
    die "${name} must not contain . or .. path components"
  }
}

normalize_directory() {
  local name="$1" path="$2" create="$3"
  validate_absolute_path "$name" "$path"
  if [[ ! -e "$path" ]]; then
    [[ "$create" == 1 ]] || die "${name} does not exist: ${path}"
    mkdir -p "$path"
  fi
  [[ -d "$path" ]] || die "${name} is not a directory: ${path}"
  [[ ! -L "$path" ]] || die "${name} must be an ordinary directory, not a symlink: ${path}"
  (cd "$path" && pwd -P)
}

parse_options() {
  if (($# > 0)); then
    case "$1" in
      start|stop|status) ACTION="$1"; shift ;;
    esac
  fi
  while (($# > 0)); do
    case "$1" in
      --gateways) (($# >= 2)) || die "--gateways requires a value"; GATEWAYS="$2"; GATEWAYS_EXPLICIT=1; shift 2 ;;
      --disk) (($# >= 2)) || die "--disk requires a value"; DISK_PATHS+=("$2"); DISKS_EXPLICIT=1; shift 2 ;;
      --disk-gateway) (($# >= 2)) || die "--disk-gateway requires a value"; DISK_GATEWAYS+=("$2"); DISK_GATEWAYS_EXPLICIT=1; shift 2 ;;
      --data-dir) (($# >= 2)) || die "--data-dir requires a value"; DATA_DIR="$2"; shift 2 ;;
      --central-port) (($# >= 2)) || die "--central-port requires a value"; CENTRAL_PORT="$2"; CENTRAL_PORT_EXPLICIT=1; shift 2 ;;
      --base-port) (($# >= 2)) || die "--base-port requires a value"; BASE_PORT="$2"; BASE_PORT_EXPLICIT=1; shift 2 ;;
      --central-token) (($# >= 2)) || die "--central-token requires a value"; CENTRAL_TOKEN="$2"; shift 2 ;;
      --tenant) (($# >= 2)) || die "--tenant requires a value"; TENANT_ID="$2"; TENANT_ID_EXPLICIT=1; shift 2 ;;
      --edge-cluster) (($# >= 2)) || die "--edge-cluster requires a value"; EDGE_CLUSTER_ID="$2"; EDGE_CLUSTER_ID_EXPLICIT=1; shift 2 ;;
      --pool-id) (($# >= 2)) || die "--pool-id requires a value"; POOL_ID="$2"; POOL_ID_EXPLICIT=1; shift 2 ;;
      --volume-id) (($# >= 2)) || die "--volume-id requires a value"; VOLUME_IDS+=("$2"); VOLUME_IDS_EXPLICIT=1; shift 2 ;;
      --region) (($# >= 2)) || die "--region requires a value"; REGION="$2"; REGION_EXPLICIT=1; shift 2 ;;
      --display-name) (($# >= 2)) || die "--display-name requires a value"; DISPLAY_NAME="$2"; DISPLAY_NAME_EXPLICIT=1; shift 2 ;;
      --auto-approve) AUTO_APPROVE=1; shift ;;
      --wait) (($# >= 2)) || die "--wait requires a value"; WAIT_SECONDS="$2"; shift 2 ;;
      --no-build) NO_BUILD=1; shift ;;
      --rebuild) REBUILD=1; shift ;;
      --dry-run) DRY_RUN=1; shift ;;
      -h|--help) usage; exit 0 ;;
      --) shift; (($# == 0)) || die "unexpected arguments: $*";;
      *) die "unknown argument: $1" ;;
    esac
  done
}

load_saved_topology() {
  local info="${DATA_DIR}/stack.info" value central_url gateway_url topology_version
  local saved_disk_count index saved_path saved_gateway saved_volume
  [[ "$ACTION" == start && -s "$info" ]] || return 0

  topology_version="$(saved_topology_value "$info" topology_version || true)"
  [[ -z "$topology_version" || "$topology_version" == 1 || "$topology_version" == 2 ]] || die "unsupported stack.info topology version: ${topology_version}"

  if (( GATEWAYS_EXPLICIT == 0 )); then
    value="$(saved_topology_value "$info" gateways || true)"
    [[ -z "$value" ]] || GATEWAYS="$value"
  fi
  saved_disk_count="$(saved_topology_value "$info" disk_count || true)"
  if (( DISKS_EXPLICIT == 0 )); then
    DISK_PATHS=()
    if [[ "$saved_disk_count" =~ ^[0-9]+$ ]] && (( saved_disk_count > 0 )); then
      for ((index = 1; index <= saved_disk_count; index++)); do
        saved_path="$(saved_topology_value "$info" "disk_${index}_path" || true)"
        [[ -z "$saved_path" ]] || DISK_PATHS+=("$saved_path")
      done
    else
      saved_path="$(saved_topology_value "$info" disk_path || true)"
      [[ -z "$saved_path" ]] || DISK_PATHS+=("$saved_path")
    fi
  fi
  if (( DISK_GATEWAYS_EXPLICIT == 0 )); then
    DISK_GATEWAYS=()
    if (( DISKS_EXPLICIT == 0 )) && [[ "$saved_disk_count" =~ ^[0-9]+$ ]] && (( saved_disk_count > 0 )); then
      for ((index = 1; index <= saved_disk_count; index++)); do
        saved_gateway="$(saved_topology_value "$info" "disk_${index}_gateway" || true)"
        [[ -z "$saved_gateway" ]] || DISK_GATEWAYS+=("$saved_gateway")
      done
    elif (( DISKS_EXPLICIT == 0 )); then
      saved_gateway="$(saved_topology_value "$info" disk_gateway || true)"
      [[ -z "$saved_gateway" ]] || DISK_GATEWAYS+=("$saved_gateway")
    fi
  fi
  if (( CENTRAL_PORT_EXPLICIT == 0 )); then
    value="$(saved_topology_value "$info" central_port || true)"
    if [[ -z "$value" ]]; then
      central_url="$(saved_topology_value "$info" central_url || true)"
      [[ "$central_url" =~ :([0-9]+)$ ]] && value="${BASH_REMATCH[1]}"
    fi
    [[ -z "$value" ]] || CENTRAL_PORT="$value"
  fi
  if (( BASE_PORT_EXPLICIT == 0 )); then
    value="$(saved_topology_value "$info" base_port || true)"
    if [[ -z "$value" ]]; then
      gateway_url="$(saved_topology_value "$info" gateway_1_agent_url || true)"
      [[ "$gateway_url" =~ :([0-9]+)$ ]] && value="${BASH_REMATCH[1]}"
    fi
    [[ -z "$value" ]] || BASE_PORT="$value"
  fi
  if (( TENANT_ID_EXPLICIT == 0 )); then
    value="$(saved_topology_value "$info" tenant_id || true)"
    [[ -z "$value" ]] || TENANT_ID="$value"
  fi
  if (( EDGE_CLUSTER_ID_EXPLICIT == 0 )); then
    value="$(saved_topology_value "$info" edge_cluster_id || true)"
    [[ -z "$value" ]] || EDGE_CLUSTER_ID="$value"
  fi
  if (( POOL_ID_EXPLICIT == 0 )); then
    value="$(saved_topology_value "$info" pool_id || true)"
    [[ -z "$value" ]] || POOL_ID="$value"
  fi
  if (( VOLUME_IDS_EXPLICIT == 0 )); then
    VOLUME_IDS=()
    if (( DISKS_EXPLICIT == 0 )) && [[ "$saved_disk_count" =~ ^[0-9]+$ ]] && (( saved_disk_count > 0 )); then
      for ((index = 1; index <= saved_disk_count; index++)); do
        saved_volume="$(saved_topology_value "$info" "disk_${index}_volume_id" || true)"
        [[ -z "$saved_volume" ]] || VOLUME_IDS+=("$saved_volume")
      done
    elif (( DISKS_EXPLICIT == 0 )); then
      value="$(saved_topology_value "$info" volume_id || true)"
      [[ -z "$value" ]] || VOLUME_IDS+=("$value")
    fi
  fi
  if (( REGION_EXPLICIT == 0 )); then
    value="$(saved_topology_value "$info" region || true)"
    [[ -z "$value" ]] || REGION="$value"
  fi
  if (( DISPLAY_NAME_EXPLICIT == 0 )); then
    value="$(saved_topology_value "$info" display_name || true)"
    [[ -z "$value" ]] || DISPLAY_NAME="$value"
  fi
}

validate_options() {
  valid_uint "$GATEWAYS" || die "--gateways must be an integer between 1 and 32"
  (( GATEWAYS >= 1 && GATEWAYS <= 32 )) || die "--gateways must be an integer between 1 and 32"
  local disk_total="${#DISK_PATHS[@]}" gateway_total="${#DISK_GATEWAYS[@]}" volume_total="${#VOLUME_IDS[@]}"
  local index gateway volume path
  (( disk_total <= 32 )) || die "at most 32 --disk entries are supported"
  if (( disk_total == 0 )); then
    (( gateway_total == 0 )) || die "--disk-gateway requires at least one --disk"
    (( volume_total == 0 )) || die "--volume-id requires at least one --disk"
  else
    (( gateway_total == 0 || gateway_total == 1 || gateway_total == disk_total )) || {
      die "provide --disk-gateway once for all disks, or once per --disk"
    }
    (( volume_total == 0 || volume_total == disk_total || (volume_total == 1 && disk_total == 1) )) || {
      die "provide --volume-id once for one disk, or once per --disk"
    }
    for ((index = 1; index <= gateway_total; index++)); do
      gateway="${DISK_GATEWAYS[$((index - 1))]}"
      valid_uint "$gateway" || die "--disk-gateway must be an integer"
      (( gateway >= 1 && gateway <= GATEWAYS )) || die "--disk-gateway must be between 1 and ${GATEWAYS}"
    done
    for ((index = 1; index <= disk_total; index++)); do
      path="${DISK_PATHS[$((index - 1))]}"
      [[ -n "$path" ]] || die "--disk path must be non-empty"
      validate_absolute_path disk "$path"
      volume="$(volume_id_for "$index")"
      validate_id volume-id "$volume"
      for ((gateway = 1; gateway < index; gateway++)); do
        [[ "$volume" != "$(volume_id_for "$gateway")" ]] || die "StorageVolume IDs must be unique"
        [[ "$path" != "${DISK_PATHS[$((gateway - 1))]}" ]] || die "--disk paths must be unique"
      done
    done
  fi
  validate_port central-port "$CENTRAL_PORT"
  validate_port base-port "$BASE_PORT"
  valid_uint "$WAIT_SECONDS" || die "--wait must be a positive integer"
  (( WAIT_SECONDS > 0 && WAIT_SECONDS <= 3600 )) || die "--wait must be between 1 and 3600 seconds"
  (( NO_BUILD == 0 || REBUILD == 0 )) || die "--rebuild cannot be combined with --no-build"
  validate_absolute_path data-dir "$DATA_DIR"
  validate_id tenant "$TENANT_ID"
  validate_id edge-cluster "$EDGE_CLUSTER_ID"
  validate_id pool-id "$POOL_ID"
  [[ -n "$CENTRAL_TOKEN" && "$CENTRAL_TOKEN" != *[[:space:]]* ]] || die "--central-token must be non-empty and contain no whitespace"
  [[ -n "$REGION" && "$REGION" =~ ^[A-Za-z0-9._-]{1,128}$ ]] || die "--region contains unsupported characters"
  [[ -n "$DISPLAY_NAME" && "$DISPLAY_NAME" != *$'\n'* && "$DISPLAY_NAME" != *$'\r'* ]] || die "--display-name must be non-empty and single-line"
  # Reserve five slots per Gateway (HTTP agent/control/peer, QUIC transfer, and one spare),
  # followed by one QUIC listener slot per volume-scoped Agent.
  local last_port=$(( BASE_PORT + GATEWAYS * 5 + ${#DISK_PATHS[@]} ))
  (( last_port <= 65535 )) || die "--base-port and --gateways produce a port above 65535"
}

gateway_agent_port() { printf '%s\n' $(( BASE_PORT + ($1 - 1) * 3 )); }
gateway_control_port() { printf '%s\n' $(( BASE_PORT + ($1 - 1) * 3 + 1 )); }
gateway_peer_port() { printf '%s\n' $(( BASE_PORT + ($1 - 1) * 3 + 2 )); }
gateway_transfer_port() { printf '%s\n' $(( BASE_PORT + GATEWAYS * 3 + ($1 - 1) )); }
agent_transfer_port() { printf '%s\n' $(( BASE_PORT + GATEWAYS * 4 + ($1 - 1) )); }
pid_file_for() { printf '%s/pids/%s.pid\n' "$DATA_DIR" "$1"; }
log_file_for() { printf '%s/logs/%s.log\n' "$DATA_DIR" "$1"; }

process_matches() {
  local name="$1" pid="$2" command expected
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  case "$name" in
    central) expected=neoengram-central ;;
    agent-*) expected=neoengram-agent ;;
    gateway-*) expected=neoengram-gateway ;;
    *) return 1 ;;
  esac
  [[ "$command" == *"$expected"* ]]
}

read_pid() {
  local path
  path="$(pid_file_for "$1")"
  [[ -s "$path" ]] || return 1
  tr -d '[:space:]' <"$path"
}

process_running() {
  local name="$1" pid
  pid="$(read_pid "$name" 2>/dev/null || true)"
  [[ -n "$pid" ]] && process_matches "$name" "$pid"
}

stack_component_record_exists() {
  local kind="$1" index="$2" name
  name="${kind}-${index}"
  [[ -e "$(pid_file_for "$name")" ]] && return 0
  case "$kind" in
    gateway) [[ -e "${DATA_DIR}/gateways/${name}" ]] ;;
    agent) [[ -e "${DATA_DIR}/agents/${name}" ]] ;;
    *) return 1 ;;
  esac
}

any_stack_process_running() {
  local index
  process_running central && return 0
  for ((index = 1; index <= 32; index++)); do
    process_running "gateway-${index}" && return 0
    process_running "agent-${index}" && return 0
  done
  return 1
}

start_process() {
  local name="$1"
  shift
  local pid_path log_path pid
  pid_path="$(pid_file_for "$name")"
  log_path="$(log_file_for "$name")"
  if [[ -s "$pid_path" ]]; then
    pid="$(read_pid "$name" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && process_matches "$name" "$pid"; then
      die "${name} is already running (pid ${pid}); use stop or another --data-dir"
    fi
    rm -f "$pid_path"
  fi
  mkdir -p "$(dirname "$pid_path")" "$(dirname "$log_path")"
  : >"$log_path"
  # Detach local services from the invoking terminal while retaining their real PID.
  nohup perl -MPOSIX -e 'POSIX::setsid() or die "setsid: $!"; exec @ARGV or die "exec: $!"' -- "$@" >"$log_path" 2>&1 </dev/null &
  pid=$!
  printf '%s\n' "$pid" >"$pid_path"
  chmod 600 "$pid_path" "$log_path"
  sleep 0.2
  if ! process_matches "$name" "$pid"; then
    log "${name} exited during startup; see ${log_path}"
    return 1
  fi
}

stop_process() {
  local name="$1" pid deadline
  pid="$(read_pid "$name" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    rm -f "$(pid_file_for "$name")"
    return 0
  fi
  if ! process_matches "$name" "$pid"; then
    log "ignoring stale or unrelated ${name} pid ${pid}"
    rm -f "$(pid_file_for "$name")"
    return 0
  fi
  kill -TERM "$pid" 2>/dev/null || true
  deadline=$((SECONDS + 10))
  while (( SECONDS < deadline )); do
    process_matches "$name" "$pid" || break
    sleep 0.2
  done
  if process_matches "$name" "$pid"; then
    log "${name} did not stop after SIGTERM; sending SIGKILL"
    kill -KILL "$pid" 2>/dev/null || true
  fi
  rm -f "$(pid_file_for "$name")"
}

stop_stack() {
  local index
  if [[ -d "$DATA_DIR" ]]; then
    for ((index = 32; index >= 1; index--)); do
      stop_process "agent-${index}" || true
    done
    for ((index = 32; index >= 1; index--)); do
      stop_process "gateway-${index}" || true
    done
    stop_process central || true
  fi
}

wait_http() {
  local url="$1" timeout_seconds deadline
  timeout_seconds="${2:-$WAIT_SECONDS}"
  deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if curl --silent --show-error --fail --max-time 2 "$url" >/dev/null 2>&1; then return 0; fi
    sleep 0.25
  done
  return 1
}

wait_file() {
  local path="$1" timeout_seconds deadline
  timeout_seconds="${2:-$WAIT_SECONDS}"
  deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    [[ -s "$path" ]] && return 0
    sleep 0.25
  done
  return 1
}

wait_agent_live() {
  wait_agent_health "$1" "$2" live "${3:-$WAIT_SECONDS}"
}

wait_agent_ready() {
  wait_agent_health "$1" "$2" ready "${3:-$WAIT_SECONDS}"
}

wait_agent_health() {
  local state_dir="$1" binary="$2" mode="$3" timeout_seconds="$4" deadline
  deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if "$binary" health --state-dir "$state_dir" --mode "$mode" >/dev/null 2>&1; then return 0; fi
    sleep 0.25
  done
  return 1
}

api_post() {
  local path="$1" body="$2" request_id="$3"
  curl --silent --show-error --fail-with-body -X POST "${CENTRAL_URL}${path}" \
    -H 'content-type: application/json' -H 'NeoEngram-API-Version: 1' \
    -H "authorization: Bearer ${CENTRAL_TOKEN}" -H "x-request-id: ${request_id}" \
    --data "$body"
}

resolve_binary() {
  local variable_name="$1" default_path="$2" command
  command="${!variable_name:-$default_path}"
  if [[ -x "$command" ]]; then printf '%s\n' "$command"; return 0; fi
  command="$(command -v "$(basename "$default_path")" 2>/dev/null || true)"
  [[ -n "$command" && -x "$command" ]] || return 1
  printf '%s\n' "$command"
}

binary_has_newer_sources() {
  local binary="$1" source_root source_file
  shift
  [[ -x "$binary" ]] || return 0
  for source_root in "$@"; do
    if [[ -f "$source_root" ]]; then
      [[ "$source_root" -nt "$binary" ]] && return 0
      continue
    fi
    [[ -d "$source_root" ]] || continue
    source_file="$(find "$source_root" -type f -newer "$binary" -print -quit 2>/dev/null)"
    [[ -n "$source_file" ]] && return 0
  done
  return 1
}

ensure_binaries() {
  CENTRAL_BIN="$(resolve_binary NEOENGRAM_CENTRAL_BIN "${REPO_ROOT}/target/debug/neoengram-central" || true)"
  GATEWAY_BIN="$(resolve_binary NEOENGRAM_GATEWAY_BIN "${REPO_ROOT}/target/debug/neoengram-gateway" || true)"
  AGENT_BIN="$(resolve_binary NEOENGRAM_AGENT_BIN "${REPO_ROOT}/target/debug/neoengram-agent" || true)"
  local need_agent=0
  local stale_binary=0
  (( ${#DISK_PATHS[@]} > 0 )) && [[ -z "$AGENT_BIN" ]] && need_agent=1
  if binary_has_newer_sources "$CENTRAL_BIN" \
      "${REPO_ROOT}/Cargo.toml" "${REPO_ROOT}/Cargo.lock" \
      "${REPO_ROOT}/crates/neoengram-domain" "${REPO_ROOT}/crates/neoengram-runtime" \
      "${REPO_ROOT}/services/neoengram-central" || \
    binary_has_newer_sources "$GATEWAY_BIN" \
      "${REPO_ROOT}/Cargo.toml" "${REPO_ROOT}/Cargo.lock" \
      "${REPO_ROOT}/crates/neoengram-domain" "${REPO_ROOT}/services/neoengram-gateway" || \
    { (( ${#DISK_PATHS[@]} > 0 )) && binary_has_newer_sources "$AGENT_BIN" \
      "${REPO_ROOT}/Cargo.toml" "${REPO_ROOT}/Cargo.lock" \
      "${REPO_ROOT}/crates/neoengram-domain" "${REPO_ROOT}/crates/neoengram-runtime" \
      "${REPO_ROOT}/services/neoengram-agent"; }; then
    stale_binary=1
  fi
  if (( REBUILD == 1 || stale_binary == 1 )) || [[ -z "$CENTRAL_BIN" || -z "$GATEWAY_BIN" ]] || (( need_agent == 1 )); then
    if (( NO_BUILD == 1 )); then
      if (( stale_binary == 1 )); then
        die "local Rust sources are newer than the development binaries; remove --no-build or build them first"
      fi
      die "required binaries are missing; remove --no-build or build them first"
    fi
    local agent_suffix=
    (( ${#DISK_PATHS[@]} > 0 )) && agent_suffix=/Agent
    if (( stale_binary == 1 && REBUILD == 0 )); then
      log "local Rust sources changed; rebuilding Central/Gateway${agent_suffix} binaries"
    else
      log "building local Central/Gateway${agent_suffix} binaries"
    fi
    local -a build_args=(cargo build --locked -p neoengram-central --bin neoengram-central -p neoengram-gateway --bin neoengram-gateway)
    if (( ${#DISK_PATHS[@]} > 0 )); then
      build_args+=(-p neoengram-agent --bin neoengram-agent)
    fi
    "${build_args[@]}"
    CENTRAL_BIN="$(resolve_binary NEOENGRAM_CENTRAL_BIN "${REPO_ROOT}/target/debug/neoengram-central")"
    GATEWAY_BIN="$(resolve_binary NEOENGRAM_GATEWAY_BIN "${REPO_ROOT}/target/debug/neoengram-gateway")"
    (( ${#DISK_PATHS[@]} > 0 )) && AGENT_BIN="$(resolve_binary NEOENGRAM_AGENT_BIN "${REPO_ROOT}/target/debug/neoengram-agent")"
  fi
  GATEWAY_SOFTWARE_VERSION="$("$GATEWAY_BIN" --version 2>/dev/null | awk 'NR == 1 { print $2; exit }')"
  [[ "$GATEWAY_SOFTWARE_VERSION" =~ ^[0-9]+(\.[0-9]+)*([.-][A-Za-z0-9.-]+)?$ ]] || die "could not determine Gateway software version from ${GATEWAY_BIN}"
  export CENTRAL_BIN GATEWAY_BIN AGENT_BIN
}

write_enrollment_keyring() {
  local path="$1" key
  key="$(printf '%s' '01234567890123456789012345678901' | base64 | tr '+/' '-_' | tr -d '=\n')"
  umask 077
  printf '{"version":1,"active_key_id":"dev-stack-key","keys":{"dev-stack-key":"%s"}}\n' "$key" >"$path"
  chmod 600 "$path"
}

write_command_trust_bundle() {
  local path="$1"
  # This is the public SPKI for the fixed [0x42; 32] Ed25519 development signer in
  # services/neoengram-central/src/main.rs. Agents only need this verification key.
  umask 077
  printf '%s\n' '{"schema_version":1,"keys":[{"key_id":"central-command-local","certificate_generation":"1","public_key_spki":"MCowBQYDK2VwAyEAIVL40Zt5HSRFMkLhXy6rbLfP-ntqXtMAl5YOBpiB2xI","state":"active"}]}' >"$path"
  chmod 600 "$path"
}

write_dev_ca() {
  local key_path="$1" cert_path="$2"
  if [[ ! -s "$cert_path" || ! -s "$key_path" ]]; then
    [[ ! -e "$cert_path" && ! -e "$key_path" ]] || die "development CA key and certificate must be present as a pair: ${key_path}, ${cert_path}"
    openssl req -x509 -newkey rsa:2048 -nodes -keyout "$key_path" -out "$cert_path" \
      -subj '/CN=NeoEngram local development CA' -days 3650 >/dev/null 2>&1
  fi
  [[ -s "$key_path" && -s "$cert_path" ]] || die "development CA files could not be created"
  chmod 600 "$key_path" "$cert_path"
}

extract_second_certificate() {
  local chain="$1"
  awk '
    /-----BEGIN CERTIFICATE-----/ { certificate += 1 }
    certificate == 2 { print }
    /-----END CERTIFICATE-----/ && certificate == 2 { exit }
  ' "$chain"
}

write_gateway_bootstrap_key() {
  local path="$1"
  if [[ ! -s "$path" ]]; then openssl genpkey -algorithm ED25519 -out "$path" >/dev/null 2>&1; fi
  chmod 600 "$path"
}

write_volume_fixture() {
  local root="$1" volume_id="$2"
  mkdir -p "$root/objects" "$root/workspaces"
  [[ -e "$root/.neoengram-volume-marker" ]] || printf '%s\n' "$volume_id" >"$root/.neoengram-volume-marker"
  [[ "$(cat "$root/.neoengram-volume-marker")" == "$volume_id" ]] || die "disk marker does not match --volume-id: ${root}"
  chmod 700 "$root"
}

yaml_quote() {
  printf '%s' "$1" | jq -Rsa .
}

write_agent_config() {
  local path="$1" gateway_endpoint="$2" token_id="$3" bootstrap_token_file="$4" digest="$5" state_dir="$6" mount_path="$7" marker_file="$8" volume_id="$9"
  local replication_enabled="${10:-false}" replication_listen="${11:-}" replication_gateway="${12:-}"
  local replication_certificate="${13:-}" replication_private_key="${14:-}" replication_ca="${15:-}"
  local gateway_endpoint_yaml trust_bundle_yaml command_trust_bundle_yaml tenant_yaml edge_cluster_yaml volume_yaml digest_yaml region_yaml
  local mount_path_yaml state_dir_yaml marker_file_yaml token_id_yaml bootstrap_token_file_yaml
  local workload_trust_domain_yaml replication_listen_yaml replication_gateway_yaml replication_certificate_yaml
  local replication_private_key_yaml replication_ca_yaml replication_yaml
  gateway_endpoint_yaml="$(yaml_quote "$gateway_endpoint")"
  trust_bundle_yaml="$(yaml_quote "$CA_CERT")"
  command_trust_bundle_yaml="$(yaml_quote "$COMMAND_TRUST_BUNDLE")"
  tenant_yaml="$(yaml_quote "$TENANT_ID")"
  edge_cluster_yaml="$(yaml_quote "$EDGE_CLUSTER_ID")"
  volume_yaml="$(yaml_quote "$volume_id")"
  digest_yaml="$(yaml_quote "$digest")"
  region_yaml="$(yaml_quote "$REGION")"
  mount_path_yaml="$(yaml_quote "$mount_path")"
  state_dir_yaml="$(yaml_quote "$state_dir")"
  marker_file_yaml="$(yaml_quote "$marker_file")"
  token_id_yaml="$(yaml_quote "$token_id")"
  bootstrap_token_file_yaml="$(yaml_quote "$bootstrap_token_file")"
  workload_trust_domain_yaml="$(yaml_quote "$WORKLOAD_TRUST_DOMAIN")"
  if [[ "$replication_enabled" == true ]]; then
    replication_listen_yaml="$(yaml_quote "$replication_listen")"
    replication_gateway_yaml="$(yaml_quote "$replication_gateway")"
    replication_certificate_yaml="$(yaml_quote "$replication_certificate")"
    replication_private_key_yaml="$(yaml_quote "$replication_private_key")"
    replication_ca_yaml="$(yaml_quote "$replication_ca")"
    replication_yaml="  enabled: true"
    replication_yaml+=$'\n  listen_endpoint: '"${replication_listen_yaml}"
    replication_yaml+=$'\n  gateway_endpoint: '"${replication_gateway_yaml}"
    replication_yaml+=$'\n  tls_certificate_file: '"${replication_certificate_yaml}"
    replication_yaml+=$'\n  tls_private_key_file: '"${replication_private_key_yaml}"
    replication_yaml+=$'\n  tls_ca_file: '"${replication_ca_yaml}"
  else
    replication_yaml="  enabled: false"
  fi
  mkdir -p "$state_dir" "$(dirname "$bootstrap_token_file")" "$(dirname "$path")"
  chmod 700 "$state_dir"
  cat >"$path" <<EOF
schema_version: 1
wire_version: 1
validation_mode: development
gateway_endpoint: ${gateway_endpoint_yaml}
trust_bundle_file: ${trust_bundle_yaml}
gateway_workload_trust_domain: ${workload_trust_domain_yaml}
central_command_trust_bundle_file: ${command_trust_bundle_yaml}
replication:
${replication_yaml}
tenant_id: ${tenant_yaml}
edge_cluster_id: ${edge_cluster_yaml}
storage_volume_id: ${volume_yaml}
volume_descriptor_digest: ${digest_yaml}
region: ${region_yaml}
storage:
  backend_type: pvc
  access_mode: read_write_many
  mount_path: ${mount_path_yaml}
  state_dir: ${state_dir_yaml}
  marker_file: ${marker_file_yaml}
  expected_volume_marker: ${volume_yaml}
  pvc_reference:
    namespace: dev-stack
    claim_name: ${volume_yaml}
registration:
  approval_required: true
  token_id: ${token_id_yaml}
  bootstrap_token_file: ${bootstrap_token_file_yaml}
session:
  heartbeat_interval_seconds: 10
  reconnect_max_delay_seconds: 30
logging:
  format: json
  level: info
EOF
  chmod 600 "$path"
}

print_dry_run() {
  local index disk_total="${#DISK_PATHS[@]}"
  printf 'mode: dry-run (no files or processes changed)\n'
  printf 'central: http://127.0.0.1:%s\n' "$CENTRAL_PORT"
  printf 'pool: %s (edge cluster %s, replicas %s)\n' "$POOL_ID" "$EDGE_CLUSTER_ID" "$GATEWAYS"
  for ((index = 1; index <= GATEWAYS; index++)); do
    printf 'gateway-%s: agent=http://127.0.0.1:%s control=http://127.0.0.1:%s peer=http://127.0.0.1:%s\n' \
      "$index" "$(gateway_agent_port "$index")" "$(gateway_control_port "$index")" "$(gateway_peer_port "$index")"
  done
  if (( disk_total > 0 )); then
    if (( disk_total == 1 )); then
      printf 'disk: gateway-%s -> %s (Agent/Volume)\n' "$(disk_gateway_for 1)" "$(disk_path_for 1)"
    else
      for ((index = 1; index <= disk_total; index++)); do
        printf 'disk-%s: gateway-%s -> %s (Agent/Volume %s)\n' \
          "$index" "$(disk_gateway_for "$index")" "$(disk_path_for "$index")" "$(volume_id_for "$index")"
      done
    fi
  else
    printf 'disk: none\n'
  fi
}

create_or_load_pool() {
  local first_agent_port="$1" body response state resource_version
  body="$(jq -cn --arg pool "$POOL_ID" --arg edge "$EDGE_CLUSTER_ID" --arg display "$DISPLAY_NAME" \
    --arg endpoint "http://127.0.0.1:${first_agent_port}" --argjson replicas "$GATEWAYS" \
    '{gateway_pool_id:$pool,edge_cluster_id:$edge,display_name:$display,agent_endpoint:$endpoint,desired_replicas:$replicas,minimum_ready_replicas:1}')"
  response="$(api_post /api/gateway/pool/create "$body" dev-stack-pool-create)" || die "GatewayPool create failed"
  state="$(jq -r '.gateway_pool.state // empty' <<<"$response")"
  resource_version="$(jq -r '.gateway_pool.resource_version // empty' <<<"$response")"
  [[ -n "$state" && -n "$resource_version" ]] || die "invalid GatewayPool response"
  printf '%s\n' "$state" >"${DATA_DIR}/pool.state"
  printf '%s\n' "$resource_version" >"${DATA_DIR}/pool.resource-version"
  printf '%s\n' "$response" >"${DATA_DIR}/pool.json"
  chmod 600 "${DATA_DIR}/pool.state" "${DATA_DIR}/pool.resource-version" "${DATA_DIR}/pool.json"
}

create_or_load_replica() {
  local index="$1" agent_port control_port peer_port replica_id replica_dir key_path token_path cert_path
  local body response state resource_version activation_token
  agent_port="$(gateway_agent_port "$index")"; control_port="$(gateway_control_port "$index")"; peer_port="$(gateway_peer_port "$index")"
  replica_id="gateway-${index}"; replica_dir="${DATA_DIR}/gateways/${replica_id}"
  key_path="${replica_dir}/bootstrap-key.pem"; token_path="${replica_dir}/activation-token"; cert_path="${replica_dir}/certificate-chain.pem"
  mkdir -p "$replica_dir"; chmod 700 "$replica_dir"; write_gateway_bootstrap_key "$key_path"
  body="$(jq -cn --arg replica "$replica_id" --arg pool "$POOL_ID" --arg software "$GATEWAY_SOFTWARE_VERSION" \
    --arg control "http://127.0.0.1:${control_port}" --arg peer "http://127.0.0.1:${peer_port}" \
    --arg bootstrap "http://127.0.0.1:${agent_port}" --argjson capabilities "$GATEWAY_CAPABILITIES_JSON" \
    '{gateway_replica_id:$replica,gateway_pool_id:$pool,control_endpoint:$control,peer_endpoint:$peer,bootstrap_endpoint:$bootstrap,software_version:$software,wire_version:1,capabilities:$capabilities}')"
  response="$(api_post /api/gateway/replica/create "$body" dev-stack-${replica_id}-create)" || die "${replica_id} create failed"
  state="$(jq -r '.gateway_replica.state // empty' <<<"$response")"; resource_version="$(jq -r '.gateway_replica.resource_version // empty' <<<"$response")"
  activation_token="$(jq -r '.activation_token // empty' <<<"$response")"
  [[ -n "$state" && -n "$resource_version" ]] || die "invalid ${replica_id} response"
  if [[ -n "$activation_token" ]]; then printf '%s\n' "$activation_token" >"$token_path"; chmod 600 "$token_path"; fi
  printf '%s\n' "$state" >"${replica_dir}/state"; printf '%s\n' "$resource_version" >"${replica_dir}/resource-version"
  printf '%s\n' "$response" >"${replica_dir}/create.json"
  chmod 600 "${replica_dir}/state" "${replica_dir}/resource-version" "${replica_dir}/create.json"
}

start_gateway() {
  local index="$1" replica_id="gateway-${1}" replica_dir="${DATA_DIR}/gateways/gateway-${1}"
  local agent_port control_port peer_port key_path token_path cert_path state token
  agent_port="$(gateway_agent_port "$index")"; control_port="$(gateway_control_port "$index")"; peer_port="$(gateway_peer_port "$index")"
  key_path="${replica_dir}/bootstrap-key.pem"; token_path="${replica_dir}/activation-token"; cert_path="${replica_dir}/certificate-chain.pem"
  state="$(cat "${replica_dir}/state")"
  if [[ "$state" == active ]]; then
    start_process "gateway-${index}" "$GATEWAY_BIN" --edge-cluster-id "$EDGE_CLUSTER_ID" --gateway-pool-id "$POOL_ID" --gateway-replica-id "$replica_id" \
      --agent-listen "127.0.0.1:${agent_port}" --control-listen "127.0.0.1:${control_port}" --peer-listen "127.0.0.1:${peer_port}" \
      --central-upstream "$CENTRAL_URL" --log 'neoengram_gateway=info'
    return 0
  fi
  token="$(cat "$token_path" 2>/dev/null || true)"
  [[ -n "$token" ]] || die "${replica_id} is ${state}, but no activation token is available"
  start_process "gateway-${index}" "$GATEWAY_BIN" --edge-cluster-id "$EDGE_CLUSTER_ID" --gateway-pool-id "$POOL_ID" --gateway-replica-id "$replica_id" \
    --agent-listen "127.0.0.1:${agent_port}" --control-listen "127.0.0.1:${control_port}" --peer-listen "127.0.0.1:${peer_port}" \
    --central-upstream "$CENTRAL_URL" --workload-trust-domain development.neoengram.local \
    --private-key-file "$key_path" --activation-token-file "$token_path" --certificate-chain-file "$cert_path" \
    --log 'neoengram_gateway=info'
  wait_http "http://127.0.0.1:${agent_port}/health/live" "$WAIT_SECONDS" || die "${replica_id} did not become live"
  local activation_body activation_response
  activation_body="$(jq -cn --arg replica "$replica_id" --arg rv "$(cat "${replica_dir}/resource-version")" --arg token "$token" '{gateway_replica_id:$replica,expected_resource_version:$rv,activation_token:$token}')"
  activation_response="$(api_post /api/gateway/replica/activate "$activation_body" dev-stack-${replica_id}-activate)" || die "${replica_id} activation failed; see $(log_file_for "gateway-${index}")"
  [[ "$(jq -r '.gateway_replica.state // empty' <<<"$activation_response")" == active ]] || die "${replica_id} activation response was not active"
  printf '%s\n' active >"${replica_dir}/state"; printf '%s\n' "$(jq -r '.gateway_replica.resource_version' <<<"$activation_response")" >"${replica_dir}/resource-version"
  printf '%s\n' "$activation_response" >"${replica_dir}/activate.json"; chmod 600 "${replica_dir}/state" "${replica_dir}/resource-version" "${replica_dir}/activate.json"
  wait_file "$cert_path" "$WAIT_SECONDS" || die "${replica_id} did not write its activation certificate"
  stop_process "gateway-${index}"
  start_process "gateway-${index}" "$GATEWAY_BIN" --edge-cluster-id "$EDGE_CLUSTER_ID" --gateway-pool-id "$POOL_ID" --gateway-replica-id "$replica_id" \
    --agent-listen "127.0.0.1:${agent_port}" --control-listen "127.0.0.1:${control_port}" --peer-listen "127.0.0.1:${peer_port}" \
    --central-upstream "$CENTRAL_URL" --log 'neoengram_gateway=info'
}

mark_pool_ready() {
  local state resource_version body response
  state="$(cat "${DATA_DIR}/pool.state")"; [[ "$state" == ready ]] && return 0
  resource_version="$(cat "${DATA_DIR}/pool.resource-version")"
  body="$(jq -cn --arg pool "$POOL_ID" --arg rv "$resource_version" '{gateway_pool_id:$pool,expected_resource_version:$rv,state:"ready"}')"
  response="$(api_post /api/gateway/pool/update "$body" dev-stack-pool-ready)" || die "GatewayPool could not be marked ready"
  [[ "$(jq -r '.gateway_pool.state // empty' <<<"$response")" == ready ]] || die "GatewayPool update response was not ready"
  printf '%s\n' ready >"${DATA_DIR}/pool.state"; printf '%s\n' "$(jq -r '.gateway_pool.resource_version' <<<"$response")" >"${DATA_DIR}/pool.resource-version"
  printf '%s\n' "$response" >"${DATA_DIR}/pool-ready.json"; chmod 600 "${DATA_DIR}/pool.state" "${DATA_DIR}/pool.resource-version" "${DATA_DIR}/pool-ready.json"
}

create_agent_enrollment() {
  local index="$1" agent_port gateway agent_dir config_path state_dir token_path marker_path body response token_id bootstrap_token digest volume_id disk_path
  gateway="$(disk_gateway_for "$index")"; agent_port="$(gateway_agent_port "$gateway")"; disk_path="$(disk_path_for "$index")"; volume_id="$(volume_id_for "$index")"
  agent_dir="${DATA_DIR}/agents/agent-${index}"
  config_path="${agent_dir}/agent.yaml"; state_dir="${agent_dir}/state"; token_path="${agent_dir}/bootstrap-token"; marker_path="${disk_path}/.neoengram-volume-marker"
  mkdir -p "$agent_dir"
  body="$(jq -cn --arg tenant "$TENANT_ID" --arg request "dev-stack-${volume_id}-token" --arg volume "$volume_id" \
    --arg display "${volume_id} development Volume" --arg edge "$EDGE_CLUSTER_ID" --arg region "$REGION" \
    '{tenant_id:$tenant,token_request_id:$request,storage_volume_id:$volume,display_name:$display,edge_cluster_id:$edge,region:$region,access_mode:"read_write_many",pvc_reference:{namespace:"dev-stack",claim_name:$volume}}')"
  response="$(api_post /api/storage/enrollment/token/create "$body" dev-stack-${volume_id}-token)" || die "Storage enrollment token creation failed"
  token_id="$(jq -r '.token_id // empty' <<<"$response")"; bootstrap_token="$(jq -r '.bootstrap_token // empty' <<<"$response")"; digest="$(jq -r '.volume_descriptor_digest // empty' <<<"$response")"
  [[ -n "$token_id" && -n "$bootstrap_token" && -n "$digest" ]] || die "invalid Storage enrollment token response"
  printf '%s\n' "$bootstrap_token" >"$token_path"; chmod 600 "$token_path"; printf '%s\n' "$response" >"${agent_dir}/enrollment-token.json"; chmod 600 "${agent_dir}/enrollment-token.json"
  write_agent_config "$config_path" "http://127.0.0.1:${agent_port}/" "$token_id" "$token_path" "$digest" "$state_dir" "$disk_path" "$marker_path" "$volume_id"
  printf '%s\n' "$config_path"
}

find_enrollment() {
  local index="$1" state="${2:-}" query response request_id volume_id edge_cluster_id
  volume_id="$(volume_id_for "$index")"; edge_cluster_id="$EDGE_CLUSTER_ID"
  if [[ -n "$state" ]]; then
    query="$(jq -cn --arg tenant "$TENANT_ID" --arg state "$state" '{tenant_id:$tenant,state:$state,page_size:100}')"
  else
    query="$(jq -cn --arg tenant "$TENANT_ID" '{tenant_id:$tenant,page_size:100}')"
  fi
  request_id="dev-stack-enrollment-list-${SECONDS}-${RANDOM}"
  response="$(api_post /api/storage/enrollment/list/query "$query" "$request_id")" || return 1
  jq -c --arg volume "$volume_id" --arg edge "$edge_cluster_id" '[.items[]? | select(.storage_volume_id == $volume and .edge_cluster_id == $edge)] | .[0] // empty' <<<"$response"
}

find_existing_enrollment() {
  local index="$1" state enrollment
  for state in pending_approval approved enrolled rejected expired; do
    enrollment="$(find_enrollment "$index" "$state" || true)"
    if [[ -n "$enrollment" ]]; then
      printf '%s\n' "$enrollment"
      return 0
    fi
  done
  return 0
}

approve_agent_enrollment() {
  local index="$1" deadline=$((SECONDS + WAIT_SECONDS)) enrollment enrollment_id resource_version body response volume_id
  volume_id="$(volume_id_for "$index")"
  while (( SECONDS < deadline )); do
    enrollment="$(find_existing_enrollment "$index")"
    if [[ -n "$enrollment" ]]; then
      case "$(jq -r '.state // empty' <<<"$enrollment")" in
        pending_approval)
          enrollment_id="$(jq -r '.storage_enrollment_id' <<<"$enrollment")"; resource_version="$(jq -r '.resource_version' <<<"$enrollment")"
          body="$(jq -cn --arg tenant "$TENANT_ID" --arg enrollment "$enrollment_id" --arg request "dev-stack-${volume_id}-approve" --arg rv "$resource_version" '{tenant_id:$tenant,storage_enrollment_id:$enrollment,approval_request_id:$request,expected_resource_version:$rv,confirm_replacement:false}')"
          response="$(api_post /api/storage/enrollment/approve "$body" dev-stack-${volume_id}-approve)" || die "Storage enrollment approval failed"
          printf '%s\n' "$response" >"${DATA_DIR}/agents/agent-${index}/approval.json"; chmod 600 "${DATA_DIR}/agents/agent-${index}/approval.json"
          log "Agent enrollment approved; waiting for its authenticated session"; return 0
          ;;
        approved|enrolled)
          log "Agent enrollment is already $(jq -r '.state' <<<"$enrollment"); waiting for its authenticated session"
          return 0
          ;;
        rejected|expired)
          die "Agent enrollment is $(jq -r '.state' <<<"$enrollment"); use a new --data-dir or --volume-id"
          ;;
      esac
    fi
    sleep 0.5
  done
  die "Agent enrollment did not reach pending_approval within ${WAIT_SECONDS}s"
}

prepare_transfer_materials() {
  local index state_db identity_json agent_id agent_dir cert_file key_file key_der_file digest gateway gateway_endpoint
  local gateway_certificate gateway_issuer
  gateway_certificate="${DATA_DIR}/gateways/gateway-1/certificate-chain.pem"
  [[ -s "$gateway_certificate" ]] || die "gateway-1 activation certificate is missing; cannot configure transfer TLS"
  TRANSFER_CA_CERT="${DATA_DIR}/keyring/transfer-ca.pem"
  # Development workload issuance creates an independent issuer for each certificate request.
  # The transfer listener authenticates both Agents and Gateway relay peers, so its CA bundle
  # must include every issuer in the local stack rather than only gateway-1's issuer.
  : >"$TRANSFER_CA_CERT"
  for ((index = 1; index <= GATEWAYS; index++)); do
    gateway_certificate="${DATA_DIR}/gateways/gateway-${index}/certificate-chain.pem"
    [[ -s "$gateway_certificate" ]] || die "gateway-${index} activation certificate is missing; cannot configure transfer TLS"
    gateway_issuer="$(extract_second_certificate "$gateway_certificate")"
    [[ -n "$gateway_issuer" ]] || die "gateway-${index} activation certificate did not contain a transfer CA"
    printf '%s\n' "$gateway_issuer" >>"$TRANSFER_CA_CERT"
  done
  [[ -s "$TRANSFER_CA_CERT" ]] || die "Gateway activation certificate did not contain a transfer CA"
  chmod 600 "$TRANSFER_CA_CERT"

  AGENT_IDS=()
  for ((index = 1; index <= ${#DISK_PATHS[@]}; index++)); do
    agent_dir="${DATA_DIR}/agents/agent-${index}"
    state_db="${agent_dir}/state/agent-state.sqlite3"
    [[ -s "$state_db" ]] || die "Agent ${index} identity database is missing"
    identity_json="$(sqlite3 "$state_db" 'SELECT payload FROM system_identity WHERE singleton = 1;' 2>/dev/null || true)"
    agent_id="$(jq -r '.value.approved.agent_id // empty' <<<"$identity_json")"
    [[ -n "$agent_id" ]] || die "Agent ${index} has no approved Agent ID"
    AGENT_IDS[$index]="$agent_id"
    cert_file="${agent_dir}/replication-certificate-chain.pem"
    key_file="${agent_dir}/replication-private-key.pem"
    key_der_file="${agent_dir}/replication-private-key.der"
    jq -r '.value.certificate.certificate_chain_pem[]' <<<"$identity_json" >"$cert_file"
    # The Agent issuer is also a trust anchor for Gateway-side mTLS. Append it after extracting
    # the identity so the same bundle can authenticate every local transfer peer.
    gateway_issuer="$(extract_second_certificate "$cert_file")"
    [[ -n "$gateway_issuer" ]] || die "Agent ${index} certificate did not contain a transfer CA"
    printf '%s\n' "$gateway_issuer" >>"$TRANSFER_CA_CERT"
    jq -r '.value.private_key[]' <<<"$identity_json" | perl -ne 'chomp; print pack("C", 0 + $_);' >"$key_der_file"
    openssl pkey -inform DER -in "$key_der_file" -out "$key_file" >/dev/null 2>&1 || die "Agent ${index} private key could not be converted to PEM"
    [[ -s "$cert_file" && -s "$key_file" ]] || die "Agent ${index} identity did not contain transfer TLS material"
    chmod 600 "$cert_file" "$key_der_file" "$key_file"
    digest="$(jq -r '.volume_descriptor_digest' "${agent_dir}/enrollment-token.json")"
    gateway="$(disk_gateway_for "$index")"
    gateway_endpoint="http://127.0.0.1:$(gateway_agent_port "$gateway")/"
    write_agent_config "${agent_dir}/agent.yaml" "$gateway_endpoint" \
      "$(jq -r '.token_id' "${agent_dir}/enrollment-token.json")" \
      "${agent_dir}/bootstrap-token" "$digest" "${agent_dir}/state" \
      "$(disk_path_for "$index")" "$(disk_path_for "$index")/.neoengram-volume-marker" \
      "$(volume_id_for "$index")" true \
      "quic://127.0.0.1:$(agent_transfer_port "$index")" \
      "quic://127.0.0.1:$(gateway_transfer_port "$gateway")" \
      "$cert_file" "$key_file" "$TRANSFER_CA_CERT"
  done
}

start_transfer_gateway() {
  local index="$1" replica_id="gateway-${1}" replica_dir="${DATA_DIR}/gateways/gateway-${1}"
  local agent_port control_port peer_port transfer_port cert_file key_file relay_role upstream_server_name
  local route_index
  local -a routes=()
  agent_port="$(gateway_agent_port "$index")"
  control_port="$(gateway_control_port "$index")"
  peer_port="$(gateway_peer_port "$index")"
  transfer_port="$(gateway_transfer_port "$index")"
  cert_file="${replica_dir}/certificate-chain.pem"
  key_file="${replica_dir}/bootstrap-key.pem"
  [[ "$(cat "${replica_dir}/state")" == active ]] || die "${replica_id} is not active"
  [[ -s "$cert_file" && -s "$key_file" && -s "$TRANSFER_CA_CERT" ]] || die "${replica_id} transfer TLS material is incomplete"
  if (( index == 1 )); then
    relay_role=source
    upstream_server_name=127.0.0.1
    for ((route_index = 1; route_index <= ${#DISK_PATHS[@]}; route_index++)); do
      routes+=(--transfer-upstream-route "${AGENT_IDS[$route_index]}=127.0.0.1:$(agent_transfer_port "$route_index")")
    done
  else
    relay_role=target
    upstream_server_name=127.0.0.1
    # Target Gateways take the next hop to the single source Gateway. The source Agent
    # identity remains in the signed ticket and is selected by the source Gateway's directory.
    for ((route_index = 1; route_index <= ${#DISK_PATHS[@]}; route_index++)); do
      routes+=(--transfer-upstream-route "${AGENT_IDS[$route_index]}=127.0.0.1:$(gateway_transfer_port 1)")
    done
  fi
  start_process "$replica_id" "$GATEWAY_BIN" --edge-cluster-id "$EDGE_CLUSTER_ID" --gateway-pool-id "$POOL_ID" --gateway-replica-id "$replica_id" \
    --agent-listen "127.0.0.1:${agent_port}" --control-listen "127.0.0.1:${control_port}" --peer-listen "127.0.0.1:${peer_port}" \
    --transfer-listen "127.0.0.1:${transfer_port}" --transfer-relay-role "$relay_role" \
    --transfer-validation-mode development \
    --transfer-upstream-server-name "$upstream_server_name" "${routes[@]}" \
    --transfer-tls-certificate-file "$cert_file" --transfer-tls-private-key-file "$key_file" \
    --transfer-tls-client-ca-file "$TRANSFER_CA_CERT" --workload-trust-domain "$WORKLOAD_TRUST_DOMAIN" \
    --central-upstream "$CENTRAL_URL" --log 'neoengram_gateway=info'
}

write_stack_info() {
  local index disk_total="${#DISK_PATHS[@]}"
  {
    printf 'topology_version=2\n'
    printf 'central_url=%s\n' "$CENTRAL_URL"; printf 'pool_id=%s\n' "$POOL_ID"; printf 'edge_cluster_id=%s\n' "$EDGE_CLUSTER_ID"
    printf 'central_port=%s\n' "$CENTRAL_PORT"; printf 'base_port=%s\n' "$BASE_PORT"
    printf 'tenant_id=%s\n' "$TENANT_ID"; printf 'region=%s\n' "$REGION"
    printf 'display_name=%s\n' "$DISPLAY_NAME"
    printf 'gateways=%s\n' "$GATEWAYS"; printf 'disk_count=%s\n' "$disk_total"
    if (( disk_total == 1 )); then
      printf 'disk_gateway=%s\n' "$(disk_gateway_for 1)"
      printf 'disk_path=%s\n' "$(disk_path_for 1)"
      printf 'volume_id=%s\n' "$(volume_id_for 1)"
    fi
    for ((index = 1; index <= disk_total; index++)); do
      printf 'disk_%s_gateway=%s\n' "$index" "$(disk_gateway_for "$index")"
      printf 'disk_%s_path=%s\n' "$index" "$(disk_path_for "$index")"
      printf 'disk_%s_volume_id=%s\n' "$index" "$(volume_id_for "$index")"
    done
    for ((index = 1; index <= GATEWAYS; index++)); do
      printf 'gateway_%s_agent_url=http://127.0.0.1:%s\n' "$index" "$(gateway_agent_port "$index")"
      printf 'gateway_%s_control_url=http://127.0.0.1:%s\n' "$index" "$(gateway_control_port "$index")"
      printf 'gateway_%s_peer_url=http://127.0.0.1:%s\n' "$index" "$(gateway_peer_port "$index")"
    done
  } >"${DATA_DIR}/stack.info"
  chmod 600 "${DATA_DIR}/stack.info"
}

start_stack() {
  local index first_agent_port config_path agent_state_dir
  validate_options
  load_saved_topology
  validate_options
  if (( DRY_RUN == 1 )); then print_dry_run; return 0; fi
  require_cmd curl; require_cmd jq; require_cmd sqlite3; require_cmd openssl; require_cmd cargo; require_cmd perl
  DATA_DIR="$(normalize_directory data-dir "$DATA_DIR" 1)"
  for ((index = 1; index <= ${#DISK_PATHS[@]}; index++)); do
    DISK_PATHS[$((index - 1))]="$(normalize_directory "disk-${index}" "$(disk_path_for "$index")" 1)"
    write_volume_fixture "$(disk_path_for "$index")" "$(volume_id_for "$index")"
  done
  if any_stack_process_running; then die "a stack is already running under ${DATA_DIR}; use stop first"; fi
  ensure_binaries
  mkdir -p "${DATA_DIR}/authority" "${DATA_DIR}/keyring" "${DATA_DIR}/gateways" "${DATA_DIR}/agents" "${DATA_DIR}/logs" "${DATA_DIR}/pids"
  chmod 700 "$DATA_DIR" "${DATA_DIR}/keyring" "${DATA_DIR}/gateways" "${DATA_DIR}/agents" "${DATA_DIR}/logs" "${DATA_DIR}/pids"
  write_enrollment_keyring "${DATA_DIR}/keyring/enrollment-keyring.json"
  write_dev_ca "${DATA_DIR}/keyring/dev-ca-key.pem" "${DATA_DIR}/keyring/dev-ca.pem"
  COMMAND_TRUST_BUNDLE="${DATA_DIR}/keyring/central-command-trust.json"
  write_command_trust_bundle "$COMMAND_TRUST_BUNDLE"
  CA_CERT="${DATA_DIR}/keyring/dev-ca.pem"; CENTRAL_URL="http://127.0.0.1:${CENTRAL_PORT}"; export CENTRAL_URL CA_CERT COMMAND_TRUST_BUNDLE
  STARTUP_IN_PROGRESS=1
  start_process central "$CENTRAL_BIN" --bind "127.0.0.1:${CENTRAL_PORT}" --authority-dir "${DATA_DIR}/authority" \
    --agent-enrollment-enabled --agent-enrollment-keyring-file "${DATA_DIR}/keyring/enrollment-keyring.json" \
    --development --development-token "$CENTRAL_TOKEN" --development-tenants "${TENANT_ID},*"
  wait_http "${CENTRAL_URL}/health/live" "$WAIT_SECONDS" || die "Central did not become live; see $(log_file_for central)"
  wait_http "${CENTRAL_URL}/health/ready" "$WAIT_SECONDS" || die "Central did not become ready; see $(log_file_for central)"
  first_agent_port="$(gateway_agent_port 1)"; create_or_load_pool "$first_agent_port"
  for ((index = 1; index <= GATEWAYS; index++)); do create_or_load_replica "$index"; done
  for ((index = 1; index <= GATEWAYS; index++)); do
    start_gateway "$index"
    wait_http "http://127.0.0.1:$(gateway_agent_port "$index")/health/live" "$WAIT_SECONDS" || die "gateway-${index} did not become live"
  done
  mark_pool_ready
  for ((index = 1; index <= GATEWAYS; index++)); do
    wait_http "http://127.0.0.1:$(gateway_agent_port "$index")/health/ready" "$WAIT_SECONDS" || die "gateway-${index} Central control session is not ready"
  done
  for ((index = 1; index <= ${#DISK_PATHS[@]}; index++)); do
    config_path="$(create_agent_enrollment "$index")"; agent_state_dir="${DATA_DIR}/agents/agent-${index}/state"
    start_process "agent-${index}" "$AGENT_BIN" run --config "$config_path" --development-directory-probe
    wait_agent_live "$agent_state_dir" "$AGENT_BIN" "$WAIT_SECONDS" || die "Agent ${index} did not become live; see $(log_file_for "agent-${index}")"
    if (( AUTO_APPROVE == 1 )); then
      approve_agent_enrollment "$index"
      wait_agent_ready "$agent_state_dir" "$AGENT_BIN" "$WAIT_SECONDS" || die "Agent ${index} did not become ready after enrollment approval; see $(log_file_for "agent-${index}")"
    else
      log "Agent ${index} enrollment is pending approval; rerun with --auto-approve for local approval"
    fi
  done
  if (( AUTO_APPROVE == 1 && ${#DISK_PATHS[@]} > 0 )); then
    # Enrollment establishes the Agent URI used by transfer mTLS. Reconfigure the approved
    # identities only after Central has assigned those IDs, then restart the local data plane.
    for ((index = 1; index <= ${#DISK_PATHS[@]}; index++)); do stop_process "agent-${index}"; done
    prepare_transfer_materials
    TRANSFER_PLANE_CONFIGURED=1
    for ((index = 1; index <= GATEWAYS; index++)); do
      stop_process "gateway-${index}"
      start_transfer_gateway "$index"
      wait_http "http://127.0.0.1:$(gateway_agent_port "$index")/health/live" "$WAIT_SECONDS" || die "gateway-${index} did not become live after transfer setup"
      wait_http "http://127.0.0.1:$(gateway_agent_port "$index")/health/ready" "$WAIT_SECONDS" || die "gateway-${index} control session is not ready after transfer setup"
    done
    for ((index = 1; index <= ${#DISK_PATHS[@]}; index++)); do
      start_process "agent-${index}" "$AGENT_BIN" run --config "${DATA_DIR}/agents/agent-${index}/agent.yaml" --development-directory-probe
      wait_agent_ready "${DATA_DIR}/agents/agent-${index}/state" "$AGENT_BIN" "$WAIT_SECONDS" || die "Agent ${index} did not become ready with replication enabled; see $(log_file_for "agent-${index}")"
    done
  fi
  write_stack_info; STARTUP_IN_PROGRESS=0
  log "stack started; Central: ${CENTRAL_URL}"
  log "use 'bash scripts/dev-stack.sh status --data-dir ${DATA_DIR}' to inspect processes"
}

status_stack() {
  validate_absolute_path data-dir "$DATA_DIR"
  if [[ ! -d "$DATA_DIR" ]]; then printf 'dev-stack: stopped (data directory does not exist: %s)\n' "$DATA_DIR"; return 0; fi
  local name index pid state found_gateway found_agent
  printf 'data-dir: %s\n' "$DATA_DIR"
  name=central; pid="$(read_pid "$name" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && process_matches "$name" "$pid"; then printf '%s: running (pid %s)\n' "$name" "$pid"; else printf '%s: stopped\n' "$name"; fi
  found_gateway=0
  for ((index = 1; index <= 32; index++)); do
    stack_component_record_exists gateway "$index" || continue
    found_gateway=1
    name="gateway-${index}"; pid="$(read_pid "$name" 2>/dev/null || true)"; state="$(cat "${DATA_DIR}/gateways/gateway-${index}/state" 2>/dev/null || printf unknown)"
    if [[ -n "$pid" ]] && process_matches "$name" "$pid"; then printf '%s: running (pid %s, registry %s)\n' "$name" "$pid" "$state"; else printf '%s: stopped (registry %s)\n' "$name" "$state"; fi
  done
  found_agent=0
  for ((index = 1; index <= 32; index++)); do
    stack_component_record_exists agent "$index" || continue
    found_agent=1
    name="agent-${index}"; pid="$(read_pid "$name" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && process_matches "$name" "$pid"; then printf '%s: running (pid %s)\n' "$name" "$pid"; else printf '%s: stopped\n' "$name"; fi
  done
  (( found_gateway == 1 || found_agent == 1 )) || printf 'gateways: none\n'
  if [[ -s "${DATA_DIR}/stack.info" ]]; then printf '\nendpoints:\n'; sed -n '1,120p' "${DATA_DIR}/stack.info"; fi
}

stop_stack_action() {
  validate_absolute_path data-dir "$DATA_DIR"
  if [[ ! -d "$DATA_DIR" ]]; then log "nothing to stop under ${DATA_DIR}"; return 0; fi
  stop_stack
  log "stack stopped; state retained under ${DATA_DIR}"
}

on_exit() {
  local status="$?"
  if (( status != 0 && STARTUP_IN_PROGRESS == 1 )); then
    set +e; log "startup failed; stopping processes started by this stack"; stop_stack
  fi
  exit "$status"
}

parse_options "$@"
case "$ACTION" in
  start) trap on_exit EXIT; start_stack ;;
  stop) stop_stack_action ;;
  status) status_stack ;;
esac
