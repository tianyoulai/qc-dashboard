#!/bin/zsh
set -euo pipefail

PROJECT_ROOT="/Users/laitianyou/WorkBuddy/20260408163242/qc-dashboard"
PYTHON_BIN="${PYTHON_BIN:-/Library/Frameworks/Python.framework/Versions/3.14/bin/python3}"
TARGET_HOST="${TARGET_HOST:-qyapi.weixin.qq.com}"
DNS_RETRY_COUNT="${DNS_RETRY_COUNT:-6}"
DNS_RETRY_SLEEP="${DNS_RETRY_SLEEP:-20}"
RUN_RETRY_COUNT="${RUN_RETRY_COUNT:-3}"
RUN_RETRY_SLEEP="${RUN_RETRY_SLEEP:-90}"
LOG_DIR="$PROJECT_ROOT/data/logs"
WRAPPER_LOG="$LOG_DIR/tao-daily-push-wrapper.log"
mkdir -p "$LOG_DIR"

log() {
  local message="$(date '+%Y-%m-%d %H:%M:%S') [tao-daily-push-wrapper] $*"
  print -r -- "$message" >> "$WRAPPER_LOG"
  print -r -- "$message"
}

is_test_mode() {
  for arg in "$@"; do
    if [[ "$arg" == "--test" ]]; then
      return 0
    fi
  done
  return 1
}

check_dns() {
  local host="$1"
  "$PYTHON_BIN" - "$host" <<'PY' >/dev/null
import socket, sys
socket.getaddrinfo(sys.argv[1], 443)
PY
}

wait_for_dns() {
  local attempt=1
  while (( attempt <= DNS_RETRY_COUNT )); do
    if check_dns "$TARGET_HOST"; then
      log "DNS ready: $TARGET_HOST"
      return 0
    fi
    log "DNS not ready for $TARGET_HOST (attempt ${attempt}/${DNS_RETRY_COUNT}), sleep ${DNS_RETRY_SLEEP}s"
    sleep "$DNS_RETRY_SLEEP"
    (( attempt++ ))
  done
  log "DNS still unavailable after ${DNS_RETRY_COUNT} attempts: $TARGET_HOST"
  return 1
}

run_once() {
  cd "$PROJECT_ROOT"
  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/daily_push.py" "$@"
}

main() {
  local attempt=1
  local exit_code=1

  if is_test_mode "$@"; then
    log "test mode detected, skip DNS wait and retry loop"
    run_once "$@"
    return $?
  fi

  while (( attempt <= RUN_RETRY_COUNT )); do
    if ! wait_for_dns; then
      exit_code=68
    else
      log "start daily_push attempt ${attempt}/${RUN_RETRY_COUNT}"
      if run_once "$@"; then
        log "daily_push succeeded on attempt ${attempt}"
        return 0
      fi
      exit_code=$?
      log "daily_push failed on attempt ${attempt} with exit code ${exit_code}"
    fi

    if (( attempt < RUN_RETRY_COUNT )); then
      log "sleep ${RUN_RETRY_SLEEP}s before retry"
      sleep "$RUN_RETRY_SLEEP"
    fi
    (( attempt++ ))
  done

  log "daily_push exhausted retries, exit code=${exit_code}"
  return "$exit_code"
}

main "$@"
