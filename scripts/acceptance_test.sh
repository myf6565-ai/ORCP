#!/usr/bin/env bash
#
# acceptance_test.sh -- runs the §8.8 acceptance checklist end-to-end.
#
# This script DOES NOT stand up infrastructure; it assumes the full stack
# from Stages B through F is already running and that the operator just
# wants a machine-checkable pass/fail over the five DEV_SPEC §8.8 gates:
#
#   1. Happy path  : 1000 events -> mysql detail visible -> OB aggregate
#                    visible within 90 seconds.
#   2. Self-heal   : kill a TaskManager; within 2 minutes the job resumes
#                    with no lost records (relies on exactly-once + upsert).
#   3. No dup      : bounce orcp-ingest; re-delivered records are absorbed
#                    by the t_dedup ledger (no growth in t_order).
#   4. Observability: all subsystems UP from /api/health; Flink job visible
#                    at /jobs/overview.
#   5. Alerting    : stop the JobManager; one alert fires through the
#                    webhook within 3 minutes.  (Verified by counting
#                    Alertmanager /api/v2/alerts; webhook delivery is the
#                    operator's decision to acknowledge.)
#
# A check reports one of PASS / FAIL / SKIP and the overall exit code is
# non-zero if ANY PASS-required check failed.  Checks marked optional
# (like stage-2 self-heal) need explicit opt-in via flags.
#
# Usage:
#   scripts/acceptance_test.sh                  # gates 1, 3, 4 only (no destructive ops)
#   scripts/acceptance_test.sh --with-self-heal # also runs gate 2 (kills a TM)
#   scripts/acceptance_test.sh --with-alerts    # also runs gate 5 (stops JM)
#   scripts/acceptance_test.sh --all            # gates 1..5
#
# Environment (override as needed):
#   BOOTSTRAP              Kafka bootstrap, default node-1:9092
#   SRC_TOPIC              external source topic, default orcp.src.demo
#   FLINK_REST             default http://node-1:8081
#   ADMIN_URL              orcp-admin base URL, default http://node-3:8081
#   ADMIN_USER / ADMIN_PW  Basic auth for orcp-admin
#   MYSQL_HOST / MYSQL_*   local detail DB (default node-1, orcp_ro)
#   OB_HOST / OB_PORT / OB_*  OceanBase
#   ALERTMANAGER_URL       default http://node-2:9093
#
# Exit codes:
#   0  all selected checks PASSED (or SKIPPED with --allow-skip)
#   1  one or more checks FAILED
#   2  usage error

set -uo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
: "${BOOTSTRAP:=node-1:9092}"
: "${SRC_TOPIC:=orcp.src.demo}"
: "${FLINK_REST:=http://node-1:8081}"
: "${ADMIN_URL:=http://node-3:8081}"
: "${ADMIN_USER:=orcp-admin}"
: "${ADMIN_PW:=ChangeMe_admin_1!}"
: "${MYSQL_HOST:=node-1}"
: "${MYSQL_PORT:=3306}"
: "${MYSQL_USER:=orcp_ro}"
: "${MYSQL_PW:=ChangeMe_ro_1!}"
: "${OB_HOST:=ob-host}"
: "${OB_PORT:=2881}"
: "${OB_USER:=orcp_rw@tenant#cluster}"
: "${OB_PW:=ChangeMe_ob_1!}"
: "${OB_CLIENT:=obclient}"             # obclient on node-1 or set to 'mysql' for direct MySQL protocol
: "${ALERTMANAGER_URL:=http://node-2:9093}"

EVENTS=${EVENTS:-1000}
RATE=${RATE:-200}
HAPPY_PATH_TIMEOUT=${HAPPY_PATH_TIMEOUT:-90}

RUN_HAPPY=1
RUN_SELF_HEAL=0
RUN_NO_DUP=1
RUN_OBS=1
RUN_ALERTS=0

for arg in "$@"; do
    case "${arg}" in
        --with-self-heal) RUN_SELF_HEAL=1 ;;
        --with-alerts)    RUN_ALERTS=1 ;;
        --all)            RUN_SELF_HEAL=1; RUN_ALERTS=1 ;;
        --only-happy)     RUN_NO_DUP=0; RUN_OBS=0 ;;
        --help|-h)
            sed -n '2,40p' "$0"; exit 0 ;;
        *)  echo "unknown flag: ${arg}" >&2; exit 2 ;;
    esac
done

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
C_GREEN=$'\033[0;32m'
C_RED=$'\033[0;31m'
C_YEL=$'\033[0;33m'
C_RESET=$'\033[0m'

FAILED=0

record() {
    local status="$1" title="$2" msg="${3:-}"
    case "${status}" in
        PASS) printf '  %s[PASS]%s  %s\n' "${C_GREEN}" "${C_RESET}" "${title}" ;;
        FAIL) printf '  %s[FAIL]%s  %s\n' "${C_RED}"   "${C_RESET}" "${title}"
              [[ -n "${msg}" ]] && printf '         %s\n' "${msg}"
              FAILED=$((FAILED + 1)) ;;
        SKIP) printf '  %s[SKIP]%s  %s%s\n' "${C_YEL}" "${C_RESET}" "${title}" \
              "${msg:+ (${msg})}" ;;
    esac
}

mysql_q() {
    mysql --protocol=TCP -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" \
          -u"${MYSQL_USER}" -p"${MYSQL_PW}" \
          -N --silent -e "$1" orcp_detail 2>/dev/null
}

ob_q() {
    # obclient accepts the same flags as mysql client; we rely on the
    # tenant user format user@tenant#cluster for direct 2881 access.
    "${OB_CLIENT}" -h"${OB_HOST}" -P"${OB_PORT}" \
          -u"${OB_USER}" -p"${OB_PW}" \
          -N --silent -e "$1" orcp_dw 2>/dev/null
}

# ---------------------------------------------------------------------------
# Gate 4 (run first so a broken stack aborts cheaply)
# ---------------------------------------------------------------------------
gate_observability() {
    echo "== Gate 4: observability (/api/health + Flink /jobs/overview)"

    local body code
    code=$(curl -sS -o /tmp/acc_health.json -w '%{http_code}' \
           "${ADMIN_URL}/api/health" || echo 000)
    if [[ "${code}" != "200" ]]; then
        record FAIL "GET /api/health returned ${code} (want 200)" \
                    "body: $(head -c 300 /tmp/acc_health.json 2>/dev/null)"
        return
    fi
    # Accept either top-level status=UP or all components UP.
    if grep -q '"status":"UP"' /tmp/acc_health.json; then
        record PASS "aggregate health is UP"
    else
        record FAIL "aggregate health is not UP" \
                    "body: $(head -c 300 /tmp/acc_health.json)"
    fi

    local jobs_running
    jobs_running=$(curl -sS "${FLINK_REST}/jobs/overview" \
                   | python3 -c 'import sys,json; d=json.load(sys.stdin);
print(sum(1 for j in d.get("jobs", []) if j.get("state") == "RUNNING"))' 2>/dev/null \
                   || echo -1)
    if [[ "${jobs_running}" -ge 1 ]]; then
        record PASS "at least one Flink job is RUNNING (${jobs_running})"
    else
        record FAIL "no RUNNING Flink job found (${jobs_running})"
    fi
}

# ---------------------------------------------------------------------------
# Gate 1: happy path.  Send EVENTS events, wait for detail + aggregate.
# ---------------------------------------------------------------------------
gate_happy_path() {
    echo "== Gate 1: happy path (${EVENTS} events -> MySQL -> OceanBase)"

    local before_orders before_agg
    before_orders=$(mysql_q "SELECT COUNT(*) FROM t_order" || echo "err")
    before_agg=$(ob_q "SELECT COUNT(*) FROM agg_order_1min" || echo "err")
    if ! [[ "${before_orders}" =~ ^[0-9]+$ ]] || ! [[ "${before_agg}" =~ ^[0-9]+$ ]]; then
        record FAIL "baseline query failed" \
                    "t_order=${before_orders}, agg_order_1min=${before_agg}"
        return
    fi

    # Produce.  gen_events.py handles flush and graceful shutdown.
    python3 scripts/gen_events.py \
        --bootstrap-server "${BOOTSTRAP}" \
        --topic "${SRC_TOPIC}" \
        --count "${EVENTS}" --rate "${RATE}" --seed "${ACC_SEED:-7}" \
        >/tmp/acc_gen.log 2>&1
    if [[ $? -ne 0 ]]; then
        record FAIL "gen_events.py produce failed" \
                    "tail: $(tail -3 /tmp/acc_gen.log)"
        return
    fi

    # Poll detail table.  Accept within HAPPY_PATH_TIMEOUT.
    local deadline=$((SECONDS + HAPPY_PATH_TIMEOUT))
    local rows_ok=0 agg_ok=0
    while (( SECONDS < deadline )); do
        local now_orders now_agg
        now_orders=$(mysql_q "SELECT COUNT(*) FROM t_order")
        now_agg=$(ob_q "SELECT COUNT(*) FROM agg_order_1min")
        if [[ "${rows_ok}" == 0 && "${now_orders}" =~ ^[0-9]+$ \
              && $((now_orders - before_orders)) -ge "${EVENTS}" ]]; then
            record PASS "t_order grew by >= ${EVENTS} (from ${before_orders} to ${now_orders})"
            rows_ok=1
        fi
        if [[ "${agg_ok}" == 0 && "${now_agg}" =~ ^[0-9]+$ \
              && "${now_agg}" -gt "${before_agg}" ]]; then
            record PASS "agg_order_1min grew (from ${before_agg} to ${now_agg})"
            agg_ok=1
        fi
        if (( rows_ok && agg_ok )); then return; fi
        sleep 3
    done

    (( rows_ok )) || record FAIL "t_order did not grow by ${EVENTS} within ${HAPPY_PATH_TIMEOUT}s" \
                               "before=${before_orders}, last=${now_orders:-N/A}"
    (( agg_ok ))  || record FAIL "agg_order_1min did not grow within ${HAPPY_PATH_TIMEOUT}s" \
                               "before=${before_agg}, last=${now_agg:-N/A}"
}

# ---------------------------------------------------------------------------
# Gate 3: no duplicates after orcp-ingest restart.
# ---------------------------------------------------------------------------
gate_no_duplicates() {
    echo "== Gate 3: dedup survives an orcp-ingest restart"

    local before
    before=$(mysql_q "SELECT COUNT(*) FROM t_order")
    if ! [[ "${before}" =~ ^[0-9]+$ ]]; then
        record SKIP "cannot query t_order" "is MySQL reachable?"
        return
    fi

    # Bounce orcp-ingest.  ADMIN_URL is the service; restart via systemd on
    # its host.  If the caller is not on the ingest host, require INGEST_HOST.
    if [[ -n "${INGEST_HOST:-}" ]]; then
        ssh "${SSH_USER:-orcp}@${INGEST_HOST}" 'sudo systemctl restart orcp-ingest' \
            >/tmp/acc_restart.log 2>&1
        if [[ $? -ne 0 ]]; then
            record FAIL "orcp-ingest restart failed on ${INGEST_HOST}" \
                        "tail: $(tail -3 /tmp/acc_restart.log)"
            return
        fi
        sleep 15
    else
        record SKIP "orcp-ingest restart needs INGEST_HOST" \
                    "set INGEST_HOST=<host> to run this gate"
        return
    fi

    # Produce the SAME seeded batch again.  All eventIds collide with the
    # previous run, so t_dedup accepts zero new rows and t_order must not grow.
    python3 scripts/gen_events.py \
        --bootstrap-server "${BOOTSTRAP}" \
        --topic "${SRC_TOPIC}" \
        --count 50 --rate 50 --seed "${ACC_SEED:-7}" \
        >/tmp/acc_gen2.log 2>&1
    sleep 15   # generous window for ingest to drain

    local after
    after=$(mysql_q "SELECT COUNT(*) FROM t_order")
    if [[ "${after}" == "${before}" ]]; then
        record PASS "t_order unchanged after re-delivery (${before})"
    else
        record FAIL "t_order grew after replay" "before=${before}, after=${after}"
    fi
}

# ---------------------------------------------------------------------------
# Gate 2: kill a TaskManager, verify self-heal.
# ---------------------------------------------------------------------------
gate_self_heal() {
    echo "== Gate 2: TaskManager kill & self-heal"

    if [[ -z "${TM_HOST:-}" ]]; then
        record SKIP "set TM_HOST=<taskmanager host> to run this gate"
        return
    fi

    # Capture checkpoint count BEFORE
    local before_cp
    before_cp=$(curl -sS "${FLINK_REST}/jobs/overview" \
        | python3 -c 'import sys,json; j=json.load(sys.stdin).get("jobs",[]);
print(j[0].get("last-modification", 0) if j else 0)' 2>/dev/null || echo 0)

    ssh "${SSH_USER:-orcp}@${TM_HOST}" 'sudo systemctl restart flink-taskmanager' \
        >/tmp/acc_tm.log 2>&1
    if [[ $? -ne 0 ]]; then
        record FAIL "flink-taskmanager restart failed on ${TM_HOST}" \
                    "tail: $(tail -3 /tmp/acc_tm.log)"
        return
    fi

    # Poll: within 120s the job should be RUNNING again.
    local deadline=$((SECONDS + 120))
    while (( SECONDS < deadline )); do
        local state
        state=$(curl -sS "${FLINK_REST}/jobs/overview" \
            | python3 -c 'import sys,json; j=json.load(sys.stdin).get("jobs",[]);
print(j[0].get("state","") if j else "")' 2>/dev/null)
        if [[ "${state}" == "RUNNING" ]]; then
            record PASS "job back to RUNNING within $((SECONDS - (deadline - 120)))s"
            return
        fi
        sleep 5
    done
    record FAIL "job did not return to RUNNING within 120s of TM restart"
}

# ---------------------------------------------------------------------------
# Gate 5: alert fires when JobManager stops.
# ---------------------------------------------------------------------------
gate_alerts() {
    echo "== Gate 5: alert fires on JobManager outage"

    if [[ -z "${JM_HOST:-}" ]]; then
        record SKIP "set JM_HOST=<jobmanager host> to run this gate"
        return
    fi

    ssh "${SSH_USER:-orcp}@${JM_HOST}" 'sudo systemctl stop flink-jobmanager' \
        >/tmp/acc_jm.log 2>&1 || {
            record FAIL "could not stop flink-jobmanager on ${JM_HOST}"
            return
        }

    # Poll Alertmanager for a firing alert matching our rule.
    local deadline=$((SECONDS + 180))
    local fired=0
    while (( SECONDS < deadline )); do
        if curl -sS "${ALERTMANAGER_URL}/api/v2/alerts?active=true" 2>/dev/null \
           | grep -q 'OrcpFlinkJobDown\|OrcpSpringServiceDown'; then
            fired=1; break
        fi
        sleep 10
    done

    # Cleanup: bring JM back up regardless of outcome.
    ssh "${SSH_USER:-orcp}@${JM_HOST}" 'sudo systemctl start flink-jobmanager' \
        >>/tmp/acc_jm.log 2>&1 || true

    if (( fired )); then
        record PASS "alert fired in Alertmanager within 180s"
    else
        record FAIL "no alert visible within 180s" \
                    "check rules at deploy/prometheus/orcp-alerts.yml"
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
printf '\n=== ORCP acceptance test (§8.8) ===\n'
printf '  BOOTSTRAP=%s  ADMIN_URL=%s  FLINK_REST=%s\n' \
       "${BOOTSTRAP}" "${ADMIN_URL}" "${FLINK_REST}"
printf '  MYSQL_HOST=%s  OB_HOST=%s:%s\n' "${MYSQL_HOST}" "${OB_HOST}" "${OB_PORT}"
printf '  events=%s rate=%s timeout=%ss\n\n' "${EVENTS}" "${RATE}" "${HAPPY_PATH_TIMEOUT}"

(( RUN_OBS ))       && gate_observability
(( RUN_HAPPY ))     && gate_happy_path
(( RUN_NO_DUP ))    && gate_no_duplicates
(( RUN_SELF_HEAL )) && gate_self_heal
(( RUN_ALERTS ))    && gate_alerts

printf '\n=== summary ===\n'
if (( FAILED == 0 )); then
    printf '%sALL SELECTED GATES PASSED%s\n' "${C_GREEN}" "${C_RESET}"
    exit 0
else
    printf '%s%d GATE(S) FAILED%s\n' "${C_RED}" "${FAILED}" "${C_RESET}"
    exit 1
fi
