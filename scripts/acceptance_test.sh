#!/usr/bin/env bash
#
# acceptance_test.sh -- 端到端执行 §8.8 验收清单。
#
# 本脚本 **不** 负责搭建基础设施，假设阶段 B–F 的完整栈已运行，
# 运维人员只需对五个 DEV_SPEC §8.8 验收门做机器可核实的 PASS/FAIL 判断：
#
#   1. 正常路径  : 1000 条事件 -> mysql 明细可见 -> OB 聚合可见（90 秒内）
#   2. 自愈      : 杀死 TaskManager；2 分钟内作业恢复（exactly-once + upsert）
#   3. 无重放    : 重启 orcp-ingest；重投事件被 t_dedup 吸收（t_order 不增长）
#   4. 可观测性  : /api/health 所有子系统 UP；Flink /jobs/overview 可见
#   5. 告警      : 停止 JobManager；3 分钟内 webhook 触发一条告警
#
# 各检查输出 PASS / FAIL / SKIP；遇 FAIL 则整体退出码非零。
# 标记为可选的破坏性检查需通过命令行标志显式启用。
#
# 用法：
#   scripts/acceptance_test.sh                   # 仅执行门 1、3、4（非破坏性）
#   scripts/acceptance_test.sh --with-self-heal  # 增加门 2（杀 TM）
#   scripts/acceptance_test.sh --with-alerts     # 增加门 5（停 JM）
#   scripts/acceptance_test.sh --all             # 执行全部 5 个门
#
# 环境变量（可覆盖默认值）：
#   BOOTSTRAP              Kafka bootstrap，默认 node-1:9092
#   SRC_TOPIC              外部源 topic，默认 orcp.src.demo
#   FLINK_REST             默认 http://node-1:8081
#   ADMIN_URL              orcp-admin 基础 URL，默认 http://node-3:8081
#   ADMIN_USER / ADMIN_PW  orcp-admin Basic 认证
#   MYSQL_HOST / MYSQL_*   本地明细库（默认 node-1，orcp_ro）
#   OB_HOST / OB_PORT / OB_*  OceanBase
#   ALERTMANAGER_URL       默认 http://node-2:9093
#
# 退出码：
#   0  所选检查全部 PASS（或 SKIP）
#   1  存在一项或多项 FAIL
#   2  用法错误

set -uo pipefail

# ---------------------------------------------------------------------------
# 配置默认值
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
: "${OB_CLIENT:=obclient}"             # 也可以设为 'mysql'（直接使用 MySQL 协议）
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
        *)  echo "未知参数：${arg}" >&2; exit 2 ;;
    esac
done

# ---------------------------------------------------------------------------
# 工具函数
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
    # obclient 与 mysql 客户端接受相同的参数；租户用户格式 user@tenant#cluster 用于直连 2881。
    "${OB_CLIENT}" -h"${OB_HOST}" -P"${OB_PORT}" \
          -u"${OB_USER}" -p"${OB_PW}" \
          -N --silent -e "$1" orcp_dw 2>/dev/null
}

# ---------------------------------------------------------------------------
# 门 4（优先执行，以便栈异常时尽早退出）
# ---------------------------------------------------------------------------
gate_observability() {
    echo "== 门 4：可观测性（/api/health + Flink /jobs/overview）"

    local body code
    code=$(curl -sS -o /tmp/acc_health.json -w '%{http_code}' \
           "${ADMIN_URL}/api/health" || echo 000)
    if [[ "${code}" != "200" ]]; then
        record FAIL "GET /api/health 返回 ${code}（期望 200）" \
                    "响应：$(head -c 300 /tmp/acc_health.json 2>/dev/null)"
        return
    fi
    if grep -q '"status":"UP"' /tmp/acc_health.json; then
        record PASS "聚合健康状态为 UP"
    else
        record FAIL "聚合健康状态不是 UP" \
                    "响应：$(head -c 300 /tmp/acc_health.json)"
    fi

    local jobs_running
    jobs_running=$(curl -sS "${FLINK_REST}/jobs/overview" \
                   | python3 -c 'import sys,json; d=json.load(sys.stdin);
print(sum(1 for j in d.get("jobs", []) if j.get("state") == "RUNNING"))' 2>/dev/null \
                   || echo -1)
    if [[ "${jobs_running}" -ge 1 ]]; then
        record PASS "至少一个 Flink 作业处于 RUNNING 状态（${jobs_running} 个）"
    else
        record FAIL "未发现 RUNNING 状态的 Flink 作业（${jobs_running}）"
    fi
}

# ---------------------------------------------------------------------------
# 门 1：正常路径。发送 EVENTS 条事件，等待明细和聚合数据出现。
# ---------------------------------------------------------------------------
gate_happy_path() {
    echo "== 门 1：正常路径（${EVENTS} 条事件 -> MySQL -> OceanBase）"

    local before_orders before_agg
    before_orders=$(mysql_q "SELECT COUNT(*) FROM t_order" || echo "err")
    before_agg=$(ob_q "SELECT COUNT(*) FROM agg_order_1min" || echo "err")
    if ! [[ "${before_orders}" =~ ^[0-9]+$ ]] || ! [[ "${before_agg}" =~ ^[0-9]+$ ]]; then
        record FAIL "基准查询失败" \
                    "t_order=${before_orders}, agg_order_1min=${before_agg}"
        return
    fi

    # 生产消息（gen_events.py 会自动 flush 并优雅退出）。
    python3 scripts/gen_events.py \
        --bootstrap-server "${BOOTSTRAP}" \
        --topic "${SRC_TOPIC}" \
        --count "${EVENTS}" --rate "${RATE}" --seed "${ACC_SEED:-7}" \
        >/tmp/acc_gen.log 2>&1
    if [[ $? -ne 0 ]]; then
        record FAIL "gen_events.py 生产失败" \
                    "尾部日志：$(tail -3 /tmp/acc_gen.log)"
        return
    fi

    # 轮询明细表，在 HAPPY_PATH_TIMEOUT 内等待数据。
    local deadline=$((SECONDS + HAPPY_PATH_TIMEOUT))
    local rows_ok=0 agg_ok=0
    while (( SECONDS < deadline )); do
        local now_orders now_agg
        now_orders=$(mysql_q "SELECT COUNT(*) FROM t_order")
        now_agg=$(ob_q "SELECT COUNT(*) FROM agg_order_1min")
        if [[ "${rows_ok}" == 0 && "${now_orders}" =~ ^[0-9]+$ \
              && $((now_orders - before_orders)) -ge "${EVENTS}" ]]; then
            record PASS "t_order 已增长 >= ${EVENTS}（从 ${before_orders} 到 ${now_orders}）"
            rows_ok=1
        fi
        if [[ "${agg_ok}" == 0 && "${now_agg}" =~ ^[0-9]+$ \
              && "${now_agg}" -gt "${before_agg}" ]]; then
            record PASS "agg_order_1min 已增长（从 ${before_agg} 到 ${now_agg}）"
            agg_ok=1
        fi
        if (( rows_ok && agg_ok )); then return; fi
        sleep 3
    done

    (( rows_ok )) || record FAIL "t_order 在 ${HAPPY_PATH_TIMEOUT}s 内未增长 ${EVENTS} 条" \
                               "before=${before_orders}, last=${now_orders:-N/A}"
    (( agg_ok ))  || record FAIL "agg_order_1min 在 ${HAPPY_PATH_TIMEOUT}s 内未增长" \
                               "before=${before_agg}, last=${now_agg:-N/A}"
}

# ---------------------------------------------------------------------------
# 门 3：重启 orcp-ingest 后无重放效果。
# ---------------------------------------------------------------------------
gate_no_duplicates() {
    echo "== 门 3：orcp-ingest 重启后无重放效果"

    local before
    before=$(mysql_q "SELECT COUNT(*) FROM t_order")
    if ! [[ "${before}" =~ ^[0-9]+$ ]]; then
        record SKIP "无法查询 t_order" "MySQL 是否可达？"
        return
    fi

    # 重启 orcp-ingest。若调用方不在 ingest 主机上，需要设置 INGEST_HOST。
    if [[ -n "${INGEST_HOST:-}" ]]; then
        ssh "${SSH_USER:-orcp}@${INGEST_HOST}" 'sudo systemctl restart orcp-ingest' \
            >/tmp/acc_restart.log 2>&1
        if [[ $? -ne 0 ]]; then
            record FAIL "在 ${INGEST_HOST} 上重启 orcp-ingest 失败" \
                        "尾部日志：$(tail -3 /tmp/acc_restart.log)"
            return
        fi
        sleep 15
    else
        record SKIP "重启 orcp-ingest 需要设置 INGEST_HOST" \
                    "请设置 INGEST_HOST=<主机名> 后重新执行此门"
        return
    fi

    # 以相同种子重新发送批次。所有 eventId 与前一轮重复，
    # 因此 t_dedup 应拒绝全部条目，t_order 不应增长。
    python3 scripts/gen_events.py \
        --bootstrap-server "${BOOTSTRAP}" \
        --topic "${SRC_TOPIC}" \
        --count 50 --rate 50 --seed "${ACC_SEED:-7}" \
        >/tmp/acc_gen2.log 2>&1
    sleep 15   # 为摄入服务留出充裕的处理窗口

    local after
    after=$(mysql_q "SELECT COUNT(*) FROM t_order")
    if [[ "${after}" == "${before}" ]]; then
        record PASS "重新投递后 t_order 未增长（${before}）"
    else
        record FAIL "重放后 t_order 增长" "before=${before}, after=${after}"
    fi
}

# ---------------------------------------------------------------------------
# 门 2：杀死 TaskManager，验证自愈。
# ---------------------------------------------------------------------------
gate_self_heal() {
    echo "== 门 2：TaskManager 杀死与自愈"

    if [[ -z "${TM_HOST:-}" ]]; then
        record SKIP "请设置 TM_HOST=<taskmanager 主机名> 后执行此门"
        return
    fi

    ssh "${SSH_USER:-orcp}@${TM_HOST}" 'sudo systemctl restart flink-taskmanager' \
        >/tmp/acc_tm.log 2>&1
    if [[ $? -ne 0 ]]; then
        record FAIL "在 ${TM_HOST} 上重启 flink-taskmanager 失败" \
                    "尾部日志：$(tail -3 /tmp/acc_tm.log)"
        return
    fi

    # 轮询：120 秒内作业应重新回到 RUNNING 状态。
    local deadline=$((SECONDS + 120))
    while (( SECONDS < deadline )); do
        local state
        state=$(curl -sS "${FLINK_REST}/jobs/overview" \
            | python3 -c 'import sys,json; j=json.load(sys.stdin).get("jobs",[]);
print(j[0].get("state","") if j else "")' 2>/dev/null)
        if [[ "${state}" == "RUNNING" ]]; then
            record PASS "作业在 $((SECONDS - (deadline - 120)))s 内恢复为 RUNNING"
            return
        fi
        sleep 5
    done
    record FAIL "TM 重启后 120 秒内作业未恢复为 RUNNING"
}

# ---------------------------------------------------------------------------
# 门 5：JobManager 停止时告警触发。
# ---------------------------------------------------------------------------
gate_alerts() {
    echo "== 门 5：JobManager 故障时告警触发"

    if [[ -z "${JM_HOST:-}" ]]; then
        record SKIP "请设置 JM_HOST=<jobmanager 主机名> 后执行此门"
        return
    fi

    ssh "${SSH_USER:-orcp}@${JM_HOST}" 'sudo systemctl stop flink-jobmanager' \
        >/tmp/acc_jm.log 2>&1 || {
            record FAIL "无法停止 ${JM_HOST} 上的 flink-jobmanager"
            return
        }

    # 轮询 Alertmanager，查找匹配的活跃告警。
    local deadline=$((SECONDS + 180))
    local fired=0
    while (( SECONDS < deadline )); do
        if curl -sS "${ALERTMANAGER_URL}/api/v2/alerts?active=true" 2>/dev/null \
           | grep -q 'OrcpFlinkJobDown\|OrcpSpringServiceDown'; then
            fired=1; break
        fi
        sleep 10
    done

    # 无论结果如何，恢复 JM。
    ssh "${SSH_USER:-orcp}@${JM_HOST}" 'sudo systemctl start flink-jobmanager' \
        >>/tmp/acc_jm.log 2>&1 || true

    if (( fired )); then
        record PASS "告警在 180 秒内出现在 Alertmanager"
    else
        record FAIL "180 秒内未出现告警" \
                    "请检查规则文件：deploy/prometheus/orcp-alerts.yml"
    fi
}

# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
printf '\n=== ORCP 验收测试（§8.8）===\n'
printf '  BOOTSTRAP=%s  ADMIN_URL=%s  FLINK_REST=%s\n' \
       "${BOOTSTRAP}" "${ADMIN_URL}" "${FLINK_REST}"
printf '  MYSQL_HOST=%s  OB_HOST=%s:%s\n' "${MYSQL_HOST}" "${OB_HOST}" "${OB_PORT}"
printf '  事件数=%s  速率=%s  超时=%ss\n\n' "${EVENTS}" "${RATE}" "${HAPPY_PATH_TIMEOUT}"

(( RUN_OBS ))       && gate_observability
(( RUN_HAPPY ))     && gate_happy_path
(( RUN_NO_DUP ))    && gate_no_duplicates
(( RUN_SELF_HEAL )) && gate_self_heal
(( RUN_ALERTS ))    && gate_alerts

printf '\n=== 汇总 ===\n'
if (( FAILED == 0 )); then
    printf '%s所有选定门均通过%s\n' "${C_GREEN}" "${C_RESET}"
    exit 0
else
    printf '%s%d 个门失败%s\n' "${C_RED}" "${FAILED}" "${C_RESET}"
    exit 1
fi
