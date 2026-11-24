"""链路11 补采核对链路测试：失败排程批量核对 → 补采清单 → 任务队列领用
→ 迟到回执 → 中断接续 → 按来源与业务日期归集确认回写。

核心断言：
- 已确认范围按「来源分片 + 业务日期」精确判定，同名分片不判整组完成；
- 共用范围的有效归集只计一次，每个排程保留自己的数据日期与已核结果；
- 迟到回执只覆盖它携带的日期，该日期不得复采，其他日期仍可独立派发；
- 中断后已确认数据与失败原因均可查，可再次提交。
"""
from __future__ import annotations

from app.pipeline import (
    build_recapture_plan,
    claim_recapture_attempt,
    confirm_recapture_attempt,
    interrupt_recapture_attempt,
    register_late_receipt,
    resume_recapture_attempt,
)
from app.services import service_for


def _make_plan(code: str, source: str, biz_date: str, reason: str = "采集失败") -> int:
    """登记一条失败排程，携带自己的来源分片、业务日期与失败原因。"""
    svc = service_for("schedule_plan")
    entry, _ = svc.create_entry({
        "计划编号": code,
        "关联模板": f"FLOW-{code}",
        "触发周期": "daily",
        "来源分片": source,
        "业务日期": biz_date,
        "失败原因": reason,
    })
    svc.run_action(entry["id"], "启效排程")
    return entry["id"]


def _plan_book(plan_id: int) -> dict:
    book = {b["plan_id"]: b for b in service_for("schedule_plan").get_verification_book()}
    return book[plan_id]


class TestRecaptureGapBuild:
    """批量核对：从已确认范围按来源筛出真正缺口。"""

    def test_builds_one_item_per_source_date_pair(self):
        p1 = _make_plan("SCHE-L20", "L", "2026-11-20")
        p2 = _make_plan("SCHE-L21", "L", "2026-11-21")
        p3 = _make_plan("SCHE-M21", "M", "2026-11-21")

        result = build_recapture_plan([p1, p2, p3])
        assert result["status"] == "planned"
        # 同名分片 L 的两个日期各自成条，共三个真实缺口
        assert result["gap_count"] == 3
        items = service_for("recapture_list").list_entries(size=1000)[0]
        pairs = sorted((it["来源分片"], it["业务日期"]) for it in items)
        assert pairs == [("L", "2026-11-20"), ("L", "2026-11-21"), ("M", "2026-11-21")]
        # 每个排程保留自己的失败原因与待核对账本
        for pid in (p1, p2, p3):
            book = _plan_book(pid)
            assert book["已核结果"] == "待核对"
            assert book["失败原因"] == "采集失败"

    def test_shared_scope_counts_once_but_keeps_every_plan(self):
        p1 = _make_plan("SCHE-L20-A", "L", "2026-11-20")
        p4 = _make_plan("SCHE-L20-B", "L", "2026-11-20")

        result = build_recapture_plan([p1, p4])
        assert result["gap_count"] == 1  # 有效归集只计一次
        item = service_for("recapture_list").list_entries(size=1000)[0][0]
        assert set(item["_关联排程ids"]) == {p1, p4}
        assert "SCHE-L20-A" in item["关联排程"] and "SCHE-L20-B" in item["关联排程"]

    def test_rebuild_is_idempotent_for_open_gaps(self):
        p1 = _make_plan("SCHE-L20", "L", "2026-11-20")
        first = build_recapture_plan([p1])
        second = build_recapture_plan([p1])
        assert first["gap_count"] == 1 and second["gap_count"] == 1
        items = service_for("recapture_list").list_entries(size=1000)[0]
        assert len(items) == 1  # 重复核对不产生重复缺口

    def test_claim_rejects_item_already_in_progress(self):
        p1 = _make_plan("SCHE-L20", "L", "2026-11-20")
        build = build_recapture_plan([p1])
        item_id = build["item_ids"][0]
        first = claim_recapture_attempt([item_id], "ROBO-0002")
        second = claim_recapture_attempt([item_id], "ROBO-0002")
        assert len(first["tasks"]) == 1
        assert all("补采中" in b["reason"] for b in second["blocked"])
        assert second["tasks"] == []

    def test_confirmed_scope_is_filtered_out_by_exact_date(self):
        p1 = _make_plan("SCHE-L20", "L", "2026-11-20")
        p2 = _make_plan("SCHE-L21", "L", "2026-11-21")
        # 已确认范围里只有 L/20；L/21 仍是缺口
        service_for("capture_batch").accept_late_receipt(
            source="L", biz_date="2026-11-20", checksum="seed-l20", late=False
        )

        result = build_recapture_plan([p1, p2])
        assert result["gap_count"] == 1
        only = service_for("recapture_list").list_entries(size=1000)[0][0]
        assert (only["来源分片"], only["业务日期"]) == ("L", "2026-11-21")
        # 被覆盖的排程直接回写已覆盖与回执，不凭同名分片波及 21 日
        assert _plan_book(p1)["已核结果"] == "已覆盖"
        assert _plan_book(p1)["批次回执"]
        assert _plan_book(p2)["已核结果"] == "待核对"


class TestLateReceiptAndDispatch:
    """迟到回执只证明较早日期：20 日不得复采，21 日独立可派发。"""

    def test_full_late_receipt_scenario(self):
        p1 = _make_plan("SCHE-L20", "L", "2026-11-20")
        p2 = _make_plan("SCHE-L21", "L", "2026-11-21")
        p3 = _make_plan("SCHE-M21", "M", "2026-11-21")
        build = build_recapture_plan([p1, p2, p3])
        item_ids = build["item_ids"]

        # 系统领用整组尝试，三个补采任务全部派发到在线节点
        claim = claim_recapture_attempt(item_ids, "ROBO-0002")
        assert claim["status"] == "claimed"
        assert len(claim["tasks"]) == 3
        queue_svc = service_for("task_queue")
        tasks = {
            (t["来源分片"], t["业务日期"]): t
            for t in queue_svc.list_entries(size=1000)[0]
            if t.get("来源分片")
        }
        assert all(t["status"] == "已派发" for t in tasks.values())

        # 旧机器人的 L 回执迟到，只能证明 20 日已完成
        late = register_late_receipt("L", "2026-11-20", "late-L-20")
        assert late["status"] == "late_received"
        late_batch = late["批次回执"]
        assert late["affected_tasks"] == [tasks[("L", "2026-11-20")]["id"]]

        # 20 日任务被回收，21 日 L 与 M 的原尝试继续保留
        assert queue_svc.get_entry(tasks[("L", "2026-11-20")]["id"])["status"] == "已消费"
        assert queue_svc.get_entry(tasks[("L", "2026-11-21")]["id"])["status"] == "已派发"
        assert queue_svc.get_entry(tasks[("M", "2026-11-21")]["id"])["status"] == "已派发"

        # 20 日不得复采：新任务派发被已确认范围拦截
        guard_task = queue_svc.enqueue_recapture_task(
            source="L", biz_date="2026-11-20", recapture_item_id=item_ids[0]
        )
        guard = queue_svc.dispatch_to_node(guard_task["id"], "ROBO-0002")
        assert guard["ok"] is False and guard["blocked"] is True
        assert queue_svc.get_entry(guard_task["id"])["status"] == "已消费"

        # 21 日仍有独立派发机会：归集确认只回写 L/21 那条排程
        confirm21 = confirm_recapture_attempt(tasks[("L", "2026-11-21")]["id"])
        assert confirm21["status"] == "confirmed"
        assert confirm21["批次回执"] != late_batch
        assert _plan_book(p2)["已核结果"] == "已确认"
        assert _plan_book(p2)["业务日期"] == "2026-11-21"
        # 迟到回执没有凭同名分片把 21 日判成完成
        assert _plan_book(p1)["已核结果"] == "已覆盖"
        assert _plan_book(p1)["批次回执"] == late_batch

    def test_late_receipt_is_idempotent_and_scope_counts_once(self):
        _make_plan("SCHE-L20", "L", "2026-11-20")
        first = register_late_receipt("L", "2026-11-20", "late-x")
        second = register_late_receipt("L", "2026-11-20", "late-x")
        assert second["deduplicated"] is True
        assert second["batch_id"] == first["batch_id"]
        scope = service_for("capture_batch").confirmed_scope()
        assert ("L", "2026-11-20") in scope
        # 集合天然折叠：同范围只计一次
        assert sum(1 for k in scope if k == ("L", "2026-11-20")) == 1


class TestInterruptAndResume:
    """M 中断后能再次提交；已确认数据与失败原因接续可查。"""

    def test_interrupt_then_resume_then_confirm(self):
        p3 = _make_plan("SCHE-M21", "M", "2026-11-21")
        build = build_recapture_plan([p3])
        item_id = build["item_ids"][0]
        claim = claim_recapture_attempt([item_id], "ROBO-0002")
        task_id = claim["tasks"][0]["task_id"]

        interrupted = interrupt_recapture_attempt(item_id, "节点网络中断")
        assert interrupted["status"] == "interrupted"
        recapture_svc = service_for("recapture_list")
        item = recapture_svc.get_entry(item_id)
        assert item["status"] == "已中断"
        assert item["最近中断"] == "节点网络中断"
        # 失败原因回写排程，中断后仍可查
        assert _plan_book(p3)["失败原因"] == "节点网络中断"
        queue_svc = service_for("task_queue")
        assert queue_svc.get_entry(task_id)["status"] == "待派发"

        # 再次提交：复用原任务并重新派发，历史中断原因保留
        resumed = resume_recapture_attempt(item_id, "ROBO-0002")
        assert resumed["status"] == "resumed"
        assert resumed["task_id"] == task_id
        assert queue_svc.get_entry(task_id)["status"] == "已派发"
        assert queue_svc.get_entry(task_id)["最近中断"] == "节点网络中断"

        confirmed = confirm_recapture_attempt(task_id)
        assert confirmed["status"] == "confirmed"
        assert _plan_book(p3)["已核结果"] == "已确认"
        assert _plan_book(p3)["批次回执"] == confirmed["批次回执"]
        assert recapture_svc.get_entry(item_id)["status"] == "已确认"

    def test_resume_is_blocked_when_late_receipt_covered_the_date(self):
        p1 = _make_plan("SCHE-L20", "L", "2026-11-20")
        build = build_recapture_plan([p1])
        item_id = build["item_ids"][0]
        claim = claim_recapture_attempt([item_id], "ROBO-0002")
        task_id = claim["tasks"][0]["task_id"]

        interrupt_recapture_attempt(item_id, "临时中断")
        register_late_receipt("L", "2026-11-20", "late-L-20")
        # 接续时迟到回执已覆盖该日期，派发防护再次拦截，不复采
        resumed = resume_recapture_attempt(item_id, "ROBO-0002")
        assert resumed["status"] == "blocked"
        assert resumed["批次回执"]
        queue_svc = service_for("task_queue")
        assert queue_svc.get_entry(task_id)["status"] == "已消费"


class TestPerPlanWritebackAndTraceability:
    """各日结果分别追到批次回执，不能仅凭同名分片判整组完成。"""

    def test_each_plan_keeps_own_date_receipt_and_result(self):
        p1 = _make_plan("SCHE-L20", "L", "2026-11-20")
        p2 = _make_plan("SCHE-L21", "L", "2026-11-21")
        p3 = _make_plan("SCHE-M21", "M", "2026-11-21")
        build = build_recapture_plan([p1, p2, p3])
        ids = build["item_ids"]
        claim = claim_recapture_attempt(ids, "ROBO-0002")
        by_pair = {(t["来源分片"], t["业务日期"]): t["task_id"] for t in claim["tasks"]}

        register_late_receipt("L", "2026-11-20", "late-L-20")
        r21 = confirm_recapture_attempt(by_pair[("L", "2026-11-21")])
        rm = confirm_recapture_attempt(by_pair[("M", "2026-11-21")])

        book = {b["plan_id"]: b for b in service_for("schedule_plan").get_verification_book()}
        assert book[p1]["业务日期"] == "2026-11-20"
        assert book[p1]["已核结果"] == "已覆盖"
        assert book[p2]["业务日期"] == "2026-11-21"
        assert book[p2]["已核结果"] == "已确认"
        assert book[p3]["业务日期"] == "2026-11-21"
        assert book[p3]["已核结果"] == "已确认"
        # 三条回执互不相同，各自可追溯
        receipts = {book[p1]["批次回执"], book[p2]["批次回执"], book[p3]["批次回执"]}
        assert len(receipts) == 3
        assert book[p2]["批次回执"] == r21["批次回执"]
        assert book[p3]["批次回执"] == rm["批次回执"]

    def test_shared_scope_confirmation_writes_back_to_every_linked_plan(self):
        p1 = _make_plan("SCHE-L20-A", "L", "2026-11-20")
        p4 = _make_plan("SCHE-L20-B", "L", "2026-11-20")
        build = build_recapture_plan([p1, p4])
        item_id = build["item_ids"][0]
        claim = claim_recapture_attempt([item_id], "ROBO-0002")
        confirm_recapture_attempt(claim["tasks"][0]["task_id"])

        for pid in (p1, p4):
            book = _plan_book(pid)
            assert book["已核结果"] == "已确认"
            assert book["业务日期"] == "2026-11-20"
            assert book["批次回执"]

    def test_audit_trail_records_late_interrupt_and_confirm(self):
        audit_svc = service_for("audit_trail")
        before = len(audit_svc.list_entries(size=10000)[0])
        p1 = _make_plan("SCHE-M21", "M", "2026-11-21")
        build = build_recapture_plan([p1])
        item_id = build["item_ids"][0]
        claim = claim_recapture_attempt([item_id], "ROBO-0002")
        interrupt_recapture_attempt(item_id, "中断留痕")
        resume_recapture_attempt(item_id, "ROBO-0002")
        confirm_recapture_attempt(claim["tasks"][0]["task_id"])

        audits = audit_svc.list_entries(size=10000)[0][before:]
        op_types = {a["操作类型"] for a in audits}
        assert {"补采清单生成", "补采领用", "补采中断", "补采归集确认"} <= op_types
