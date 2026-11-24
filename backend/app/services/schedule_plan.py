"""排程计划服务：周期性排程、时区管理与任务投递。

维护流程的周期性排程、时区设定和生效区间，触发时向任务
队列投递排队任务。支持迟到数据防护和并发上限控制。

补采核对相关字段挂在每条排程自己的记录上：来源分片、业务日期、
失败原因、已核结果与批次回执，保证中断接续后每个排程仍保有
自己的数据日期和核对结论，可逐条追溯到批次回执。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


class SchedulePlanService(BaseModuleService):
    MODULE = "schedule_plan"
    ENTITY = "排程计划"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "计划编号", "关联模板", "触发周期", "时区设定", "生效起日",
        "生效止日", "并发上限", "下次触发",
        # 补采核对账本字段：每条排程各自留存，不被同名分片的其他排程覆盖
        "来源分片", "业务日期", "失败原因", "已核结果", "批次回执", "核对时间",
    ]
    REQUIRED_FIELDS = ["计划编号", "关联模板", "触发周期"]
    STATUS_ORDER = ["待生效", "生效中", "已暂停", "已过期"]
    ACTION_RULES = {"启效排程": "生效中", "暂停排程": "已暂停", "终止排程": "已过期"}
    NEGATIVE_ACTIONS = []

    def check_late_arrival(self, plan_id: int) -> dict[str, Any]:
        """检查排程是否发生迟到数据（生效止日已过）。"""
        entry = self.get_entry(plan_id)
        if entry is None:
            return {"ok": False, "reason": "排程计划不存在"}
        end_date = entry.get("生效止日", "")
        if not end_date:
            return {"ok": True, "late": False}
        now = datetime.now(timezone(timedelta(hours=8))).isoformat()[:10]
        if end_date < now:
            self.run_action(plan_id, "终止排程")
            return {"ok": True, "late": True, "reason": f"生效止日{end_date}已过期，迟到数据拒绝"}
        return {"ok": True, "late": False}

    def get_active_plans(self) -> list[dict[str, Any]]:
        """获取生效中的排程计划列表。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("status") == "生效中"]

    # ── 补采核对账本 ─────────────────────────────────────────────

    def mark_collection_failed(self, plan_id: int, reason: str) -> dict[str, Any]:
        """登记排程的采集失败原因，失败原因在后续接续中始终保留可查。"""
        entry = self.get_entry(plan_id)
        if entry is None:
            return {"ok": False, "reason": "排程计划不存在"}
        entry["失败原因"] = reason
        if not entry.get("已核结果"):
            entry["已核结果"] = "待核对"
        return {"ok": True, "plan_id": plan_id, "失败原因": reason}

    def record_verification(
        self,
        plan_id: int,
        *,
        result: str,
        receipt: str = "",
        batch_id: int | None = None,
        checked_at: str | None = None,
    ) -> dict[str, Any]:
        """把核对结果回写到单条排程，只改这一条，保留它自己的业务日期。

        同名来源分片下不同业务日期的排程互不影响：回写键是排程 id，
        调用方按「来源分片 + 业务日期」定位到具体排程后逐条写入。
        """
        entry = self.get_entry(plan_id)
        if entry is None:
            return {"ok": False, "reason": f"排程计划 {plan_id} 不存在"}
        entry["已核结果"] = result
        if receipt:
            entry["批次回执"] = receipt
        if batch_id is not None:
            entry["_归集批次id"] = batch_id
        entry["核对时间"] = checked_at or datetime.now(
            timezone(timedelta(hours=8))
        ).isoformat(timespec="seconds")
        return {"ok": True, "plan_id": plan_id, "entry": entry}

    def get_verification_book(self, plan_ids: list[int] | None = None) -> list[dict[str, Any]]:
        """输出每条排程自己的核对账本：数据日期、已核结果、回执与失败原因。"""
        items, _ = self.list_entries(size=10000)
        wanted = set(plan_ids or [])
        book = []
        for it in items:
            if wanted and int(it.get("id", 0)) not in wanted:
                continue
            if not it.get("来源分片"):
                continue
            book.append({
                "plan_id": it["id"],
                "计划编号": it.get("计划编号", ""),
                "来源分片": it.get("来源分片", ""),
                "业务日期": it.get("业务日期", ""),
                "已核结果": it.get("已核结果", "待核对"),
                "批次回执": it.get("批次回执", ""),
                "失败原因": it.get("失败原因", ""),
                "核对时间": it.get("核对时间", ""),
            })
        return book
