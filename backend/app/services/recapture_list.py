"""补采清单服务：按已确认归集范围核对失败排程，生成真实缺口清单。

批量选中的失败排程先与已确认采集批次（按上游来源 + 业务日期）
核对：已被覆盖的范围不进清单，真正缺的采集数据才生成补采条目。

去重规则：补采范围键是 (来源分片, 业务日期)。共用范围的有效归集
只能计一次——多个排程指向同一来源同一日期时，清单只保留一条
补采条目，但条目会记住所有关联排程；不同业务日期即使同名分片
也各自独立成条，分别保留自己的数据日期与核对结果。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService

# 注意：服务注册表在本模块之后才完成装配，service_for 一律在方法内延迟导入，
# 避免与 app.services 包初始化形成循环导入。


def _service_for(module: str):
    from app.services import service_for
    return service_for(module)

# 补采条目进入终态/挂起后不再占待派发名额
_CLOSED_RESULTS = {"已覆盖", "已确认", "已中断"}


class RecaptureListService(BaseModuleService):
    MODULE = "recapture_list"
    ENTITY = "补采清单"
    OPERATION_TERM = "核对"
    LIST_FIELDS = [
        "清单编号", "来源分片", "业务日期", "关联排程", "缺口说明",
        "已核结果", "批次回执", "占用任务", "最近中断", "生成时间",
    ]
    REQUIRED_FIELDS = ["清单编号", "来源分片", "业务日期"]
    STATUS_ORDER = ["待补采", "补采中", "已确认", "已覆盖", "已中断"]
    ACTION_RULES = {
        "领用补采": "补采中",
        "确认补采": "已确认",
        "标记覆盖": "已覆盖",
        "中断补采": "已中断",
        "重新打开": "待补采",
    }
    NEGATIVE_ACTIONS = ["中断补采"]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """补采条目进入已确认/已覆盖/已中断后不再计入待处理。"""
        entry, message = super().run_action(entry_id, action)
        if entry is not None:
            # 重新打开后已核结果仍是旧终态，以状态机结果为准重算待处理标记
            entry["pending"] = entry["status"] not in _CLOSED_RESULTS
        return entry, message

    def _now(self) -> str:
        return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")

    def build_from_plans(self, plan_ids: list[int]) -> dict[str, Any]:
        """批量核对失败排程，对照已确认范围生成补采清单。

        - 已确认批次精确按 (来源分片, 业务日期) 命中即视为已覆盖，
          同名分片但日期不同不构成覆盖；
        - 同一 (来源, 日期) 的多个排程折叠为一条补采条目（有效归集
          只计一次），关联排程全部留存，每个排程各自回写核对账本；
        - 未覆盖的日期分别生成条目，可分别补采。
        """
        plan_svc = _service_for("schedule_plan")
        batch_svc = _service_for("capture_batch")
        if plan_svc is None or batch_svc is None:
            return {"ok": False, "reason": "依赖服务未注册"}

        rows = self.list_entries(size=10000)[0]
        next_seq = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        index: dict[tuple[str, str], dict[str, Any]] = {}
        covered_plans: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        # 幂等：已存在且未关闭的同范围条目直接复用，重复核对不产生重复缺口
        for existing in rows:
            if existing.get("已核结果") in _CLOSED_RESULTS:
                continue
            key = (str(existing.get("来源分片", "")), str(existing.get("业务日期", "")))
            index.setdefault(key, existing)

        for plan_id in plan_ids:
            plan = plan_svc.get_entry(int(plan_id))
            if plan is None:
                skipped.append({"plan_id": plan_id, "reason": "排程不存在"})
                continue
            source = str(plan.get("来源分片", "")).strip()
            biz_date = str(plan.get("业务日期", "")).strip()
            if not source or not biz_date:
                skipped.append({"plan_id": plan_id, "reason": "排程缺少来源分片或业务日期"})
                continue

            coverage = batch_svc.find_coverage(source, biz_date)
            if coverage is not None:
                # 已确认范围命中：该排程不需要补采，回执精确到本日期
                plan_svc.record_verification(
                    plan["id"],
                    result="已覆盖",
                    receipt=str(coverage.get("批次编号", "")),
                    batch_id=coverage.get("id"),
                )
                covered_plans.append({
                    "plan_id": plan["id"],
                    "来源分片": source,
                    "业务日期": biz_date,
                    "批次回执": coverage.get("批次编号", ""),
                })
                continue

            # 真正的缺口：同范围只建一条，关联排程各自留存
            key = (source, biz_date)
            if key in index:
                item = index[key]
                linked = item.setdefault("_关联排程ids", [])
                if plan["id"] not in linked:
                    linked.append(plan["id"])
                    item["关联排程"] = "、".join(
                        plan_svc.get_entry(pid).get("计划编号", str(pid)) for pid in linked
                    )
                plan_svc.mark_collection_failed(
                    plan["id"], plan.get("失败原因") or f"{source}分片{biz_date}采集失败待补采"
                )
                continue

            plan_svc.mark_collection_failed(
                plan["id"], plan.get("失败原因") or f"{source}分片{biz_date}采集失败待补采"
            )
            values = {
                "清单编号": f"RCAP-{next_seq:04d}",
                "来源分片": source,
                "业务日期": biz_date,
                "关联排程": plan.get("计划编号", str(plan["id"])),
                "缺口说明": f"来源{source}在{biz_date}的采集数据未确认，需补采",
                "已核结果": "待补采",
                "批次回执": "",
                "占用任务": "",
                "最近中断": "",
                "生成时间": self._now(),
            }
            entry, _ = self.create_entry(values)
            entry["_关联排程ids"] = [plan["id"]]
            index[key] = entry
            next_seq += 1

        return {
            "ok": True,
            "created": list(index.values()),
            "gap_count": len(index),
            "covered": covered_plans,
            "skipped": skipped,
        }

    def get_open_items(self) -> list[dict[str, Any]]:
        """返回仍可派发的补采条目（待补采，或中断后重新打开）。"""
        items, _ = self.list_entries(size=10000)
        return [
            it for it in items
            if it.get("已核结果") not in _CLOSED_RESULTS
        ]

    def reopen_item(self, item_id: int, reason: str = "") -> dict[str, Any]:
        """中断接续：把已中断条目重新打开，已确认数据与失败原因保留可查。"""
        entry = self.get_entry(item_id)
        if entry is None:
            return {"ok": False, "reason": "补采条目不存在"}
        if entry.get("status") != "已中断":
            return {"ok": False, "reason": "仅已中断的补采条目可重新打开"}
        entry, _ = self.run_action(item_id, "重新打开")
        entry["已核结果"] = "待补采"
        entry["占用任务"] = ""
        entry["最近中断"] = reason or entry.get("最近中断", "")
        return {"ok": True, "entry": entry}
