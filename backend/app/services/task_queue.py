"""任务队列服务：排队顺序管理、资源额度占用与派发回收。

维护待执行任务的排队顺序、优先级和资源额度占用情况，
派发失败时回收入队。支持优先级排序和等待秒数计算。

补采任务额外携带「来源分片 + 业务日期」：派发前先核对已确认
归集范围，已被（含迟到回执）覆盖的日期不得复采；任务队列产生
的归集确认结果按来源与业务日期回写到每条原排程，并沉淀到补采
清单的批次回执上，各日结果可分别追溯。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


class TaskQueueService(BaseModuleService):
    MODULE = "task_queue"
    ENTITY = "排队任务"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "队列编号", "关联模板", "优先级", "期望开始", "重试次数",
        "占用节点", "等待秒数", "入队时间",
        # 补采任务账本：来源分片、业务日期、中断原因与关联补采条目
        "来源分片", "业务日期", "最近中断", "补采条目",
    ]
    REQUIRED_FIELDS = ["队列编号", "关联模板", "优先级"]
    STATUS_ORDER = ["待派发", "已派发", "已消费", "已丢弃"]
    ACTION_RULES = {"派发任务": "已派发", "回收任务": "已消费", "丢弃任务": "已丢弃"}
    NEGATIVE_ACTIONS = []

    _PRIORITY_WEIGHTS = {"high": 3, "medium": 2, "low": 1}

    def get_pending_by_priority(self) -> list[dict[str, Any]]:
        """按优先级排序返回待派发任务。"""
        items, _ = self.list_entries(size=10000)
        pending = [it for it in items if it.get("status") == "待派发"]
        pending.sort(
            key=lambda t: self._PRIORITY_WEIGHTS.get(
                str(t.get("优先级", "low")).lower(), 1
            ),
            reverse=True,
        )
        return pending

    def recycle_failed_dispatches(self) -> dict[str, Any]:
        """回收派发失败的任务（已派发但被标记为待派发）。"""
        items, _ = self.list_entries(size=10000)
        recycled = 0
        for it in items:
            if it.get("status") == "已派发" and not it.get("占用节点"):
                self.run_action(it["id"], "回收任务")
                recycled += 1
        return {"ok": True, "recycled_count": recycled}

    def dispatch_to_node(self, task_id: int, node_code: str) -> dict[str, Any]:
        """将任务派发到指定节点；补采任务派发前先做已确认范围防护。"""
        entry = self.get_entry(task_id)
        if entry is None:
            return {"ok": False, "reason": "任务不存在"}
        if entry.get("status") != "待派发":
            return {"ok": False, "reason": "任务状态不是待派发"}

        source = entry.get("来源分片")
        biz_date = entry.get("业务日期")
        if source and biz_date:
            from app.services import service_for
            batch_svc = service_for("capture_batch")
            if batch_svc is not None and batch_svc.is_covered(source, biz_date):
                # 迟到回执或前序补采已经覆盖该日期：不得复采，任务回收
                self.run_action(task_id, "回收任务")
                return {
                    "ok": False,
                    "blocked": True,
                    "reason": f"来源{source}的{biz_date}已有已确认归集，禁止重复采集",
                }

        entry["占用节点"] = node_code
        self.run_action(task_id, "派发任务")
        return {"ok": True, "task_id": task_id, "node": node_code}

    # ── 补采任务：领用、中断接续与归集确认回写 ──────────────────

    def enqueue_recapture_task(
        self,
        *,
        source: str,
        biz_date: str,
        recapture_item_id: int,
        priority: str = "high",
        linked_plan_codes: str = "",
    ) -> dict[str, Any]:
        """把补采清单里的真实缺口转成任务队列里的待派发补采任务。"""
        rows = self.list_entries(size=10000)[0]
        next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        now_iso = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
        values = {
            "队列编号": f"TASK-RCAP-{next_id:04d}",
            "关联模板": f"补采:{source}:{biz_date}",
            "优先级": priority,
            "期望开始": now_iso,
            "重试次数": "0",
            "占用节点": "",
            "等待秒数": "0",
            "入队时间": now_iso,
            "来源分片": source,
            "业务日期": biz_date,
            "最近中断": "",
            "补采条目": str(recapture_item_id),
        }
        entry, _ = self.create_entry(values)
        entry["_关联排程编号"] = linked_plan_codes
        return entry

    def interrupt_task(self, task_id: int, reason: str) -> dict[str, Any]:
        """中断已派发的补采任务：回到待派发，记录中断原因供接续。"""
        entry = self.get_entry(task_id)
        if entry is None:
            return {"ok": False, "reason": "任务不存在"}
        if entry.get("status") != "已派发":
            return {"ok": False, "reason": "仅已派发任务可中断"}
        entry["最近中断"] = reason
        entry["占用节点"] = ""
        entry["status"] = "待派发"
        entry["pending"] = True
        return {"ok": True, "task_id": task_id, "reason": reason}

    def requeue_task(self, task_id: int) -> dict[str, Any]:
        """中断接续后重新提交任务，保留来源、业务日期与历史中断原因。"""
        entry = self.get_entry(task_id)
        if entry is None:
            return {"ok": False, "reason": "任务不存在"}
        if entry.get("status") != "待派发":
            return {"ok": False, "reason": "任务当前状态不允许重新提交"}
        entry["重试次数"] = str(int(entry.get("重试次数", "0") or "0") + 1)
        return {"ok": True, "task_id": task_id, "重试次数": entry["重试次数"]}

    def confirm_recapture_collection(self, task_id: int, checksum: str | None = None) -> dict[str, Any]:
        """确认补采任务的归集结果，按来源与业务日期回写每条原排程。

        1. 在采集批次落一条已确认批次（同范围去重，有效归集只计一次）；
        2. 补采清单条目标记已确认并登记批次回执；
        3. 该任务关联的每条原排程分别回写自己的已核结果与回执，
           各日结果可分别追到批次回执，不凭同名分片判整组完成；
        4. 任务本身回收为已消费。
        """
        task = self.get_entry(task_id)
        if task is None:
            return {"ok": False, "reason": "任务不存在"}
        source = str(task.get("来源分片", ""))
        biz_date = str(task.get("业务日期", ""))
        if not source or not biz_date:
            return {"ok": False, "reason": "任务缺少来源分片或业务日期，无法按范围回写"}

        from app.services import service_for
        batch_svc = service_for("capture_batch")
        plan_svc = service_for("schedule_plan")
        recapture_svc = service_for("recapture_list")
        if batch_svc is None or plan_svc is None or recapture_svc is None:
            return {"ok": False, "reason": "依赖服务未注册"}

        receipt = batch_svc.accept_late_receipt(
            source=source,
            biz_date=biz_date,
            checksum=checksum or f"recap-{source}-{biz_date}",
            row_count=1,
            ref_exec=task.get("队列编号", ""),
            late=False,
        )
        batch_id = receipt["batch_id"]
        batch_code = receipt["批次编号"]

        # 回写补采清单条目
        item_id_raw = task.get("补采条目")
        item_id = int(item_id_raw) if str(item_id_raw or "").isdigit() else None
        if item_id is not None:
            item = recapture_svc.get_entry(item_id)
            if item is not None:
                recapture_svc.run_action(item_id, "确认补采")
                item["已核结果"] = "已确认"
                item["批次回执"] = batch_code
                item["占用任务"] = ""
                # 逐条回写原排程，各自保留数据日期与已核结果
                for plan_id in item.get("_关联排程ids", []):
                    plan_svc.record_verification(
                        plan_id, result="已确认", receipt=batch_code, batch_id=batch_id
                    )

        self.run_action(task_id, "回收任务")
        return {
            "ok": True,
            "task_id": task_id,
            "来源分片": source,
            "业务日期": biz_date,
            "batch_id": batch_id,
            "批次回执": batch_code,
            "deduplicated": receipt.get("deduplicated", False),
        }
