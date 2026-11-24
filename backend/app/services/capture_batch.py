"""采集批次服务：执行产出归集、完整性校验、窗口去重和批次确认。

一次或多次流程执行的产出记录归集成批次，附带采集窗口和
完整性校验值供下游引用。去重逻辑按采集窗口+上游来源判重。

补采核对场景下，已确认批次按「上游来源 + 业务日期」构成有效归集
范围：同范围的有效归集只能计一次，迟到回执只证明它携带的那个
业务日期，不能凭同名分片把其他日期判成已完成。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


class CaptureBatchService(BaseModuleService):
    MODULE = "capture_batch"
    ENTITY = "采集批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "批次编号", "关联执行", "记录行数", "采集窗口", "完整性校验",
        "批次标签", "上游来源", "业务日期", "生成时间",
    ]
    REQUIRED_FIELDS = ["批次编号", "关联执行", "记录行数"]
    STATUS_ORDER = ["归集中", "校验中", "已确认", "校验失败"]
    ACTION_RULES = {"启动校验": "校验中", "确认批次": "已确认", "标记失败": "校验失败"}
    NEGATIVE_ACTIONS = []

    def find_duplicate(self, checksum: str, source: str) -> dict[str, Any] | None:
        """按完整性校验值和上游来源查找重复批次。"""
        items, _ = self.list_entries(size=10000)
        for it in items:
            if it.get("完整性校验") == checksum and it.get("上游来源") == source:
                return it
        return None

    def get_confirmed_batches(self) -> list[dict[str, Any]]:
        """获取所有已确认的采集批次，供下游交接打包。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("status") == "已确认"]

    def verify_integrity(self, batch_id: int) -> dict[str, Any]:
        """启动校验并返回校验结果。"""
        entry = self.get_entry(batch_id)
        if entry is None:
            return {"ok": False, "reason": "批次不存在"}
        self.run_action(batch_id, "启动校验")
        row_count = int(entry.get("记录行数", "0") or "0")
        if row_count <= 0:
            self.run_action(batch_id, "标记失败")
            return {"ok": False, "reason": "批次记录行数为0，校验失败"}
        self.run_action(batch_id, "确认批次")
        return {"ok": True, "batch_id": batch_id, "row_count": row_count}

    # ── 已确认范围与迟到回执 ─────────────────────────────────────

    def confirmed_scope(self) -> set[tuple[str, str]]:
        """已确认批次构成的有效归集范围：(上游来源, 业务日期) 去重集合。

        共用范围只计一次——多个已确认批次落在同一 (来源, 日期) 上，
        在集合里自然折叠为一个键，不会重复占用补采名额。
        """
        return {
            (str(b.get("上游来源", "")), str(b.get("业务日期", "")))
            for b in self.get_confirmed_batches()
            if b.get("业务日期")
        }

    def is_covered(self, source: str, biz_date: str) -> bool:
        """判断某来源在某个业务日期上是否已有已确认归集（精确到日期）。"""
        return (source, biz_date) in self.confirmed_scope()

    def find_coverage(self, source: str, biz_date: str) -> dict[str, Any] | None:
        """按来源与业务日期定位有效归集批次，找不到返回 None。

        只命中同一业务日期的已确认批次；同名分片但日期不同的批次
        不算覆盖，避免仅凭同名分片判整组已完成。
        """
        for batch in self.get_confirmed_batches():
            if batch.get("上游来源") == source and batch.get("业务日期") == biz_date:
                return batch
        return None

    def accept_late_receipt(
        self,
        *,
        source: str,
        biz_date: str,
        checksum: str,
        row_count: int = 1,
        ref_exec: str = "LATE-ROBOT",
        late: bool = True,
    ) -> dict[str, Any]:
        """接收旧机器人迟到的批次回执并完成确认。

        回执只携带它自己的来源与业务日期：确认后仅该 (来源, 日期)
        进入已确认范围，不影响同名分片下其他业务日期的补采机会。
        同范围已有已确认批次时，按「有效归集只计一次」直接复用，
        不再新建批次。
        """
        existing = self.find_coverage(source, biz_date)
        if existing is not None:
            return {
                "ok": True,
                "deduplicated": True,
                "batch_id": existing["id"],
                "批次编号": existing.get("批次编号", ""),
                "上游来源": source,
                "业务日期": biz_date,
            }

        rows = self.list_entries(size=10000)[0]
        next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        now_iso = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
        label = "late-receipt" if late else "recapture-confirm"
        values = {
            "批次编号": f"CAPT-L{next_id:04d}",
            "关联执行": ref_exec,
            "记录行数": str(row_count),
            "采集窗口": f"[{biz_date}]",
            "完整性校验": checksum,
            "批次标签": label,
            "上游来源": source,
            "业务日期": biz_date,
            "生成时间": now_iso,
        }
        entry, _ = self.create_entry(values)
        self.run_action(entry["id"], "启动校验")
        self.run_action(entry["id"], "确认批次")
        return {
            "ok": True,
            "deduplicated": False,
            "batch_id": entry["id"],
            "批次编号": values["批次编号"],
            "上游来源": source,
            "业务日期": biz_date,
        }
