"""快照管理领域服务：生成、标记、过期与回退湖表快照。"""
from __future__ import annotations

import time  # noqa: F401
from typing import Any

from app.services.base import BaseModuleService
from app.store import store  # noqa: F401
from app.lake_store import lake_store


class SnapshotMgrService(BaseModuleService):
    MODULE = "snapshot_mgr"
    ENTITY = "湖表快照"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照编号", "所属湖表", "快照标识", "父快照", "新增行数", "删除行数", "文件清单摘要", "生成时间"]
    REQUIRED_FIELDS = ["快照编号", "所属湖表", "快照标识"]
    STATUS_ORDER = ["生成中", "可用", "已过期", "已回退"]
    ACTION_RULES = {"标记可用": "可用", "标记过期": "已过期", "执行回退": "已回退"}
    NEGATIVE_ACTIONS = []

    def generate_snapshot_for_table(self, lake_table: str, file_ids: list[str],
                                    parent_snap: str | None = None) -> dict[str, Any]:
        """为指定湖表生成新快照，标记可用。"""
        snap = lake_store.create_snapshot(lake_table, file_ids, parent_snap,
                                          added_rows=0, deleted_rows=0)
        lake_store.mark_snapshot_available(lake_table, snap.snapshot_id)
        return {
            "snapshot_id": snap.snapshot_id,
            "lake_table": lake_table,
            "file_count": len(file_ids),
            "parent": snap.parent_snapshot,
            "created_at": snap.created_at,
        }

    def get_ancestor_chain(self, lake_table: str) -> list[str]:
        """获取快照祖先链（按时间顺序）。"""
        snaps = lake_store.list_snapshots(lake_table)
        chain: list[str] = []
        snap_map = {s.snapshot_id: s for s in snaps}
        if not snaps:
            return chain
        current = snaps[-1]
        while current is not None:
            chain.append(current.snapshot_id)
            current = snap_map.get(current.parent_snapshot or "")
        return list(reversed(chain))

    def diff_snapshots(self, lake_table: str, snap_a: str, snap_b: str) -> dict[str, Any]:
        """比较两个快照间的文件差异。"""
        s_a = lake_store.get_snapshot(lake_table, snap_a)
        s_b = lake_store.get_snapshot(lake_table, snap_b)
        if not s_a or not s_b:
            return {"error": "snapshot not found"}
        set_a = set(s_a.file_ids)
        set_b = set(s_b.file_ids)
        return {
            "added": list(set_b - set_a),
            "removed": list(set_a - set_b),
            "unchanged": list(set_a & set_b),
        }

    def __getattr__(self, name: str) -> Any:
        # 清单/回放能力以 LakeStore 上的 ReplayEngine 为唯一状态机。
        if name.startswith("_"):
            raise AttributeError(name)
        engine = getattr(lake_store, "_replay_engine", None)
        if engine is not None and hasattr(engine, name):
            return getattr(engine, name)
        if hasattr(lake_store, name):
            return getattr(lake_store, name)
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")
