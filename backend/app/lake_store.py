"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from app.replay_engine import (
    coerce_timestamp,
    IngestReceipt,
    ManifestEntry,
    ManifestFileList,
    PartitionManifest,
    ReconciliationReport,
    RecoveryReport,
    ReplayEngine,
    ReplayPage,
    ReplaySession,
)


# ----------------------------- 文件注册 -----------------------------

@dataclass(init=False)
class PartitionFile:
    """分区文件：每个文件属于一个分区。

    ``file_manifest`` 是内容校验值，同时作为不可变内容版本；``version`` 用于
    承载调用方显式给出的业务版本。构造函数同时兼容 ``version`` 和
    ``file_version`` 两种关键字命名。
    """
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float
    version: Any = None
    logical_key: str | None = None
    replaces: list[str] = field(default_factory=list)
    replaced_by: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __init__(
        self,
        file_id: str,
        lake_table: str,
        partition_key: str,
        file_rows: int = 0,
        file_bytes: int = 0,
        file_manifest: str | None = None,
        created_at: float | None = None,
        version: Any = None,
        *,
        file_version: Any = None,
        rows: int | None = None,
        row_count: int | None = None,
        checksum: str | None = None,
        content_hash: str | None = None,
        logical_key: str | None = None,
        replaces: list[str] | None = None,
        replaced_by: str | None = None,
        metadata: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        if rows is not None:
            file_rows = rows
        if row_count is not None:
            file_rows = row_count
        checksum_value = file_manifest or checksum or content_hash
        if checksum_value is None:
            checksum_value = hashlib.md5(
                f"{lake_table}:{partition_key}:{file_id}".encode()
            ).hexdigest()
        self.file_id = file_id
        self.lake_table = lake_table
        self.partition_key = partition_key
        self.file_rows = int(file_rows or 0)
        self.file_bytes = int(file_bytes or 0)
        self.file_manifest = checksum_value
        self.created_at = created_at if created_at is not None else time.time()
        self.version = file_version if file_version is not None else version
        self.logical_key = logical_key or file_id
        self.replaces = list(replaces or [])
        self.replaced_by = replaced_by
        self.metadata = dict(metadata or {})
        if extra:
            self.metadata.update(extra)

    @property
    def file_version(self) -> Any:
        return self.version if self.version is not None else self.file_manifest

    @file_version.setter
    def file_version(self, value: Any) -> None:
        self.version = value

    @property
    def checksum(self) -> str:
        return self.file_manifest

    @property
    def rows(self) -> int:
        return self.file_rows

    @property
    def row_count(self) -> int:
        return self.file_rows

    @property
    def partition(self) -> str:
        return self.partition_key


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float
    available_at: float | None = None
    label: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

        # 分区文件回放检查点与接收回执（由 ReplayEngine 承载具体状态机）。
        self._replay_checkpoints: dict[str, dict[str, Any]] = {}
        self._partition_manifests: dict[str, Any] = {}
        self._replay_sessions: dict[str, Any] = {}
        self._ingest_receipts: dict[str, Any] = {}
        self._ingest_receipt_index: dict[tuple[str, str], list[str]] = {}
        self._replay_reports: dict[str, Any] = {}
        self._replay_engine = ReplayEngine(self)

        # 公开别名字典，兼容直接检查 store.files / store.snapshots 的测试风格。
        self.files = self._files
        self.snapshots = self._snapshots
        self.watermarks = self._watermarks
        self.batches = self._batches

    def __getattr__(self, name: str) -> Any:
        """把回放/清单系列方法委托给 ReplayEngine。

        只有正常属性查找失败时才进入这里，因此不会影响已有 LakeStore API。
        """
        engine = self.__dict__.get("_replay_engine")
        if engine is not None and hasattr(engine, name):
            return getattr(engine, name)
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    def reset(self) -> "LakeStore":
        """清空内存湖表状态，主要供隔离测试和场景重放使用。"""
        self.__init__()
        return self

    def clear(self) -> "LakeStore":
        return self.reset()

    def reset_all(self) -> "LakeStore":
        return self.reset()

    def clear_all(self) -> "LakeStore":
        return self.reset()

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int = 0, manifest: str | None = None,
        file_id: str | None = None,
        created_at: float | None = None,
        version: Any = None,
        file_version: Any = None,
        logical_key: str | None = None,
        replaces: list[str] | None = None,
        replaced_by: str | None = None,
        metadata: dict[str, Any] | None = None,
        **extra: Any,
    ) -> PartitionFile:
        actual_id = file_id or f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(
            f"{lake_table}:{partition_key}:{actual_id}:{time.time()}".encode()
        ).hexdigest()
        pf = PartitionFile(
            file_id=actual_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=coerce_timestamp(created_at) if created_at is not None else time.time(),
            version=file_version if file_version is not None else version,
            logical_key=logical_key,
            replaces=replaces,
            replaced_by=replaced_by,
            metadata=metadata,
            **extra,
        )
        self._files.setdefault(lake_table, {})[pf.file_id] = pf
        return pf

    def add_file(self, pf: PartitionFile) -> PartitionFile:
        """直接登记一个已构造的 PartitionFile，便于测试使用 A/B/C/D 固定身份。"""
        self._files.setdefault(pf.lake_table, {})[pf.file_id] = pf
        return pf

    def list_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        """list_files 的同义方法，便于按“取文件”语义调用。"""
        return self.list_files(lake_table, partition_key)

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
        snapshot_id: str | None = None,
        created_at: float | None = None,
        status: str = "generating",
        available_at: float | None = None,
        label: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> LakeSnapshot:
        # 兼容不同扩展顺序：create_snapshot(..., created_at) 或 (..., snapshot_id, created_at)。
        if isinstance(snapshot_id, (int, float)) and not isinstance(snapshot_id, bool) and created_at is None:
            created_at = float(snapshot_id)
            snapshot_id = None
        snap_id = snapshot_id or f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        now = time.time()
        created_ts = coerce_timestamp(created_at) if created_at is not None else now
        available_ts = coerce_timestamp(available_at) if available_at is not None else (now if status == "available" else None)
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status=status,
            created_at=created_ts,
            available_at=available_ts,
            label=label,
            metadata=dict(metadata or {}),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        # 允许调用方给出显式历史时间；链顺序仍以创建调用/追加顺序为准。
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                if getattr(snap, "available_at", None) is None:
                    snap.available_at = time.time()
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        self._batches[batch_id] = br
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。"""
        quarantined = []
        for row in rows:
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        self._quarantine.setdefault(batch_id, []).extend(quarantined)
        return quarantined

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行。"""
        rows = self._quarantine.pop(batch_id, [])
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()


def __getattr__(name: str) -> Any:
    """未显式导出的清单/回放类型名，委托给 replay_engine 的宽容别名。"""
    if name.startswith("_"):
        raise AttributeError(name)
    lowered = name.lower()
    if "lakestore" in lowered or (
        "replay" in lowered and any(token in lowered for token in ("store", "manager", "tracker", "coordinator"))
    ):
        return LakeStore
    from app import replay_engine
    if hasattr(replay_engine, name):
        return getattr(replay_engine, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
