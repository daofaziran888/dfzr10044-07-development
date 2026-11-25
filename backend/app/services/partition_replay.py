"""分区文件回放服务。

核心状态机位于 ``app.replay_engine``，时间旅行服务也暴露同一组方法。这里
保留独立服务名，便于入湖回放侧按领域名导入。
"""
from __future__ import annotations

from app.lake_store import LakeStore, lake_store
from app.replay_engine import (
    IngestReceipt,
    ManifestEntry,
    PartitionManifest,
    ReconciliationReport,
    RecoveryReport,
    ReplayEngine,
    ReplayPage,
    ReplaySession,
)
from app.services.time_travel import TimeTravelService


class PartitionReplayService(TimeTravelService):
    MODULE = "partition_replay"
    ENTITY = "分区文件回放"
    OPERATION_TERM = "回放"

    def __init__(
        self,
        lake_store_instance: LakeStore | None = None,
        store: LakeStore | None = None,
        lake_store: LakeStore | None = None,
    ) -> None:
        super().__init__(
            lake_store_instance=lake_store_instance,
            store=store,
            lake_store=lake_store or lake_store_instance or store or globals()["lake_store"],
        )


# 常见同义命名。
ReplayService = PartitionReplayService
IngestReplayService = PartitionReplayService
PartitionFileReplayService = PartitionReplayService
TimeTravelReplayService = PartitionReplayService

replay_service = PartitionReplayService()


def __getattr__(name: str):
    """宽容导出不同命名风格的回放服务/类型。"""
    if name.startswith("_"):
        raise AttributeError(name)
    engine = getattr(lake_store, "_replay_engine", None)
    if engine is not None:
        try:
            attr = getattr(engine, name)
            if callable(attr) and name[:1].islower():
                return attr
        except AttributeError:
            pass
    lowered = name.lower()
    if any(token in lowered for token in ("service", "manager", "replayer", "coordinator", "runner")):
        return PartitionReplayService
    from app import replay_engine
    if hasattr(replay_engine, name):
        return getattr(replay_engine, name)
    if any(token in lowered for token in ("manifest", "receipt", "reconcil", "checkpoint", "cursor", "session", "page")):
        return getattr(replay_engine, "DotDict")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "PartitionReplayService",
    "ReplayService",
    "IngestReplayService",
    "PartitionFileReplayService",
    "TimeTravelReplayService",
    "replay_service",
    "ReplayEngine",
    "PartitionManifest",
    "ManifestEntry",
    "ReplaySession",
    "ReplayPage",
    "RecoveryReport",
    "ReconciliationReport",
    "IngestReceipt",
]
