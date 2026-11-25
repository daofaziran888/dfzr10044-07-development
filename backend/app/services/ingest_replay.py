"""入湖回放服务的兼容导入路径。"""
from app.services.partition_replay import (  # noqa: F401
    IngestReplayService,
    PartitionReplayService,
    ReplayService,
    replay_service,
)
