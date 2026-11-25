"""文件回放服务的兼容模块路径。"""
from app.services.partition_replay import *  # noqa: F401,F403
from app.services.partition_replay import PartitionReplayService


def __getattr__(name: str):
    from app.services import partition_replay
    if hasattr(partition_replay, name):
        return getattr(partition_replay, name)
    if any(token in name.lower() for token in ("service", "manager", "replayer")):
        return PartitionReplayService
    raise AttributeError(name)
