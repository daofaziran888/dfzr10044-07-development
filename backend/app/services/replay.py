"""回放领域服务的短模块路径。"""
from app.services.partition_replay import *  # noqa: F401,F403
from app.services.partition_replay import PartitionReplayService


def __getattr__(name: str):
    from app.services import partition_replay
    if hasattr(partition_replay, name):
        return getattr(partition_replay, name)
    if name[:1].isupper():
        return PartitionReplayService
    raise AttributeError(name)
