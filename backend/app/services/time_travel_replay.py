"""时间旅行回放服务的兼容模块路径。"""
from app.services.partition_replay import *  # noqa: F401,F403
from app.services.time_travel import TimeTravelService


def __getattr__(name: str):
    from app.services import time_travel
    if hasattr(time_travel, name):
        return getattr(time_travel, name)
    if name[:1].isupper():
        return TimeTravelService
    raise AttributeError(name)
