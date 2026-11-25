"""顶层兼容路径：时间旅行分区清单。"""
from app.replay_engine import *  # noqa: F401,F403
from app.services.time_travel import TimeTravelService


def __getattr__(name: str):
    import app.replay_engine as engine
    if hasattr(engine, name):
        return getattr(engine, name)
    if hasattr(TimeTravelService, name):
        return getattr(TimeTravelService, name)
    raise AttributeError(name)
