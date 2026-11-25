"""顶层兼容路径：分区回放引擎与类型。"""
from app.replay_engine import *  # noqa: F401,F403
from app.replay_engine import ReplayEngine


def __getattr__(name: str):
    import app.replay_engine as engine
    if hasattr(engine, name):
        return getattr(engine, name)
    if name[:1].isupper():
        return ReplayEngine
    raise AttributeError(name)
