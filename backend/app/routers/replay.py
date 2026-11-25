"""分区清单与入湖回放 HTTP 接口。

这些接口是领域引擎的薄适配层；所有状态判断都在 ReplayEngine 中。路径同时
覆盖 time_travel / partition_replay / ingest_replay 三种模块命名习惯。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from app.lake_store import lake_store

router = APIRouter(prefix="/api", tags=["分区文件回放"])


async def _params(request: Request) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for key, value in request.query_params.items():
        if value == "true":
            params[key] = True
        elif value == "false":
            params[key] = False
        else:
            params[key] = value
    if request.method in {"POST", "PUT", "PATCH"}:
        try:
            body = await request.json()
            if isinstance(body, dict):
                params.update(body)
        except Exception:
            pass
    if request.path_params:
        params.update(request.path_params)
    return params


def _engine():
    return lake_store._replay_engine


async def _call(request: Request, method: str) -> Any:
    params = await _params(request)
    return getattr(_engine(), method)(**params)


# ---------------- 清单 ----------------

@router.api_route("/time_travel/partition-manifest", methods=["GET", "POST"])
@router.api_route("/time-travel/partition-manifest", methods=["GET", "POST"])
@router.api_route("/time_travel/manifest", methods=["GET", "POST"])
@router.api_route("/partition_replay/manifest", methods=["GET", "POST"])
@router.api_route("/partition-replay/manifest", methods=["GET", "POST"])
@router.api_route("/ingest_replay/manifest", methods=["GET", "POST"])
async def partition_manifest(request: Request):
    return await _call(request, "build_partition_manifest")


# ---------------- 回放生命周期 ----------------

@router.api_route("/time_travel/replays", methods=["POST"])
@router.api_route("/partition_replay/replays", methods=["POST"])
@router.api_route("/ingest_replay/replays", methods=["POST"])
@router.api_route("/replays", methods=["POST"])
async def start_replay(request: Request):
    return await _call(request, "start_replay")


@router.api_route("/time_travel/replays/{replay_id}", methods=["GET"])
@router.api_route("/partition_replay/replays/{replay_id}", methods=["GET"])
@router.api_route("/ingest_replay/replays/{replay_id}", methods=["GET"])
@router.api_route("/replays/{replay_id}", methods=["GET"])
async def get_replay(request: Request):
    params = await _params(request)
    detail = params.pop("detail", None)
    if detail in {True, "true", "1", "detail"}:
        return _engine().get_replay_detail(params["replay_id"])
    return _engine().get_replay(params["replay_id"])


@router.api_route("/time_travel/replays/{replay_id}/detail", methods=["GET"])
@router.api_route("/partition_replay/replays/{replay_id}/detail", methods=["GET"])
@router.api_route("/replays/{replay_id}/detail", methods=["GET"])
async def replay_detail(request: Request):
    params = await _params(request)
    return _engine().get_replay_detail(params["replay_id"])


@router.api_route("/time_travel/replays/{replay_id}/page", methods=["GET", "POST"])
@router.api_route("/partition_replay/replays/{replay_id}/page", methods=["GET", "POST"])
@router.api_route("/replays/{replay_id}/page", methods=["GET", "POST"])
async def fetch_page(request: Request):
    return await _call(request, "fetch_page")


@router.api_route("/time_travel/replays/{replay_id}/next-file", methods=["GET", "POST"])
@router.api_route("/replays/{replay_id}/next-file", methods=["GET", "POST"])
async def next_file(request: Request):
    return await _call(request, "fetch_next_file")


@router.api_route("/time_travel/replays/{replay_id}/ack", methods=["POST"])
@router.api_route("/time_travel/replays/{replay_id}/confirm", methods=["POST"])
@router.api_route("/partition_replay/replays/{replay_id}/ack", methods=["POST"])
@router.api_route("/replays/{replay_id}/ack", methods=["POST"])
async def ack(request: Request):
    return await _call(request, "ack_files")


@router.api_route("/time_travel/replays/{replay_id}/pause", methods=["POST"])
@router.api_route("/replays/{replay_id}/pause", methods=["POST"])
async def pause(request: Request):
    return await _call(request, "pause_replay")


@router.api_route("/time_travel/replays/{replay_id}/recover", methods=["POST", "GET"])
@router.api_route("/time_travel/replays/{replay_id}/resume", methods=["POST", "GET"])
@router.api_route("/partition_replay/replays/{replay_id}/recover", methods=["POST", "GET"])
@router.api_route("/replays/{replay_id}/recover", methods=["POST", "GET"])
@router.api_route("/replays/{replay_id}/resume", methods=["POST", "GET"])
async def recover(request: Request):
    params = await _params(request)
    method = "resume_replay" if request.url.path.endswith("/resume") else "recover_replay"
    return getattr(_engine(), method)(**params)


@router.api_route("/time_travel/replays/{replay_id}/adopt", methods=["POST"])
@router.api_route("/replays/{replay_id}/adopt", methods=["POST"])
async def adopt(request: Request):
    return await _call(request, "adopt_snapshot")


@router.api_route("/time_travel/replays/{replay_id}/restart", methods=["POST"])
@router.api_route("/replays/{replay_id}/restart", methods=["POST"])
async def restart(request: Request):
    return await _call(request, "restart_replay")


@router.api_route("/time_travel/replays/{replay_id}/rows", methods=["GET"])
@router.api_route("/replays/{replay_id}/rows", methods=["GET"])
async def rows(request: Request):
    params = await _params(request)
    return _engine().recompute_rows(params["replay_id"])


@router.api_route("/time_travel/replays/{replay_id}/reconcile", methods=["GET", "POST"])
@router.api_route("/time_travel/replays/{replay_id}/close", methods=["POST"])
@router.api_route("/replays/{replay_id}/reconcile", methods=["GET", "POST"])
@router.api_route("/replays/{replay_id}/close", methods=["POST"])
async def close_or_reconcile(request: Request):
    params = await _params(request)
    if request.url.path.endswith("/close"):
        return _engine().close_replay(**params)
    return _engine().reconcile_replay(**params)


# ---------------- 回执/幂等入湖 ----------------

@router.api_route("/ingest/files", methods=["POST"])
@router.api_route("/ingest_batch/files", methods=["POST"])
@router.api_route("/replays/ingest-files", methods=["POST"])
async def ingest_files(request: Request):
    return await _call(request, "ingest_files")


@router.api_route("/ingest/receipts", methods=["POST"])
@router.api_route("/ingest_batch/receipts", methods=["POST"])
@router.api_route("/partition_replay/receipts", methods=["POST"])
@router.api_route("/receipts", methods=["POST"])
async def record_receipt(request: Request):
    return await _call(request, "record_receipt")


@router.api_route("/ingest/receipts/lost-commit", methods=["POST"])
@router.api_route("/receipts/lost-commit", methods=["POST"])
async def lost_commit(request: Request):
    return await _call(request, "commit_with_lost_response")


@router.api_route("/ingest/receipts/{receipt_id}", methods=["GET"])
@router.api_route("/receipts/{receipt_id}", methods=["GET"])
async def get_receipt(receipt_id: str):
    receipt = _engine().get_receipt(receipt_id)
    if receipt is None:
        return {"success": False, "error": "receipt not found"}
    return receipt
