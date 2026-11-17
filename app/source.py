"""本地配置源（config-source）。

职责：
- 按服务保存路由发布，每次发布令该服务的 revision 单调递增；
- 提供"当前修订"轻量查询与"指定修订全量配置"查询；
- 支持故障注入：慢响应、错误响应、超时（挂起），仅作用于全量配置拉取，
  当前修订查询始终可靠（保证缓存实例能感知修订推进）。
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

DB_PATH = os.environ.get("SOURCE_DB", "/data/source.db")

# 故障注入状态（进程内即可，编排中只有一个配置源实例）
_fault_lock = asyncio.Lock()
_fault: dict[str, Any] = {"mode": "ok", "delay": 0.0, "code": 500}

# 运行期统计，便于测试断言"并发合并后实际回源次数"
_metrics: dict[str, Any] = {
    "publish_total": 0,
    "fetch_total": 0,
    "fetch_by_revision": {},
    "fault_injected_total": 0,
}
_metrics_lock = asyncio.Lock()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = _connect()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS releases (
            service  TEXT NOT NULL,
            revision INTEGER NOT NULL,
            nodes    TEXT NOT NULL,
            published_at REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS current_rev (
            service TEXT PRIMARY KEY,
            revision INTEGER NOT NULL
        )
        """
    )
    conn.close()
    yield


app = FastAPI(
    title="配置源 API（config-source）",
    description="路由配置发布、修订查询与故障模拟。故障只作用于全量配置拉取。",
    version="1.0.0",
    lifespan=lifespan,
)


class PublishRequest(BaseModel):
    service: str = Field(..., examples=["payments"])
    nodes: list[str] = Field(..., examples=[["10.0.0.1:80", "10.0.0.2:80"]])


class FaultRequest(BaseModel):
    mode: str = Field(
        "ok",
        description="ok=正常；delay=延迟 delay 秒后正常返回；error=返回 HTTP 错误；hang=挂起直到超时",
    )
    delay: float = Field(0.0, ge=0.0, description="delay 模式下的延迟秒数")
    code: int = Field(500, ge=400, le=599, description="error 模式下返回的状态码")


@app.post("/admin/publish", summary="发布一次路由配置（修订 +1）")
async def publish(req: PublishRequest) -> dict[str, Any]:
    import json

    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT revision FROM current_rev WHERE service=?", (req.service,)
        ).fetchone()
        next_rev = (row["revision"] + 1) if row else 1
        conn.execute(
            "INSERT INTO releases(service, revision, nodes, published_at) VALUES (?,?,?,?)",
            (req.service, next_rev, json.dumps(req.nodes), time.time()),
        )
        conn.execute(
            "INSERT INTO current_rev(service, revision) VALUES(?,?) "
            "ON CONFLICT(service) DO UPDATE SET revision=excluded.revision",
            (req.service, next_rev),
        )
        conn.commit()
    finally:
        conn.close()

    async with _metrics_lock:
        _metrics["publish_total"] += 1
    return {
        "service": req.service,
        "revision": next_rev,
        "nodes": req.nodes,
        "published_at": time.time(),
    }


@app.get("/services/{service}/current", summary="查询当前修订（轻量、始终可靠）")
async def current_revision(service: str) -> dict[str, Any]:
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT revision FROM current_rev WHERE service=?", (service,)
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail=f"service '{service}' not found")
    return {"service": service, "revision": row["revision"]}


@app.get(
    "/services/{service}/revisions/{revision}",
    summary="拉取指定修订的全量路由（会被故障注入影响）",
)
async def get_revision(service: str, revision: int) -> dict[str, Any]:
    # 故障注入仅作用于全量拉取
    async with _fault_lock:
        mode, delay, code = _fault["mode"], float(_fault["delay"]), int(_fault["code"])
    if mode != "ok":
        async with _metrics_lock:
            _metrics["fault_injected_total"] += 1

    if mode == "delay" and delay > 0:
        await asyncio.sleep(delay)
    elif mode == "hang":
        # 一直挂起，依赖客户端超时/连接断开
        await asyncio.Event().wait()
    elif mode == "error":
        raise HTTPException(status_code=code, detail=f"injected error {code}")

    import json

    conn = _connect()
    try:
        row = conn.execute(
            "SELECT revision, nodes, published_at FROM releases "
            "WHERE service=? AND revision=?",
            (service, revision),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"revision {revision} of service '{service}' not found",
        )

    async with _metrics_lock:
        _metrics["fetch_total"] += 1
        by_rev = _metrics["fetch_by_revision"].setdefault(service, {})
        by_rev[str(revision)] = by_rev.get(str(revision), 0) + 1

    return {
        "service": service,
        "revision": row["revision"],
        "nodes": json.loads(row["nodes"]),
        "published_at": row["published_at"],
    }


@app.get("/admin/fault", summary="查看当前故障注入配置")
async def get_fault() -> dict[str, Any]:
    async with _fault_lock:
        return dict(_fault)


@app.post("/admin/fault", summary="设置故障注入（ok/delay/error/hang）")
async def set_fault(req: FaultRequest) -> dict[str, Any]:
    if req.mode not in {"ok", "delay", "error", "hang"}:
        raise HTTPException(status_code=400, detail="mode must be ok/delay/error/hang")
    async with _fault_lock:
        _fault.update(mode=req.mode, delay=req.delay, code=req.code)
        return dict(_fault)


@app.get("/admin/metrics", summary="查看发布/回源统计")
async def metrics() -> dict[str, Any]:
    async with _metrics_lock:
        import copy

        return copy.deepcopy(_metrics)
