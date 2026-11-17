"""路由查询缓存实例（cache）。

两个实例共享同一个 SQLite 数据库（docker volume / 本地共享目录），通过
SQLite 的行级事务协调"跨实例并发未命中合并（singleflight）"。

关键保证：
1. 修订单调：每次查询先向配置源确认当前修订；新修订提交后发起的查询
   不可能返回旧修订的节点。
2. 同修订并发未命中合并：同一 (service, revision) 同一时刻只允许一个实例
   回源，其余实例在库内等待结果（snapshot 或 failure 记录）。
3. 租约接管：回源持有者周期性续租；持有者超时或进程退出后，其他实例
   可在租约过期后接管回源。
4. 慢回源跨修订不写脏数据：回源返回后再次核对"该修订仍是当前修订"，
   过期修订只标记为 stale，绝不作为有效缓存。
5. 回源失败明确 503：失败记录带冷却期，冷却期内快速失败；旧快照
   仍可通过 /snapshots 与故障响应中的 last_good 排障。
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
import uuid
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DB_PATH = os.environ.get("CACHE_DB", "/data/cache.db")
SOURCE_BASE = os.environ.get("SOURCE_BASE", "http://source:8080")
INSTANCE_ID = os.environ.get("INSTANCE_ID", f"cache-{uuid.uuid4().hex[:8]}")
HTTP_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "6.0"))
LEASE_TTL = float(os.environ.get("LEASE_TTL", "2.0"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "0.7"))
FAILURE_COOLDOWN = float(os.environ.get("FAILURE_COOLDOWN", "3.0"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "0.15"))


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = _connect()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            service     TEXT NOT NULL,
            revision    INTEGER NOT NULL,
            nodes       TEXT NOT NULL,
            status      TEXT NOT NULL,            -- valid | stale
            source_published_at REAL,
            cached_at   REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inflight (
            service     TEXT NOT NULL,
            revision    INTEGER NOT NULL,
            leader      TEXT NOT NULL,
            lease_expires_at REAL NOT NULL,
            started_at  REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS failures (
            service    TEXT NOT NULL,
            revision   INTEGER NOT NULL,
            error      TEXT NOT NULL,
            cooldown_until REAL NOT NULL,
            updated_at REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    conn.close()


# ---------------------------------------------------------------------------
# 配置源客户端
# ---------------------------------------------------------------------------
class SourceUnavailable(Exception):
    pass


async def _source_current(service: str) -> int:
    try:
        async with httpx.AsyncClient(base_url=SOURCE_BASE, timeout=HTTP_TIMEOUT) as c:
            r = await c.get(f"/services/{service}/current")
            if r.status_code == 404:
                raise HTTPException(status_code=404, detail=f"service '{service}' not found")
            r.raise_for_status()
            return int(r.json()["revision"])
    except HTTPException:
        raise
    except Exception as exc:  # 连不上 / 超时 / 5xx
        raise SourceUnavailable(f"current revision lookup failed: {exc}") from exc


async def _source_fetch(service: str, revision: int) -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=SOURCE_BASE, timeout=HTTP_TIMEOUT) as c:
        r = await c.get(f"/services/{service}/revisions/{revision}")
        if r.status_code >= 400:
            raise SourceUnavailable(f"origin returned HTTP {r.status_code}: {r.text[:200]}")
        return r.json()


# ---------------------------------------------------------------------------
# DB 辅助
# ---------------------------------------------------------------------------
def _get_snapshot(conn: sqlite3.Connection, service: str, revision: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM snapshots WHERE service=? AND revision=?", (service, revision)
    ).fetchone()


def _last_good(conn: sqlite3.Connection, service: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM snapshots WHERE service=? AND status='valid' "
        "ORDER BY revision DESC LIMIT 1",
        (service,),
    ).fetchone()
    if row is None:
        return None
    return {
        "service": row["service"],
        "revision": row["revision"],
        "nodes": json.loads(row["nodes"]),
        "cached_at": row["cached_at"],
    }


def _try_acquire_lease(conn: sqlite3.Connection, service: str, revision: int) -> bool:
    """尝试成为 (service, revision) 的回源 leader；已有未过期租约则失败。"""
    now = time.time()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT leader, lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, revision),
        ).fetchone()
        if row is not None and row["lease_expires_at"] > now:
            conn.commit()
            return False
        # 无记录，或租约已过期 -> 接管
        conn.execute(
            "INSERT INTO inflight(service, revision, leader, lease_expires_at, started_at) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT(service, revision) DO UPDATE SET "
            "leader=excluded.leader, lease_expires_at=excluded.lease_expires_at, "
            "started_at=excluded.started_at",
            (service, revision, INSTANCE_ID, now + LEASE_TTL, now),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise


def _renew_lease(conn: sqlite3.Connection, service: str, revision: int) -> bool:
    now = time.time()
    row = conn.execute(
        "SELECT leader FROM inflight WHERE service=? AND revision=?", (service, revision)
    ).fetchone()
    if row is None or row["leader"] != INSTANCE_ID:
        return False
    conn.execute(
        "UPDATE inflight SET lease_expires_at=? WHERE service=? AND revision=? AND leader=?",
        (now + LEASE_TTL, service, revision, INSTANCE_ID),
    )
    return True


def _release_lease(conn: sqlite3.Connection, service: str, revision: int) -> None:
    conn.execute(
        "DELETE FROM inflight WHERE service=? AND revision=? AND leader=?",
        (service, revision, INSTANCE_ID),
    )


def _record_failure(conn: sqlite3.Connection, service: str, revision: int, error: str) -> None:
    now = time.time()
    conn.execute(
        "INSERT INTO failures(service, revision, error, cooldown_until, updated_at) "
        "VALUES(?,?,?,?,?) "
        "ON CONFLICT(service, revision) DO UPDATE SET error=excluded.error, "
        "cooldown_until=excluded.cooldown_until, updated_at=excluded.updated_at",
        (service, revision, error[:500], now + FAILURE_COOLDOWN, now),
    )


def _save_snapshot(
    conn: sqlite3.Connection,
    service: str,
    revision: int,
    nodes: list[str],
    source_published_at: float | None,
    still_current: bool,
) -> str:
    """写入快照；仅当修订仍为当前修订时标记 valid，否则标记 stale。"""
    now = time.time()
    status = "valid" if still_current else "stale"
    conn.execute(
        "INSERT INTO snapshots(service, revision, nodes, status, source_published_at, cached_at) "
        "VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(service, revision) DO UPDATE SET nodes=excluded.nodes, "
        "status=excluded.status, source_published_at=excluded.source_published_at, "
        "cached_at=excluded.cached_at",
        (service, revision, json.dumps(nodes), status, source_published_at, now),
    )
    return status


# ---------------------------------------------------------------------------
# 核心：singleflight 查询
# ---------------------------------------------------------------------------
async def _query(service: str) -> dict[str, Any]:
    # 第一步：确认当前修订。配置源整体不可达时降级：
    # 有本地有效快照则降级服务（标记 degraded），无快照才明确 503。
    try:
        current = await _source_current(service)
    except SourceUnavailable:
        conn = _connect()
        try:
            good = _last_good(conn, service)
            if good is None:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "origin unavailable and no local snapshot",
                        "service": service,
                        "last_good": None,
                    },
                )
            return {
                "service": service,
                "revision": good["revision"],
                "nodes": good["nodes"],
                "served_by": INSTANCE_ID,
                "from_cache": True,
                "degraded": True,
                "cached_at": good["cached_at"],
            }
        finally:
            conn.close()

    conn = _connect()
    try:
        snap = _get_snapshot(conn, service, current)
        if snap is not None and snap["status"] == "valid":
            return {
                "service": service,
                "revision": current,
                "nodes": json.loads(snap["nodes"]),
                "served_by": INSTANCE_ID,
                "from_cache": True,
                "cached_at": snap["cached_at"],
            }

        # 失败冷却期内快速明确失败（旧快照仍可单独查询）。
        # 若仍有 leader 持有效租约（其回源可能仍在途），不提前失败，转入等待/接管逻辑。
        fail = conn.execute(
            "SELECT * FROM failures WHERE service=? AND revision=?", (service, current)
        ).fetchone()
        inflight_row = conn.execute(
            "SELECT lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, current),
        ).fetchone()
        lease_alive = inflight_row is not None and inflight_row["lease_expires_at"] > time.time()
        if fail is not None and fail["cooldown_until"] > time.time() and not lease_alive:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "origin unavailable (failure cooldown)",
                    "reason": fail["error"],
                    "retry_after": round(fail["cooldown_until"] - time.time(), 2),
                    "last_good": _last_good(conn, service),
                },
            )

        is_leader = _try_acquire_lease(conn, service, current)

        if is_leader:
            return await _run_as_leader(conn, service, current)
        return await _wait_as_follower(conn, service, current)
    finally:
        conn.close()


async def _run_as_leader(conn: sqlite3.Connection, service: str, revision: int) -> dict[str, Any]:
    heartbeat_task = asyncio.create_task(_heartbeat(service, revision))
    try:
        # 硬时限：回源不能无限占用回源资格；超时后停心跳、放租约，
        # 其他实例可在租约过期后接管（即使本进程仍然存活）。
        payload = await asyncio.wait_for(
            _source_fetch(service, revision), timeout=HTTP_TIMEOUT
        )
    except asyncio.TimeoutError:
        heartbeat_task.cancel()
        conn2 = _connect()
        try:
            conn2.execute("BEGIN IMMEDIATE")
            _record_failure(conn2, service, revision, f"origin fetch timeout after {HTTP_TIMEOUT}s")
            _release_lease(conn2, service, revision)
            conn2.commit()
        finally:
            conn2.close()
        raise HTTPException(
            status_code=503,
            detail={
                "error": "origin unavailable",
                "reason": f"origin fetch timeout after {HTTP_TIMEOUT}s",
                "service": service,
                "requested_revision": revision,
                "last_good": _last_good(conn, service),
            },
        )
    except SourceUnavailable as exc:
        heartbeat_task.cancel()
        conn2 = _connect()
        try:
            conn2.execute("BEGIN IMMEDIATE")
            _record_failure(conn2, service, revision, str(exc))
            _release_lease(conn2, service, revision)
            conn2.commit()
        finally:
            conn2.close()
        raise HTTPException(
            status_code=503,
            detail={
                "error": "origin unavailable",
                "reason": str(exc),
                "service": service,
                "requested_revision": revision,
                "last_good": _last_good(conn, service),
            },
        )
    except Exception as exc:
        heartbeat_task.cancel()
        conn2 = _connect()
        try:
            conn2.execute("BEGIN IMMEDIATE")
            _record_failure(conn2, service, revision, f"unexpected: {exc!r}")
            _release_lease(conn2, service, revision)
            conn2.commit()
        finally:
            conn2.close()
        raise HTTPException(
            status_code=503,
            detail={"error": "origin unavailable", "reason": repr(exc),
                    "last_good": _last_good(conn, service)},
        )
    else:
        heartbeat_task.cancel()

    # 回源成功：核对该修订是否仍是当前修订，防止慢回源把旧响应写成有效缓存
    try:
        latest = await _source_current(service)
    except SourceUnavailable:
        latest = revision  # 当前修订查询可靠；极端情况下不降级，按当前值处理
    still_current = latest == revision

    conn3 = _connect()
    try:
        conn3.execute("BEGIN IMMEDIATE")
        status = _save_snapshot(
            conn3,
            service,
            revision,
            payload["nodes"],
            payload.get("published_at"),
            still_current,
        )
        # 成功后清除失败记录与租约
        conn3.execute(
            "DELETE FROM failures WHERE service=? AND revision=?", (service, revision)
        )
        _release_lease(conn3, service, revision)
        conn3.commit()
    finally:
        conn3.close()

    if not still_current:
        # 旧修订已过期：不把它当有效结果返回，让请求走新修订
        raise HTTPException(
            status_code=409,
            detail={
                "error": "revision advanced during slow origin fetch; retry for latest",
                "fetched_revision": revision,
                "current_revision": latest,
            },
        )

    return {
        "service": service,
        "revision": revision,
        "nodes": payload["nodes"],
        "served_by": INSTANCE_ID,
        "from_cache": False,
        "cached_at": time.time(),
        "snapshot_status": status,
    }


async def _heartbeat(service: str, revision: int) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            conn = _connect()
            try:
                if not _renew_lease(conn, service, revision):
                    return  # 已被接管
            finally:
                conn.close()
        except Exception:
            pass


async def _wait_as_follower(
    conn: sqlite3.Connection, service: str, revision: int
) -> dict[str, Any]:
    # 覆盖：leader 硬超时 + 租约 TTL + 接管者自己的回源时间，再加余量
    deadline = time.time() + HTTP_TIMEOUT * 3 + LEASE_TTL * 2
    while time.time() < deadline:
        await asyncio.sleep(POLL_INTERVAL)

        snap = _get_snapshot(conn, service, revision)
        if snap is not None and snap["status"] == "valid":
            return {
                "service": service,
                "revision": revision,
                "nodes": json.loads(snap["nodes"]),
                "served_by": INSTANCE_ID,
                "from_cache": True,
                "coalesced": True,
                "cached_at": snap["cached_at"],
            }

        # leader 失败：共享失败记录生效，冷却期内明确 503。
        # 但若仍有 leader 持有有效租约（例如其回源挂起、只是客户端先超时），
        # 不能直接失败，应等待租约过期后接管。
        fail = conn.execute(
            "SELECT * FROM failures WHERE service=? AND revision=?", (service, revision)
        ).fetchone()
        inflight_row = conn.execute(
            "SELECT lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, revision),
        ).fetchone()
        lease_alive = inflight_row is not None and inflight_row["lease_expires_at"] > time.time()
        if fail is not None and not lease_alive:
            now = time.time()
            if fail["cooldown_until"] > now:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "origin unavailable (coalesced leader failure)",
                        "reason": fail["error"],
                        "retry_after": round(fail["cooldown_until"] - now, 2),
                        "last_good": _last_good(conn, service),
                    },
                )
            # 冷却已过：清掉旧失败记录，尝试接管重新回源
            conn.execute(
                "DELETE FROM failures WHERE service=? AND revision=?", (service, revision)
            )
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(conn, service, revision)

        # 租约过期（leader 超时/退出）：接管
        if inflight_row is None:
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(conn, service, revision)
        elif inflight_row["lease_expires_at"] <= time.time():
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(conn, service, revision)

    raise HTTPException(
        status_code=503,
        detail={"error": "timed out waiting for in-flight origin fetch",
                "last_good": _last_good(conn, service)},
    )


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(
    title=f"路由查询缓存 API（{INSTANCE_ID}）",
    description=(
        "两个缓存实例共享 SQLite 状态，提供跨实例并发合并、租约接管与"
        "修订一致性保证。/docs 可直接操作查询、快照排障与状态检查。"
    ),
    version="1.0.0",
)


@app.on_event("startup")
async def _startup() -> None:
    init_db()


class QueryResult(BaseModel):
    service: str
    revision: int
    nodes: list[str]
    served_by: str
    from_cache: bool
    coalesced: bool | None = None
    degraded: bool | None = None
    cached_at: float | None = None
    snapshot_status: str | None = None


@app.get("/routes/{service}", response_model=QueryResult, summary="查询服务当前路由（含修订）")
async def query_route(service: str) -> dict[str, Any]:
    return await _query(service)


@app.get("/snapshots/{service}", summary="列出该服务全部缓存快照（含 stale，供排障）")
async def list_snapshots(service: str) -> dict[str, Any]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT service, revision, nodes, status, source_published_at, cached_at "
            "FROM snapshots WHERE service=? ORDER BY revision",
            (service,),
        ).fetchall()
        return {
            "service": service,
            "snapshots": [
                {
                    "revision": r["revision"],
                    "nodes": json.loads(r["nodes"]),
                    "status": r["status"],
                    "source_published_at": r["source_published_at"],
                    "cached_at": r["cached_at"],
                }
                for r in rows
            ],
        }
    finally:
        conn.close()


@app.get("/snapshots/{service}/revisions/{revision}", summary="查看指定修订的历史快照（排障）")
async def get_snapshot(service: str, revision: int) -> dict[str, Any]:
    conn = _connect()
    try:
        r = _get_snapshot(conn, service, revision)
        if r is None:
            raise HTTPException(status_code=404, detail="snapshot not found")
        return {
            "service": service,
            "revision": revision,
            "nodes": json.loads(r["nodes"]),
            "status": r["status"],
            "source_published_at": r["source_published_at"],
            "cached_at": r["cached_at"],
        }
    finally:
        conn.close()


@app.get("/debug/state", summary="共享数据库内部状态（inflight/failures/快照）")
async def debug_state() -> dict[str, Any]:
    conn = _connect()
    try:
        inflight = [
            dict(r)
            for r in conn.execute(
                "SELECT service, revision, leader, lease_expires_at, started_at FROM inflight"
            ).fetchall()
        ]
        failures = [dict(r) for r in conn.execute("SELECT * FROM failures").fetchall()]
        snaps = [
            {
                "service": r["service"],
                "revision": r["revision"],
                "status": r["status"],
                "cached_at": r["cached_at"],
            }
            for r in conn.execute(
                "SELECT service, revision, status, cached_at FROM snapshots "
                "ORDER BY service, revision"
            ).fetchall()
        ]
        return {
            "instance_id": INSTANCE_ID,
            "now": time.time(),
            "inflight": inflight,
            "failures": failures,
            "snapshots": snaps,
        }
    finally:
        conn.close()


@app.post("/debug/reset", summary="清空共享缓存状态（测试用）")
async def debug_reset() -> dict[str, Any]:
    conn = _connect()
    try:
        conn.execute("DELETE FROM snapshots")
        conn.execute("DELETE FROM inflight")
        conn.execute("DELETE FROM failures")
    finally:
        conn.close()
    return {"reset": True, "by": INSTANCE_ID}


@app.get("/health", summary="健康检查")
async def health() -> dict[str, str]:
    return {"status": "ok", "instance_id": INSTANCE_ID}
