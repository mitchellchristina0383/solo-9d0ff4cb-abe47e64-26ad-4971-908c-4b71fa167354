"""路由查询缓存实例（cache）。

两个实例共享同一个 SQLite 数据库（docker volume / 本地共享目录），通过
SQLite 的行级事务协调"跨实例并发未命中合并（singleflight）"。

关键保证：
1. 修订单调：每次查询先向配置源确认当前修订；新修订提交后发起的查询
   不可能返回旧修订的节点。
2. 同修订并发未命中合并：同一 (service, revision) 同一时刻只允许一个实例
   回源，其余实例在库内等待结果（snapshot 或 failure 记录）。
3. 租约接管：回源持有者周期性续租；持有者超时、请求被取消或进程退出后，
   其他实例可在租约释放/过期后接管回源。取消会立即停心跳并释放租约。
4. 租约围栏（fencing）：每次取得回源资格都会生成新的 lease_token。续租、
   写快照、写失败记录和释放租约都必须同时匹配 leader + lease_token。
   已被接管的旧任务即使结果迟到，也只能丢弃，不能覆盖新持有者的租约、
   失败状态或有效快照。
5. 慢回源跨修订不写脏数据：回源返回后再次核对"该修订仍是当前修订"，
   过期修订只标记为 stale，绝不作为有效缓存；已存在的 valid 快照也
   不会被迟到的 stale 结果降级。
6. 回源失败明确 503：失败记录带冷却期，冷却期内快速失败；旧快照
   仍可通过 /snapshots 与故障响应中的 last_good 排障。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("CACHE_DB", "/data/cache.db")
SOURCE_BASE = os.environ.get("SOURCE_BASE", "http://source:8080")
INSTANCE_ID = os.environ.get("INSTANCE_ID", f"cache-{uuid.uuid4().hex[:8]}")
HTTP_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "6.0"))
LEASE_TTL = float(os.environ.get("LEASE_TTL", "2.0"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "0.7"))
FAILURE_COOLDOWN = float(os.environ.get("FAILURE_COOLDOWN", "3.0"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "0.15"))

logger = logging.getLogger("uvicorn.error.cache")


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
            lease_token TEXT NOT NULL DEFAULT '',  -- 租约围栏令牌（每次取得资格都换新）
            lease_expires_at REAL NOT NULL,
            started_at  REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    # 兼容旧库：补上 lease_token 列。旧版本写入的租约没有围栏令牌
    # （lease_token=''），无法证明持有者身份；新版本一律视为可立即接管，
    # 避免滚动维护时旧的已取消任务继续挡住回源。
    conn.execute("BEGIN IMMEDIATE")
    try:
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(inflight)").fetchall()}
        if "lease_token" not in cols:
            try:
                conn.execute(
                    "ALTER TABLE inflight ADD COLUMN lease_token TEXT NOT NULL DEFAULT ''"
                )
            except sqlite3.OperationalError as exc:
                # 另一个实例在滚动启动时同时迁移：列已被加上即可继续
                if "duplicate column name" not in str(exc).lower():
                    raise
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
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


def _lease_row(
    conn: sqlite3.Connection, service: str, revision: int
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT leader, lease_token, lease_expires_at, started_at "
        "FROM inflight WHERE service=? AND revision=?",
        (service, revision),
    ).fetchone()


def _owns_lease(
    conn: sqlite3.Connection, service: str, revision: int, token: str
) -> bool:
    row = _lease_row(conn, service, revision)
    return (
        row is not None
        and row["leader"] == INSTANCE_ID
        and row["lease_token"] == token
    )


def _lease_blocks_takeover(row: sqlite3.Row | None) -> bool:
    """该租约是否仍应阻止其他实例取得回源资格。

    没有围栏令牌（lease_token=''）的租约来自旧版本进程，无法证明/约束
    持有者身份，滚动维护时视为可立即接管，避免已取消的旧任务继续挡住回源。
    """
    return (
        row is not None
        and bool(row["lease_token"])
        and row["lease_expires_at"] > time.time()
    )


def _acquire_lease(
    conn: sqlite3.Connection, service: str, revision: int
) -> tuple[str | None, bool]:
    """尝试成为 (service, revision) 的回源 leader。

    返回 (lease_token, takeover)。token 为 None 表示已有未过期租约。
    takeover 表示这次取得资格是从另一个实例的租约/请求手中接过来的。
    """
    now = time.time()
    token = uuid.uuid4().hex
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = _lease_row(conn, service, revision)
        if _lease_blocks_takeover(row):
            conn.commit()
            return None, False
        # 存在任何旧租约行都意味着本次是在接手一个已在途的回源：另一个实例
        # 的租约过期/退出，或回源请求被取消后留下的交接标记。
        takeover = row is not None and row["leader"] != INSTANCE_ID
        conn.execute(
            "INSERT INTO inflight(service, revision, leader, lease_token, "
            "lease_expires_at, started_at) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(service, revision) DO UPDATE SET "
            "leader=excluded.leader, lease_token=excluded.lease_token, "
            "lease_expires_at=excluded.lease_expires_at, "
            "started_at=excluded.started_at",
            (service, revision, INSTANCE_ID, token, now + LEASE_TTL, now),
        )
        conn.commit()
        return token, takeover
    except BaseException:
        conn.rollback()
        raise


def _renew_lease(conn: sqlite3.Connection, service: str, revision: int, token: str) -> bool:
    """续租；只有仍持有同一 token 的 leader 才能续。"""
    cur = conn.execute(
        "UPDATE inflight SET lease_expires_at=? WHERE service=? AND revision=? "
        "AND leader=? AND lease_token=?",
        (time.time() + LEASE_TTL, service, revision, INSTANCE_ID, token),
    )
    return cur.rowcount == 1


def _release_lease(
    conn: sqlite3.Connection,
    service: str,
    revision: int,
    token: str,
    handoff: bool = False,
) -> bool:
    """释放自己的租约。

    - handoff=False（已有明确结果：成功/失败）：直接删除租约。
    - handoff=True（请求被取消、没有结果）：留下已失效的交接标记
      （lease_token 清空、立即过期、保留原 leader），使下一个取得资格的
      实例能识别出"这是接手"；该标记不阻止任何实例取得回源资格。
    两种方式都只在 leader + token 仍匹配时生效，已被接管时不会误删。
    """
    if handoff:
        cur = conn.execute(
            "UPDATE inflight SET lease_token='', lease_expires_at=? "
            "WHERE service=? AND revision=? AND leader=? AND lease_token=?",
            (time.time(), service, revision, INSTANCE_ID, token),
        )
    else:
        cur = conn.execute(
            "DELETE FROM inflight WHERE service=? AND revision=? "
            "AND leader=? AND lease_token=?",
            (service, revision, INSTANCE_ID, token),
        )
    return cur.rowcount == 1


def _release_lease_best_effort(
    service: str, revision: int, token: str, handoff: bool = True
) -> bool:
    try:
        conn = _connect()
        try:
            return _release_lease(conn, service, revision, token, handoff=handoff)
        finally:
            conn.close()
    except Exception:  # 释放失败不应掩盖原始取消/异常
        logger.warning(
            "failed to release lease for %s rev %s token %s",
            service, revision, token[:8], exc_info=True,
        )
        return False


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
    """写入快照；仅当修订仍为当前修订时标记 valid，否则标记 stale。

    额外的防降级条件：已存在的 valid 快照不会被迟到的 stale 结果覆盖。
    返回数据库中该快照的最终状态。
    """
    now = time.time()
    wanted = "valid" if still_current else "stale"
    conn.execute(
        "INSERT INTO snapshots(service, revision, nodes, status, source_published_at, cached_at) "
        "VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(service, revision) DO UPDATE SET nodes=excluded.nodes, "
        "status=excluded.status, source_published_at=excluded.source_published_at, "
        "cached_at=excluded.cached_at "
        "WHERE snapshots.status != 'valid' OR excluded.status = 'valid'",
        (service, revision, json.dumps(nodes), wanted, source_published_at, now),
    )
    row = _get_snapshot(conn, service, revision)
    return row["status"] if row is not None else wanted


def _drop_handoff_marker(
    conn: sqlite3.Connection, service: str, revision: int
) -> None:
    """清掉同一修订上已失效的交接标记（无令牌、不阻止任何人取得资格）。"""
    conn.execute(
        "DELETE FROM inflight WHERE service=? AND revision=? AND lease_token=''",
        (service, revision),
    )


def _commit_success(
    service: str,
    revision: int,
    nodes: list[str],
    source_published_at: float | None,
    still_current: bool,
    token: str,
) -> tuple[bool, str | None]:
    """围栏式提交回源结果。

    在同一个 BEGIN IMMEDIATE 事务里核对租约 token，再写快照、清失败记录、
    释放租约。token 已被新持有者替换时什么都不写，返回 (False, None)。
    """
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            if not _owns_lease(conn, service, revision, token):
                conn.rollback()
                return False, None
            status = _save_snapshot(
                conn, service, revision, nodes, source_published_at, still_current
            )
            conn.execute(
                "DELETE FROM failures WHERE service=? AND revision=?",
                (service, revision),
            )
            _release_lease(conn, service, revision, token)
            _drop_handoff_marker(conn, service, revision)
            conn.commit()
            return True, status
        except BaseException:
            conn.rollback()
            raise
    finally:
        conn.close()


def _commit_failure(service: str, revision: int, error: str, token: str) -> bool:
    """围栏式记录失败并释放租约；租约已被接管时不污染新持有者的状态。"""
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            if not _owns_lease(conn, service, revision, token):
                conn.rollback()
                return False
            _record_failure(conn, service, revision, error)
            _release_lease(conn, service, revision, token)
            _drop_handoff_marker(conn, service, revision)
            conn.commit()
            return True
        except BaseException:
            conn.rollback()
            raise
    finally:
        conn.close()


def _try_commit_failure(
    service: str, revision: int, error: str, token: str
) -> bool | None:
    """返回 True=已记录失败，False=租约已被接管未记录，None=写库失败。"""
    try:
        return _commit_failure(service, revision, error, token)
    except Exception:
        logger.warning(
            "failed to persist failure for %s rev %s", service, revision, exc_info=True
        )
        return None


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    """取走后台任务结果，避免 "Task exception was never retrieved" 噪音。"""
    if task.cancelled():
        return
    try:
        task.exception()
    except Exception:  # pragma: no cover
        pass


# ---------------------------------------------------------------------------
# 取消支持：请求被取消（客户端断开 / 显式故障模拟）时必须立即交出回源资格
# ---------------------------------------------------------------------------
_cancel_timers: set[asyncio.Task[Any]] = set()

# 本进程内仍在回源的 leader 任务，进程关闭时统一取消以立即释放租约。
_active_leaders: dict[tuple[str, int, str], asyncio.Task[Any]] = {}


def _cancel_active_leaders() -> None:
    for task in list(_active_leaders.values()):
        task.cancel()


def _schedule_self_cancel(delay: float) -> None:
    """在 delay 秒后取消当前请求任务，模拟调用方中断。

    仅用于 /docs 的故障模拟（`?cancel_after=`）；语义与客户端取消完全一致。
    """
    task = asyncio.current_task()
    if task is None:  # pragma: no cover - 正常请求总在 task 内
        return

    async def _cancel_later() -> None:
        await asyncio.sleep(delay)
        if not task.done():
            logger.warning("simulated cancel fired for request task")
            task.cancel()

    timer = asyncio.create_task(_cancel_later())
    _cancel_timers.add(timer)
    timer.add_done_callback(_cancel_timers.discard)


async def _watch_disconnect(receive: Any, handler: asyncio.Task[Any]) -> None:
    """等待客户端断开；断开即取消处理任务，从而停心跳并释放回源租约。

    查询接口不读取请求体，因此可以独占 receive。连接断开时 uvicorn 会唤醒
    该调用并返回 `http.disconnect`。
    """
    try:
        while True:
            message = await receive()
            if message.get("type") == "http.disconnect":
                logger.warning("client disconnected; cancelling in-flight request")
                handler.cancel()
                return
    except asyncio.CancelledError:
        raise
    except Exception:  # pragma: no cover - 监测失败不应影响请求
        logger.debug("disconnect watcher failed", exc_info=True)


# ---------------------------------------------------------------------------
# 核心：singleflight 查询
# ---------------------------------------------------------------------------
async def _query(service: str, cancel_after: float | None = None) -> dict[str, Any]:
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
        inflight_row = _lease_row(conn, service, current)
        lease_alive = _lease_blocks_takeover(inflight_row)
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

        token, takeover = _acquire_lease(conn, service, current)

        if token is not None:
            return await _run_as_leader(
                conn, service, current, token,
                takeover=takeover, cancel_after=cancel_after,
            )
        return await _wait_as_follower(conn, service, current, cancel_after)
    finally:
        conn.close()


async def _run_as_leader(
    conn: sqlite3.Connection,
    service: str,
    revision: int,
    lease_token: str,
    takeover: bool = False,
    cancel_after: float | None = None,
) -> dict[str, Any]:
    """作为 leader 回源。所有共享状态写入都带 lease_token 围栏。"""
    heartbeat_task = asyncio.create_task(
        _heartbeat(service, revision, lease_token)
    )
    current = asyncio.current_task()
    leader_key = (service, revision, lease_token)
    if current is not None:
        _active_leaders[leader_key] = current
    if cancel_after is not None:
        _schedule_self_cancel(cancel_after)

    lease_finalized = False
    try:
        try:
            # 硬时限：回源不能无限占用回源资格；超时后停心跳、放租约，
            # 其他实例可在租约过期后接管（即使本进程仍然存活）。
            payload = await asyncio.wait_for(
                _source_fetch(service, revision), timeout=HTTP_TIMEOUT
            )
        except asyncio.TimeoutError:
            reason = f"origin fetch timeout after {HTTP_TIMEOUT}s"
            recorded = _try_commit_failure(service, revision, reason, lease_token)
            # recorded=False 说明租约已被接管，无需再释放；None 表示写库失败，
            # 让 finally 再尽力释放一次，避免租约滞留。
            lease_finalized = recorded is not None
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "origin unavailable",
                    "reason": reason,
                    "service": service,
                    "requested_revision": revision,
                    "superseded": recorded is False,
                    "last_good": _last_good(conn, service),
                },
            )
        except SourceUnavailable as exc:
            recorded = _try_commit_failure(service, revision, str(exc), lease_token)
            lease_finalized = recorded is not None
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "origin unavailable",
                    "reason": str(exc),
                    "service": service,
                    "requested_revision": revision,
                    "superseded": recorded is False,
                    "last_good": _last_good(conn, service),
                },
            )
        except Exception as exc:
            recorded = _try_commit_failure(
                service, revision, f"unexpected: {exc!r}", lease_token
            )
            lease_finalized = recorded is not None
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "origin unavailable",
                    "reason": repr(exc),
                    "superseded": recorded is False,
                    "last_good": _last_good(conn, service),
                },
            )

        # 回源成功：核对该修订是否仍是当前修订，防止慢回源把旧响应写成有效缓存
        try:
            latest = await _source_current(service)
        except SourceUnavailable:
            latest = revision  # 当前修订查询可靠；极端情况下不降级，按当前值处理
        still_current = latest == revision

        applied, status = _commit_success(
            service,
            revision,
            payload["nodes"],
            payload.get("published_at"),
            still_current,
            lease_token,
        )
        lease_finalized = True

        if not applied:
            # 等待期间租约已被取消/接管：迟到结果必须丢弃。
            logger.warning(
                "discarding late origin result for %s rev %s on %s: lease lost",
                service, revision, INSTANCE_ID,
            )
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "lease lost before commit; late origin result discarded",
                    "service": service,
                    "fetched_revision": revision,
                    "current_revision": latest,
                    "superseded": True,
                    "last_good": _last_good(conn, service),
                },
            )

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
            "takeover": True if takeover else None,
            "cached_at": time.time(),
            "snapshot_status": status,
        }
    except asyncio.CancelledError:
        # 请求被取消（客户端断开 / 调用方中断）：立即停心跳并释放回源资格，
        # 让其他实例可以马上接管，而不是等租约自然过期。
        logger.warning(
            "leader request cancelled for %s rev %s on %s; releasing lease",
            service, revision, INSTANCE_ID,
        )
        raise
    finally:
        # 先同步释放回源资格，再收尾心跳；取消路径下不 await，避免二次取消
        # 打断收尾。心跳发现 token 不匹配或租约已删会自行退出。
        if not lease_finalized:
            # 任何非正常收尾（取消、未预期异常）都不能把租约留在库里持续续期。
            _release_lease_best_effort(service, revision, lease_token)
        heartbeat_task.cancel()
        heartbeat_task.add_done_callback(_consume_task_result)
        _active_leaders.pop(leader_key, None)


async def _heartbeat(service: str, revision: int, lease_token: str) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            conn = _connect()
            try:
                if not _renew_lease(conn, service, revision, lease_token):
                    return  # 已被接管或租约已释放
            finally:
                conn.close()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass


async def _wait_as_follower(
    conn: sqlite3.Connection,
    service: str,
    revision: int,
    cancel_after: float | None = None,
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
        inflight_row = _lease_row(conn, service, revision)
        lease_alive = _lease_blocks_takeover(inflight_row)
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
            token, _ = _acquire_lease(conn, service, revision)
            if token is not None:
                return await _run_as_leader(
                    conn, service, revision, token,
                    takeover=True, cancel_after=cancel_after,
                )

        # 租约已释放/过期/无围栏令牌（leader 取消、超时、退出或旧版本残留）：接管
        if not _lease_blocks_takeover(inflight_row):
            token, _ = _acquire_lease(conn, service, revision)
            if token is not None:
                return await _run_as_leader(
                    conn, service, revision, token,
                    takeover=True, cancel_after=cancel_after,
                )

    raise HTTPException(
        status_code=503,
        detail={
            "error": "timed out waiting for in-flight origin fetch",
            "last_good": _last_good(conn, service),
        },
    )


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(
    title=f"路由查询缓存 API（{INSTANCE_ID}）",
    description=(
        "两个缓存实例共享 SQLite 状态，提供跨实例并发合并、租约接管与"
        "修订一致性保证。回源请求被取消时会立即释放回源资格，其他实例可接管；"
        "所有共享状态写入都带租约围栏，迟到结果不会覆盖新持有者。"
        "/docs 可直接操作查询、快照排障、状态检查与取消故障模拟。"
    ),
    version="1.1.0",
)


@app.on_event("startup")
async def _startup() -> None:
    init_db()


@app.on_event("shutdown")
async def _shutdown() -> None:
    # 滚动重启/优雅关闭：立即取消在本进程内回源的请求，从而停止续租并释放
    # 回源资格，其他实例无需等租约 TTL 过期即可接管。
    if _active_leaders:
        logger.warning(
            "shutting down with %d in-flight origin fetch(es); releasing leases",
            len(_active_leaders),
        )
    _cancel_active_leaders()
    await asyncio.sleep(0)


class QueryResult(BaseModel):
    service: str
    revision: int
    nodes: list[str]
    served_by: str
    from_cache: bool
    coalesced: bool | None = None
    takeover: bool | None = None
    degraded: bool | None = None
    cached_at: float | None = None
    snapshot_status: str | None = None


@app.get(
    "/routes/{service}",
    response_model=QueryResult,
    summary="查询服务当前路由（含修订）",
)
async def query_route(
    request: Request,
    service: str,
    cancel_after: float | None = None,
) -> dict[str, Any]:
    """查询当前修订的路由。

    - 命中有效快照直接返回（`from_cache=true`）。
    - 成为回源 leader 时从源拉取；被其他实例接管时 `takeover=true`。
    - 故障模拟：`?cancel_after=0.5` 会让本请求在成为 leader 后 0.5s 被取消，
      立即释放回源资格（等价于客户端断连）；另一个实例再查询即可接管并成功。
    """
    # 用独立任务执行查询：客户端断开时可以取消它，从而停心跳并释放租约。
    task = asyncio.create_task(_query(service, cancel_after=cancel_after))
    task.add_done_callback(_consume_task_result)
    watcher = asyncio.create_task(_watch_disconnect(request.receive, task))
    try:
        return await task
    except asyncio.CancelledError:
        handler = asyncio.current_task()
        if handler is not None and handler.cancelling() > 0:
            # 外层（进程关闭等）被取消：先让查询任务处理取消并同步释放租约，
            # 再把取消继续向上传播。
            task.cancel()
            try:
                await asyncio.wait({task}, timeout=2.0)
            except asyncio.CancelledError:
                pass
            raise
        # 查询任务自身被取消（客户端断开 / ?cancel_after 模拟）：租约已在
        # 任务内释放，其他实例可立即接管。这里给出明确的未完成响应。
        return JSONResponse(
            status_code=409,
            content={
                "detail": {
                    "error": "query cancelled before origin fetch completed; "
                             "origin fetch lease released, retry to let another instance take over",
                    "service": service,
                    "served_by": INSTANCE_ID,
                }
            },
        )
    finally:
        watcher.cancel()
        watcher.add_done_callback(_consume_task_result)


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
                "SELECT service, revision, leader, lease_token, lease_expires_at, "
                "started_at FROM inflight"
            ).fetchall()
        ]
        for row in inflight:  # 不暴露完整令牌，只给短指纹便于排查
            row["lease_token"] = str(row["lease_token"])[:8]
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
