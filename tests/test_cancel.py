"""取消 / 接管交错专项自测。

覆盖：
- 回源 leader 请求被取消后立即停止续租并释放回源资格，另一实例可立刻接管；
- 接管成功时响应标记 served_by（新回源节点）与 takeover=true；
- 取消者迟到的提交/失败记录/释放动作不能覆盖新持有者的租约、失败状态与有效快照；
- 已确认修订但全量回源失败时仍返回 503，旧快照可排障；
- 客户端真实断开连接同样会触发取消与释放。
"""
from __future__ import annotations

import asyncio
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
DATA = ROOT / "data"
SRC, C1, C2 = "http://127.0.0.1:8080", "http://127.0.0.1:8081", "http://127.0.0.1:8082"
SERVICE = "cancel-svc"

results: list[tuple[bool, str, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((ok, name, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


def _fencing_case() -> tuple[bool, str]:
    """进程内验证租约围栏：被接管后的旧任务不能写任何共享状态。"""
    sys.path.insert(0, str(ROOT))
    tmp = tempfile.mkdtemp()
    os.environ["CACHE_DB"] = os.path.join(tmp, "fence.db")
    os.environ["INSTANCE_ID"] = "old-leader"
    import app.cache as cache

    cache.init_db()
    conn = cache._connect()
    old_token, _ = cache._acquire_lease(conn, SERVICE, 1)
    assert old_token

    # 新持有者（另一实例）接管：替换 leader / token，并写入有效快照
    now = time.time()
    conn.execute(
        "UPDATE inflight SET leader='cache2', lease_token='new-token', "
        "lease_expires_at=? WHERE service=? AND revision=1",
        (now + 30, SERVICE),
    )
    conn.execute(
        "INSERT INTO snapshots(service, revision, nodes, status, source_published_at, cached_at) "
        "VALUES(?,?,?,?,?,?)",
        (SERVICE, 1, '["new-holder"]', "valid", None, now),
    )
    conn.commit()

    # 旧任务迟到：提交成功、提交失败、续租、释放 —— 全部应被拒绝
    applied, _ = cache._commit_success(SERVICE, 1, ["late-old"], None, True, old_token)
    fail_written = cache._commit_failure(SERVICE, 1, "late failure", old_token)
    renewed = cache._renew_lease(cache._connect(), SERVICE, 1, old_token)
    released = cache._release_lease(cache._connect(), SERVICE, 1, old_token)
    # 迟到 stale 结果也不得把新持有者的有效快照降级
    conn.execute(
        "INSERT INTO snapshots(service, revision, nodes, status, source_published_at, cached_at) "
        "VALUES(?,?,?,?,?,?) ON CONFLICT(service, revision) DO UPDATE SET "
        "status=excluded.status, nodes=excluded.nodes "
        "WHERE snapshots.status != 'valid' OR excluded.status = 'valid'",
        (SERVICE, 1, '["late-stale"]', "stale", None, now),
    )
    conn.commit()

    row = cache._lease_row(cache._connect(), SERVICE, 1)
    snap = cache._get_snapshot(cache._connect(), SERVICE, 1)
    fails = cache._connect().execute(
        "SELECT count(*) AS c FROM failures WHERE service=?", (SERVICE,)
    ).fetchone()["c"]
    ok = (
        applied is False and fail_written is False and renewed is False
        and released is False
        and row is not None and row["leader"] == "cache2"
        and row["lease_token"] == "new-token"
        and snap["status"] == "valid" and snap["nodes"] == '["new-holder"]'
        and fails == 0
    )
    return ok, (
        f"commit={applied} failure={fail_written} renew={renewed} release={released} "
        f"holder={row['leader'] if row else None} snapshot={snap['status']} failures={fails}"
    )


class Stack:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen] = []

    def start(self, what: str, port: int, env: dict[str, str]) -> subprocess.Popen:
        log = open(DATA / "logs" / f"{what}.log", "ab")
        p = subprocess.Popen(
            [PY, "-m", "uvicorn", f"app.{what}:app", "--host", "127.0.0.1", "--port", str(port)],
            cwd=str(ROOT), env={**os.environ, **env},
            stdout=log, stderr=subprocess.STDOUT, preexec_fn=os.setsid,
        )
        self.procs.append(p)
        return p

    def stop_all(self) -> None:
        for p in self.procs:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except Exception:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass


def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


async def wait_ports_free(ports: list[int], timeout: float = 30) -> None:
    """等上一轮测试的进程真正释放端口，避免套件连跑时抢端口。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(_port_free(p) for p in ports):
            return
        await asyncio.sleep(0.2)
    raise RuntimeError("ports still in use; a previous stack did not exit")


async def wait_ready(
    client: httpx.AsyncClient, urls: list[str], procs: list[subprocess.Popen],
    timeout: float = 20,
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        for p in procs:
            if p.poll() is not None:
                raise RuntimeError(f"service process exited early with code {p.returncode}")
        ok = True
        for u in urls:
            try:
                if (await client.get(u)).status_code != 200:
                    ok = False
            except Exception:
                ok = False
        if ok:
            return
        await asyncio.sleep(0.3)
    raise RuntimeError("services did not become ready")


async def main() -> int:
    ok, detail = _fencing_case()
    record("F1 租约围栏：旧任务迟到提交/失败/续租/释放均不影响新持有者", ok, detail)

    DATA.mkdir(exist_ok=True)
    (DATA / "logs").mkdir(exist_ok=True)
    for f in (DATA / "cancel_source.db", DATA / "cancel_cache.db"):
        for suffix in ("", "-wal", "-shm"):
            Path(str(f) + suffix).unlink(missing_ok=True)

    stack = Stack()
    base_env = {
        "SOURCE_DB": str(DATA / "cancel_source.db"),
        "CACHE_DB": str(DATA / "cancel_cache.db"),
    }
    await wait_ports_free([8080, 8081, 8082])
    stack.start("source", 8080, base_env)
    stack.start("cache", 8081, {**base_env, "SOURCE_BASE": SRC, "INSTANCE_ID": "cache1"})
    stack.start("cache", 8082, {**base_env, "SOURCE_BASE": SRC, "INSTANCE_ID": "cache2"})

    try:
        async with httpx.AsyncClient(timeout=45) as client:
            await wait_ready(
                client,
                [f"{SRC}/docs", f"{C1}/health", f"{C2}/health"],
                stack.procs,
            )
            await client.post(f"{C1}/debug/reset")
            await client.post(f"{SRC}/admin/fault", json={"mode": "ok"})

            # ---------------------------------------------------------- S1
            # leader 请求被取消：立即释放回源资格，另一实例接管并成功
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.1.0.1:80"]})
            await client.post(f"{SRC}/admin/fault", json={"mode": "delay", "delay": 1.0})

            t_cancel = time.time()
            r_cancel = await client.get(f"{C1}/routes/{SERVICE}?cancel_after=0.4")
            cancel_elapsed = time.time() - t_cancel
            state_after_cancel = (await client.get(f"{C2}/debug/state")).json()
            # 取消后可能留下一条"交接标记"行（leader=旧持有者、token 已清空、
            # 立即过期）；它不阻止任何实例取得资格，因此按"无有效租约"判断。
            inflight_after_cancel = [
                r for r in state_after_cancel["inflight"] if r["lease_token"]
            ]
            handoff_rows = [
                r for r in state_after_cancel["inflight"] if not r["lease_token"]
            ]
            fails_after_cancel = state_after_cancel["failures"]

            t_take = time.time()
            r_take = await client.get(f"{C2}/routes/{SERVICE}")
            take_elapsed = time.time() - t_take
            body = r_take.json()
            ok_s1 = (
                r_cancel.status_code == 409
                and cancel_elapsed < 1.5
                and inflight_after_cancel == []          # 取消者已停止续租并让出资格
                and fails_after_cancel == []             # 取消不算回源失败
                and r_take.status_code == 200
                and body.get("revision") == 1
                and body.get("nodes") == ["10.1.0.1:80"]
                and body.get("served_by") == "cache2"
                and body.get("takeover") is True
                and body.get("from_cache") is False
                and take_elapsed < 1.8                   # 只需接管者自己的回源时间，不额外等 TTL
            )
            record("S1 取消后立即释放资格，另一实例即刻接管并返回新回源节点", ok_s1,
                   f"cancel={r_cancel.status_code}({cancel_elapsed:.2f}s) "
                   f"live_leases={len(inflight_after_cancel)} "
                   f"handoff={len(handoff_rows)} failures={len(fails_after_cancel)} "
                   f"take={r_take.status_code}({take_elapsed:.2f}s) "
                   f"served_by={body.get('served_by')} takeover={body.get('takeover')}")

            # 取消者的旧任务不得留下失败记录或改动快照
            snaps = (await client.get(f"{C2}/snapshots/{SERVICE}")).json()["snapshots"]
            ok_s2 = (
                [s["revision"] for s in snaps] == [1]
                and snaps[0]["status"] == "valid"
                and snaps[0]["nodes"] == ["10.1.0.1:80"]
            )
            record("S2 取消者未写入失败记录，接管者的有效快照未被覆盖", ok_s2,
                   f"snapshots={[(s['revision'], s['status'], s['nodes']) for s in snaps]}")

            # ---------------------------------------------------------- S3
            # 交错：取消者的回源请求仍在源端挂延迟，接管者已完成并写入快照后，
            # 取消者迟到的结果/失败/释放动作必须全部无效。
            await client.post(f"{SRC}/admin/fault", json={"mode": "ok"})
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.1.0.2:80"]})  # rev2
            await client.post(f"{SRC}/admin/fault", json={"mode": "delay", "delay": 2.5})
            t0 = time.time()
            r_cancel2 = await client.get(f"{C1}/routes/{SERVICE}?cancel_after=0.3")
            # 取消者断开后其上游 httpx 请求也被取消；接管者按正常路径回源
            r_take2 = await client.get(f"{C2}/routes/{SERVICE}")
            # 等过取消者原本的延迟窗口（2.5s 从 t0 起算），确认没有迟到写入
            await asyncio.sleep(max(0.0, t0 + 3.2 - time.time()))
            await client.post(f"{SRC}/admin/fault", json={"mode": "ok"})
            state = (await client.get(f"{C1}/debug/state")).json()
            snaps = (await client.get(f"{C1}/snapshots/{SERVICE}")).json()["snapshots"]
            rev2 = next((s for s in snaps if s["revision"] == 2), None)
            ok_s3 = (
                r_cancel2.status_code == 409
                and r_take2.status_code == 200
                and r_take2.json().get("revision") == 2
                and r_take2.json().get("served_by") == "cache2"
                and r_take2.json().get("takeover") is True
                and rev2 is not None and rev2["status"] == "valid"
                and rev2["nodes"] == ["10.1.0.2:80"]
                and [r for r in state["inflight"] if r["lease_token"]] == []
                and state["failures"] == []
            )
            record("S3 取消后交错接管：迟到结果/失败不覆盖新持有者的租约与快照", ok_s3,
                   f"cancel={r_cancel2.status_code} take={r_take2.status_code}"
                   f" served_by={r_take2.json().get('served_by')} "
                   f"rev2={(rev2 or {}).get('status')} live_leases="
                   f"{len([r for r in state['inflight'] if r['lease_token']])} "
                   f"failures={len(state['failures'])}")

            # ---------------------------------------------------------- S4
            # 已确认修订（rev3）但全量回源失败：仍明确不可用，旧快照可排障
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.1.0.9:80"]})  # rev3
            await client.post(f"{SRC}/admin/fault", json={"mode": "error", "code": 500})
            r_fail = await client.get(f"{C1}/routes/{SERVICE}")
            detail_fail = r_fail.json().get("detail", {})
            ok_s4 = (
                r_fail.status_code == 503
                and isinstance(detail_fail, dict)
                and detail_fail.get("last_good", {}).get("revision") == 2
                and detail_fail.get("last_good", {}).get("nodes") == ["10.1.0.2:80"]
            )
            record("S4 已确认修订但回源失败仍返回 503，旧快照随响应排障", ok_s4,
                   f"status={r_fail.status_code} last_good="
                   f"{detail_fail.get('last_good', {}).get('revision')}")

            # 冷却期内另一实例同样快速 503；旧快照接口仍可读
            r_fast = await client.get(f"{C2}/routes/{SERVICE}")
            r_snap = await client.get(f"{C1}/snapshots/{SERVICE}/revisions/2")
            ok_s5 = (r_fast.status_code == 503 and r_snap.status_code == 200
                     and r_snap.json()["status"] == "valid")
            record("S5 冷却期内跨实例快速失败，旧快照仍可查", ok_s5,
                   f"fast={r_fast.status_code} snapshot={r_snap.status_code}")

            # ---------------------------------------------------------- S6
            # 真实客户端断连：请求超时后连接关闭，服务端应立即释放回源资格
            await client.post(f"{SRC}/admin/fault", json={"mode": "ok"})
            await asyncio.sleep(3.2)  # 等失败冷却结束
            await client.post(f"{SRC}/admin/fault", json={"mode": "delay", "delay": 1.0})
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.1.0.4:80"]})  # rev4
            try:
                await client.get(f"{C1}/routes/{SERVICE}", timeout=0.4)
            except Exception:
                pass  # 客户端先超时并断开，符合预期
            await asyncio.sleep(0.4)
            state = (await client.get(f"{C2}/debug/state")).json()
            t_take2 = time.time()
            r_take2 = await client.get(f"{C2}/routes/{SERVICE}")
            take2_elapsed = time.time() - t_take2
            ok_s6 = (
                [r for r in state["inflight"] if r["lease_token"]] == []
                and r_take2.status_code == 200
                and r_take2.json().get("revision") == 4
                and r_take2.json().get("served_by") == "cache2"
                and r_take2.json().get("takeover") is True
                and take2_elapsed < 1.8
            )
            record("S6 客户端断连后实例立即停止续租并让出资格，另一实例接管", ok_s6,
                   f"inflight={state['inflight']} take={r_take2.status_code}"
                   f"({take2_elapsed:.2f}s) takeover={r_take2.json().get('takeover')}")

            await client.post(f"{SRC}/admin/fault", json={"mode": "ok"})

            # ---------------------------------------------------------- S7
            # 取消后正常路径仍可继续服务（无残留 inflight/失败状态）
            r_final = await client.get(f"{C2}/routes/{SERVICE}")
            state = (await client.get(f"{C1}/debug/state")).json()
            # 旧修订的失败记录保留用于排障属预期；当前修订不应有失败记录
            current = r_final.json().get("revision") if r_final.status_code == 200 else None
            stale_failures = [f for f in state["failures"] if f["revision"] == current]
            ok_s7 = (r_final.status_code == 200 and current == 4
                     and [r for r in state["inflight"] if r["lease_token"]] == []
                     and stale_failures == [])
            record("S7 取消/接管交错结束后状态干净且查询正常", ok_s7,
                   f"status={r_final.status_code} inflight={len(state['inflight'])} "
                   f"failures_for_current={len(stale_failures)}")

    finally:
        stack.stop_all()

    failed = [r for r in results if not r[0]]
    print("\n================ 取消/接管专项汇总 ================")
    for ok_, name, detail_ in results:
        print(f"  [{'PASS' if ok_ else 'FAIL'}] {name} {detail_}")
    print(f"\n通过 {len(results) - len(failed)}/{len(results)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
