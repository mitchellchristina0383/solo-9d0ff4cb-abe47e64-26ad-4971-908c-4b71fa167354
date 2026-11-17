"""端到端集成自测（不依赖 Docker，直接驱动本地三个进程）。

覆盖场景：
A. 基本发布/查询：修订递增；新修订发布后的查询拿不到旧路由；两实例共享缓存
B. 同修订并发未命中合并：N 个并发请求只产生 1 次回源，follower 命中 coalesced
C. 租约接管：回源 leader 被 kill / 租约超时后，另一实例接手完成
D. 慢回源跨越修订更新：旧响应只能落为 stale，新查询必须返回新修订
E. 回源失败明确 503；旧快照仍可通过 /snapshots 排障；恢复后自动回源
"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
DATA = ROOT / "data"
LOG = DATA / "logs"
SRC, C1, C2 = "http://127.0.0.1:8080", "http://127.0.0.1:8081", "http://127.0.0.1:8082"
SERVICE = "payments"
HTTP_TIMEOUT_EXPECT = 6.0  # 与 app/cache.py 的 FETCH_TIMEOUT 默认值一致

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((PASS if ok else FAIL, name, detail))
    print(f"[{PASS if ok else FAIL}] {name} {detail}")


class Stack:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen] = []

    def start(self, what: str, port: int, env: dict[str, str]) -> subprocess.Popen:
        log = open(LOG / f"{what}.log", "ab")
        p = subprocess.Popen(
            [PY, "-m", "uvicorn", f"app.{what}:app", "--host", "127.0.0.1", "--port", str(port)],
            cwd=str(ROOT),
            env={**os.environ, **env},
            stdout=log,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
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


async def wait_ready(client: httpx.AsyncClient, urls: list[str], timeout: float = 20) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = True
        for u in urls:
            try:
                r = await client.get(u)
                if r.status_code != 200:
                    ok = False
            except Exception:
                ok = False
        if ok:
            return
        await asyncio.sleep(0.3)
    raise RuntimeError("services did not become ready")


async def reset(client: httpx.AsyncClient) -> None:
    await client.post(f"{C1}/debug/reset")
    await client.post(f"{SRC}/admin/fault", json={"mode": "ok", "delay": 0, "code": 500})


async def main() -> int:
    DATA.mkdir(exist_ok=True)
    LOG.mkdir(parents=True, exist_ok=True)
    for f in (DATA / "source.db", DATA / "cache.db"):
        for suffix in ("", "-wal", "-shm"):
            Path(str(f) + suffix).unlink(missing_ok=True)

    stack = Stack()
    base_env = {"SOURCE_DB": str(DATA / "source.db"), "CACHE_DB": str(DATA / "cache.db")}
    stack.start("source", 8080, base_env)
    stack.start("cache", 8081, {**base_env, "SOURCE_BASE": SRC, "INSTANCE_ID": "cache1"})
    c1_proc = stack.procs[-1]
    stack.start("cache", 8082, {**base_env, "SOURCE_BASE": SRC, "INSTANCE_ID": "cache2"})

    try:
        async with httpx.AsyncClient(timeout=45) as client:
            await wait_ready(client, [f"{SRC}/docs", f"{C1}/health", f"{C2}/health"])
            await reset(client)

            # ------------------------------------------------------------ A
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.0.0.1:80", "10.0.0.2:80"]})
            r1 = await client.get(f"{C1}/routes/{SERVICE}")
            r2 = await client.get(f"{C2}/routes/{SERVICE}")  # 跨实例共享缓存
            ok_a = (
                r1.status_code == 200 and r2.status_code == 200
                and r1.json()["revision"] == 1 and r2.json()["revision"] == 1
                and r1.json()["nodes"] == ["10.0.0.1:80", "10.0.0.2:80"]
                and r2.json().get("from_cache") is True
            )
            record("A1 发布/查询/跨实例共享缓存", ok_a,
                   f"r1={r1.json().get('from_cache')} r2={r2.json().get('from_cache')}")

            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.0.0.3:80", "10.0.0.4:80"]})
            r3 = await client.get(f"{C1}/routes/{SERVICE}")
            ok_a2 = (r3.status_code == 200 and r3.json()["revision"] == 2
                     and r3.json()["nodes"] == ["10.0.0.3:80", "10.0.0.4:80"])
            record("A2 新修订提交后的查询不能拿到旧路由", ok_a2,
                   f"revision={r3.json().get('revision')} nodes={r3.json().get('nodes')}")

            # ------------------------------------------------------------ B
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.0.0.5:80"]})  # rev3
            # 新修订未被缓存；让回源慢一点，制造并发未命中窗口
            await client.post(f"{SRC}/admin/fault", json={"mode": "delay", "delay": 1.2})
            before = (await client.get(f"{SRC}/admin/metrics")).json()["fetch_total"]
            reqs = [client.get(f"{C1 if i % 2 else C2}/routes/{SERVICE}") for i in range(10)]
            responses = await asyncio.gather(*reqs, return_exceptions=True)
            await client.post(f"{SRC}/admin/fault", json={"mode": "ok"})
            after = (await client.get(f"{SRC}/admin/metrics")).json()["fetch_total"]
            statuses = sorted({getattr(x, "status_code", None) for x in responses})
            revs = {x.json()["revision"] for x in responses if hasattr(x, "status_code") and x.status_code == 200}
            coalesced = [x for x in responses
                         if hasattr(x, "status_code") and x.status_code == 200 and x.json().get("coalesced")]
            ok_b = (statuses == [200] and revs == {3} and (after - before) == 1 and len(coalesced) == 9)
            record("B 同修订 10 并发未命中合并为 1 次回源", ok_b,
                   f"origin_fetches={after - before} coalesced={len(coalesced)} statuses={statuses}")

            # ------------------------------------------------------------ C1: leader 被 kill
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.0.0.6:80"]})  # rev4
            await client.post(f"{SRC}/admin/fault", json={"mode": "delay", "delay": 2.5})

            async def fire(url: str) -> httpx.Response | Exception:
                try:
                    return await client.get(f"{url}/routes/{SERVICE}")
                except Exception as exc:
                    return exc

            t0 = time.time()
            asyncio.create_task(fire(C1))
            await asyncio.sleep(0.6)  # 确保 cache1 拿到租约并开始慢回源
            leader = (await client.get(f"{C2}/debug/state")).json()["inflight"]
            leader_id = leader[0]["leader"] if leader else None
            os.killpg(os.getpgid(c1_proc.pid), signal.SIGKILL)  # 模拟实例退出
            # 对 cache1 的在途请求随进程死亡而失败，符合预期
            await asyncio.sleep(0.2)
            # cache2 在租约过期后应接管（LEASE_TTL=2s）
            r_takeover = await client.get(f"{C2}/routes/{SERVICE}")
            elapsed = time.time() - t0
            ok_c1 = (leader_id == "cache1" and r_takeover.status_code == 200
                     and r_takeover.json()["revision"] == 4
                     and r_takeover.json().get("from_cache") is False)
            record("C1 leader 实例退出后其他实例接手回源", ok_c1,
                   f"dead_leader={leader_id} takeover_rev={r_takeover.json().get('revision')} "
                   f"elapsed={elapsed:.1f}s")
            await client.post(f"{SRC}/admin/fault", json={"mode": "ok"})

            # 重启 cache1 供后续场景使用
            stack.start("cache", 8081, {**base_env, "SOURCE_BASE": SRC, "INSTANCE_ID": "cache1"})
            c1_proc = stack.procs[-1]
            await wait_ready(client, [f"{C1}/health"])

            # ------------------------------------------------------------ C2: 租约超时（leader 卡死但进程还在）
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.0.0.7:80"]})  # rev5
            # hang 让 cache1 的回源一直挂住：硬超时后停心跳 -> 租约过期 -> cache2 接管
            await client.post(f"{SRC}/admin/fault", json={"mode": "hang"})
            t0 = time.time()
            task_hang = asyncio.create_task(fire(C1))
            await asyncio.sleep(0.8)
            inflight = (await client.get(f"{C2}/debug/state")).json()["inflight"]
            # follower 等待：leader 硬超时(6s) + 租约过期(2s) 后接管，接管同样撞硬超时
            r_take2 = await client.get(f"{C2}/routes/{SERVICE}")
            take_elapsed = time.time() - t0
            # 此时故障仍在，接管者也失败 -> 明确 503（而非无限等待）
            await client.post(f"{SRC}/admin/fault", json={"mode": "ok"})
            # 等 cache1 的在途请求落定，冷却过后重新回源成功
            res_hang = await task_hang
            await asyncio.sleep(3.2)
            r_recover = await client.get(f"{C2}/routes/{SERVICE}")
            ok_c2 = (inflight and inflight[0]["leader"] == "cache1"
                     and r_take2.status_code == 503 and take_elapsed >= HTTP_TIMEOUT_EXPECT
                     and r_recover.status_code == 200 and r_recover.json()["revision"] == 5)
            record("C2 leader 回源挂起、租约超时后接管，恢复后成功", ok_c2,
                   f"takeover={r_take2.status_code}({take_elapsed:.1f}s) "
                   f"leader_req={getattr(res_hang, 'status_code', res_hang)} "
                   f"recover={r_recover.status_code}")

            # ------------------------------------------------------------ D: 慢回源跨修订
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.0.0.8:80"]})  # rev6
            await client.post(f"{SRC}/admin/fault", json={"mode": "delay", "delay": 2.0})
            t0 = time.time()
            task_d = asyncio.create_task(fire(C1))
            await asyncio.sleep(0.8)  # rev6 回源在途
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.0.0.9:80"]})  # rev7
            await client.post(f"{SRC}/admin/fault", json={"mode": "ok"})
            rd = await task_d
            elapsed_d = time.time() - t0
            # leader 的慢响应回来时 rev6 已过期：409 且 rev6 快照只能是 stale
            snaps = (await client.get(f"{C1}/snapshots/{SERVICE}")).json()["snapshots"]
            s6 = next(s for s in snaps if s["revision"] == 6)
            r_latest = await client.get(f"{C2}/routes/{SERVICE}")
            ok_d = (rd.status_code == 409 and s6["status"] == "stale"
                    and r_latest.status_code == 200 and r_latest.json()["revision"] == 7
                    and r_latest.json()["nodes"] == ["10.0.0.9:80"])
            record("D 慢回源跨修订：旧响应不写成有效缓存，新查询返回新修订", ok_d,
                   f"slow_resp={rd.status_code} rev6={s6['status']} latest_rev={r_latest.json().get('revision')} "
                   f"elapsed={elapsed_d:.1f}s")

            # ------------------------------------------------------------ E: 失败明确、旧快照可排障
            await client.post(f"{SRC}/admin/publish",
                              json={"service": SERVICE, "nodes": ["10.0.0.10:80"]})  # rev8
            await client.post(f"{SRC}/admin/fault", json={"mode": "error", "code": 500})
            re = await client.get(f"{C1}/routes/{SERVICE}")
            detail = re.json().get("detail", {})
            ok_e1 = (re.status_code == 503 and detail.get("last_good", {}).get("revision") == 7)
            record("E1 回源失败明确返回 503 且携带 last_good 排障信息", ok_e1,
                   f"status={re.status_code} last_good={detail.get('last_good', {}).get('revision')}")

            # 冷却期内 follower 也快速 503（共享失败记录）
            re2 = await client.get(f"{C2}/routes/{SERVICE}")
            ok_e2 = re2.status_code == 503
            record("E2 失败冷却期内另一实例同样快速明确失败", ok_e2, f"status={re2.status_code}")

            # 旧快照仍可查询（即使源故障中）
            rs = await client.get(f"{C1}/snapshots/{SERVICE}/revisions/7")
            ok_e3 = rs.status_code == 200 and rs.json()["status"] == "valid"
            record("E3 旧快照仍可查作排障依据", ok_e3,
                   f"status={rs.status_code} rev={rs.json().get('revision')}")

            # 故障解除、冷却过后自动恢复
            await client.post(f"{SRC}/admin/fault", json={"mode": "ok"})
            await asyncio.sleep(3.2)
            rr = await client.get(f"{C2}/routes/{SERVICE}")
            ok_e4 = (rr.status_code == 200 and rr.json()["revision"] == 8
                     and rr.json()["nodes"] == ["10.0.0.10:80"])
            record("E4 故障恢复后重新回源成功", ok_e4,
                   f"status={rr.status_code} rev={rr.json().get('revision')}")

            # 文档可操作
            for name, url in [("source /docs", f"{SRC}/docs"),
                              ("cache1 /docs", f"{C1}/docs"),
                              ("cache2 /docs", f"{C2}/docs")]:
                rd = await client.get(url)
                record(f"F {name} 可打开", rd.status_code == 200, f"status={rd.status_code}")

    finally:
        stack.stop_all()

    failed = [r for r in results if r[0] == FAIL]
    print("\n================ 自测汇总 ================")
    for st, name, detail in results:
        print(f"  [{st}] {name} {detail}")
    print(f"\n通过 {len(results) - len(failed)}/{len(results)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
