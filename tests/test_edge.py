"""补充边界：配置源整体宕机 / 未知服务 / 发布过程中的交错查询。"""
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
SRC, C1, C2 = "http://127.0.0.1:8080", "http://127.0.0.1:8081", "http://127.0.0.1:8082"


async def main():
    procs = []

    def start(mod, port, env):
        p = subprocess.Popen(
            [PY, "-m", "uvicorn", f"app.{mod}:app", "--host", "127.0.0.1", "--port", str(port)],
            cwd=str(ROOT), env={**os.environ, **env},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=os.setsid,
        )
        procs.append(p)
        return p

    base = {"SOURCE_DB": str(DATA / "edge_source.db"), "CACHE_DB": str(DATA / "edge_cache.db")}
    for f in (DATA / "edge_source.db", DATA / "edge_cache.db"):
        for s in ("", "-wal", "-shm"):
            Path(str(f) + s).unlink(missing_ok=True)
    src = start("source", 8080, base)
    start("cache", 8081, {**base, "SOURCE_BASE": SRC, "INSTANCE_ID": "cache1"})
    start("cache", 8082, {**base, "SOURCE_BASE": SRC, "INSTANCE_ID": "cache2"})
    failures = 0
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            for _ in range(60):
                try:
                    await c.get(f"{C1}/health"); await c.get(f"{C2}/health"); break
                except Exception:
                    await asyncio.sleep(0.3)

            # 未知服务 -> 404
            r = await c.get(f"{C1}/routes/unknown")
            print("unknown service:", r.status_code)
            assert r.status_code == 404
            failures += (r.status_code != 404)

            # 建立 rev1/2 有效缓存
            for i in range(1, 3):
                await c.post(f"{SRC}/admin/publish", json={"service": "billing", "nodes": [f"h{i}"]})
                await c.get(f"{C1}/routes/billing")

            # 杀掉整个配置源
            os.killpg(os.getpgid(src.pid), signal.SIGKILL)
            await asyncio.sleep(0.5)

            # 已缓存修订仍可服务（不依赖源存活），标记 degraded
            r = await c.get(f"{C2}/routes/billing")
            body = r.json() if r.status_code == 200 else {}
            print("cached while source down:", r.status_code, body.get("revision"), "degraded=", body.get("degraded"))
            assert r.status_code == 200 and body["revision"] == 2 and body.get("degraded") is True
            failures += (r.status_code != 200)

            # 新服务无缓存、源不可达 -> 明确 503
            r = await c.get(f"{C1}/routes/brand-new")
            print("new service while source down:", r.status_code, r.json().get("detail", {}).get("error") if isinstance(r.json().get("detail"), dict) else r.json().get("detail"))
            assert r.status_code == 503
            failures += (r.status_code != 503)

            # 旧快照排障接口在源宕机期间仍可用
            r = await c.get(f"{C2}/snapshots/billing")
            print("snapshots while source down:", r.status_code, [s["revision"] for s in r.json()["snapshots"]])
            assert r.status_code == 200 and len(r.json()["snapshots"]) == 2
            failures += (r.status_code != 200)

            # 重启源，发布 rev3 后查询必须拿到新修订（不能停在旧缓存）
            src = start("source", 8080, base)
            for _ in range(60):
                try:
                    if (await c.get(f"{SRC}/docs")).status_code == 200:
                        break
                except Exception:
                    await asyncio.sleep(0.3)
            await c.post(f"{SRC}/admin/publish", json={"service": "billing", "nodes": ["h3"]})
            r = await c.get(f"{C1}/routes/billing")
            print("after source restart + publish:", r.status_code, r.json().get("revision"), r.json().get("nodes"))
            assert r.status_code == 200 and r.json()["revision"] == 3 and r.json()["nodes"] == ["h3"]
            failures += (r.status_code != 200 or r.json()["revision"] != 3)

            # 交错：发布 rev4/rev5 期间连续查询，结果只可能是发布当时的最新修订，绝不回退
            seen = []
            async def publish(rev_nodes):
                await c.post(f"{SRC}/admin/publish", json={"service": "billing", "nodes": rev_nodes})
            qtask = asyncio.create_task(_poll(c, seen))
            await publish(["h4"]); await asyncio.sleep(0.05)
            await publish(["h5"]); await asyncio.sleep(0.05)
            await publish(["h6"])
            await qtask
            revs = [x["revision"] for x in seen]
            monotonic = all(revs[i] <= revs[i + 1] for i in range(len(revs) - 1))
            print("interleave revisions:", revs, "monotonic:", monotonic)
            assert monotonic and max(revs) == 6
            failures += (not monotonic)
    finally:
        for p in procs:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
    print("EDGE FAILURES:", failures)
    sys.exit(1 if failures else 0)


async def _poll(c, seen):
    end = time.time() + 3
    while time.time() < end:
        try:
            r = await c.get(f"{C1 if len(seen) % 2 else C2}/routes/billing")
            if r.status_code == 200:
                seen.append(r.json())
        except Exception:
            pass
        await asyncio.sleep(0.02)


asyncio.run(main())
