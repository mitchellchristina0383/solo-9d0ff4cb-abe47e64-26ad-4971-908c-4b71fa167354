# 路由查询服务：双缓存实例 + 本地配置源（FastAPI + SQLite）

值班工程师更新服务发现节点列表后，入口实例不能继续把已下线节点从缓存里吐出来。
本项目用 FastAPI + SQLite 实现带**修订一致性、跨实例并发合并、租约接管**的路由查询层。

## 架构

```
                         ┌─────────────────────────────┐
  publish / fault  ───►  │ config-source (FastAPI)     │  SQLite: releases / current_rev
  /docs 可操作           │ :8080  修订按服务单调递增     │  故障注入: ok/delay/error/hang
                         └──────────────┬──────────────┘
                                        │ HTTP（current 轻量查询始终可靠；全量拉取可被故障注入）
                     ┌──────────────────┴──────────────────┐
                     ▼                                      ▼
          ┌────────────────────┐                ┌────────────────────┐
          │ cache1  :8081      │                │ cache2  :8082      │
          │ app/cache.py       │   共享 SQLite   │ app/cache.py       │
          └─────────┬──────────┘  (WAL, 卷挂载)  └─────────┬──────────┘
                    └──────────────► cache.db ◄────────────┘
                  表: snapshots(valid/stale) / inflight(租约+围栏令牌) / failures(冷却)
```

- **配置源** (`app/source.py`)：`POST /admin/publish` 每发一次，该服务 `revision + 1`；
  `GET /services/{s}/current` 返回当前修订（轻量、不受故障注入影响）；
  `GET /services/{s}/revisions/{r}` 返回指定修订全量节点（慢/错/挂起只作用在这里）。
- **缓存实例** (`app/cache.py`)：两个实例共享一个 SQLite 文件，无独立缓存进程，
  协调状态全部落在共享库的三张表里。`inflight` 除 leader 外还保存一次性
  `lease_token`，作为接管后阻止旧任务写入的围栏。

## 关键保证与实现方式

1. **新修订提交后的查询拿不到旧路由**
   每次查询先问配置源当前修订，只返回/写入该修订的快照；旧修订快照即使存在也不会被命中。

2. **同一修订的并发未命中合并（跨实例 singleflight）**
   `inflight` 表以 `(service, revision)` 为主键，`BEGIN IMMEDIATE` 事务抢租约，
   只有一个实例成为 leader 发起回源；其余实例轮询共享库，直接复用 leader 写入的快照
   （响应里 `coalesced=true`）。配置源 `/admin/metrics` 可核对实际回源次数。

3. **leader 超时、被取消或退出后其他实例接手**
   leader 每 0.7s 续租（TTL 2s），租约带一次性围栏令牌 `lease_token`。
   回源有 6s 硬时限：超时即停心跳、释放租约并记录失败；进程被 kill 时租约自然过期。
   **请求被取消（客户端断开 / 调用方中断）时立即停心跳并释放回源资格**，
   其他实例无需等 TTL 到期即可接管；接管成功的响应带 `served_by=<新回源实例>`
   与 `takeover=true`。等待中的 follower 在租约仍有效时不会被失败记录提前打回 503。
   滚动重启时 Compose 用 `--timeout-graceful-shutdown 1`：关闭钩子会取消在途回源，
   同样走取消路径释放资格（实测 SIGTERM 后 ~1s 内另一实例即可接管）。

4. **接管后旧任务的迟到动作不生效（围栏）**
   每次取得回源资格都会生成新的 `lease_token`；续租、写快照、写失败记录、释放租约
   都必须同时匹配 `leader + lease_token`。请求被取消后留下的交接标记
   （token 清空、立即过期）不阻止任何人接管，但旧任务此后的迟到结果/失败/清理
   全部落空，不会覆盖新持有者的租约、失败状态与有效快照；迟到的 stale 结果
   也不能把已有的 valid 快照降级。

5. **慢回源跨过修订更新，不把旧响应写成有效缓存**
   回源返回后再次核对当前修订：若已推进，旧修订快照只写为 `status=stale`，
   本次请求返回 409 提示重试；后续查询走新修订。stale 快照保留供排障。

6. **回源失败明确不可用，旧快照可排障**
   已确认修订但全量回源失败时统一 `503`，body 含 `reason` 与 `last_good`
   （最近一次有效快照）；失败有 3s 冷却（共享，两个实例都快速失败，避免雪崩），
   冷却过后自动重试。`GET /snapshots/{service}` 与
   `/snapshots/{service}/revisions/{r}` 在源故障期间仍可读。
   源整体宕机时，已有有效快照的服务降级返回（`degraded=true`），无快照才 503。

## 运行

### Docker Compose（目标部署方式）

```bash
docker compose up --build
# source  http://localhost:8080/docs
# cache1  http://localhost:8081/docs
# cache2  http://localhost:8082/docs
```
两个 cache 容器挂载同一个命名卷 `cache-shared`，SQLite 以 WAL 模式并发访问。

### 本地多进程（无 Docker 环境的等价方式）

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt   # 若环境无 pip，先 bootstrap（见下）
bash scripts/run_local.sh
```

> 极简环境（无 ensurepip/pip）可先 `curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && .venv/bin/python /tmp/get-pip.py`。

## /docs 可操作的完整流程

1. source `/docs` → `POST /admin/publish` 发布 `payments` 的节点（重复发布观察 revision 递增）
2. cache `/docs` → `GET /routes/payments` 查询，观察 `revision / nodes / from_cache / served_by`
3. source → `POST /admin/fault` 设 `{"mode":"delay","delay":2}` 模拟慢响应；
   `error` 模拟 5xx；`hang` 模拟挂起；`ok` 恢复
4. cache → `GET /debug/state` 看 inflight 租约（含 `lease_token` 短指纹）、failures 冷却、snapshots 状态
5. cache → `GET /snapshots/payments` 看全部历史快照（含 stale）

### 在 /docs 复现取消后的跨实例接管

1. source 故障注入 `{"mode":"delay","delay":2}`，并 `POST /admin/publish` 出一个新修订
2. cache1 `/docs` → `GET /routes/payments`，参数 `cancel_after=0.5`
   → 约 0.5s 后返回 409 `query cancelled ... lease released`，回源资格立即让出
3. 立刻在 cache2 `/docs` → `GET /routes/payments`
   → 200，`served_by=cache2`、`takeover=true`、`from_cache=false`（约 1–2s 内完成，
   不需要等 2s 租约过期）
4. 任意 cache → `GET /debug/state`：无带令牌的有效租约；取消者没有写入失败记录
5. 想验证"迟到结果不覆盖"：把 delay 调到 3s 并在步骤 2 用 `cancel_after=0.5`，
   接管完成后 `GET /snapshots/payments`，该修订仍是接管者写入的 `valid` 快照

## 自测

```bash
.venv/bin/python -I tests/test_integration.py   # 13 项：修订一致性/并发合并/接管/跨修订/故障
.venv/bin/python -I tests/test_cancel.py        # 8 项：取消/接管交错、围栏、断连、失败不可用
.venv/bin/python -I tests/test_edge.py          # 边界：源宕机降级、404、发布交错单调性
```

## 环境变量（cache）

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `CACHE_DB` | `/data/cache.db` | 共享 SQLite 路径 |
| `SOURCE_BASE` | `http://source:8080` | 配置源地址 |
| `INSTANCE_ID` | 自动生成 | 实例标识（租约 leader 字段） |
| `FETCH_TIMEOUT` | `6` | 回源硬超时秒数 |
| `LEASE_TTL` | `2` | 租约有效期秒数 |
| `HEARTBEAT_INTERVAL` | `0.7` | 续租间隔秒数 |
| `FAILURE_COOLDOWN` | `3` | 失败冷却秒数 |
