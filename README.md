# 差分隐私公交客流查询服务

FastAPI + SQLite 实现的差分隐私客流查询 API：乘车 CSV 一次导入后只读，查询返回
**去重乘客数 + 独立 Laplace(0, 1/ε) 噪声**（不返回真值），并在两个 API 进程共享
同一 SQLite 库的前提下，完成预算记账、相同查询去重与崩溃租约接续。

## 一、需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 一次导入 CSV（匿名乘客号、上车站），之后只读 | `POST /import` 单次导入，`meta.imported` 锁定，重复导入返回 409 |
| 非空站点集合 + ε∈{0.1, 0.2, 0.5} | Pydantic 校验，空集合/非法 ε 返回 422 |
| 去重乘客数 + 独立 Laplace(0,1/ε)，不返回真值 | `app/privacy.py` 逆变换采样（SystemRandom）；真值只在计算瞬间存在，库里只存加噪值 |
| 总预算 1，新查询预留 ε，成功后扣 ε | 预算账本 `budget_ledger`：reserve(+ε) → 成功时 release(-ε)+commit(-ε)，同一事务 |
| 同集合、同 ε 永久复用原结果，顺序无关，不重复扣费 | 站点去空白/去重/排序后 SHA-256 得 `canonical_key`，`(key, ε)` 唯一缓存；命中直接返回，零扣费、同噪声 |
| 预留+已用不得超过 1，不足拒绝 | 预算检查与预留同在 `BEGIN IMMEDIATE` 事务，不足返回 HTTP 402，不留任何预留 |
| 两个 API 进程共享库，相同查询复用在途任务，不重复计算 | 部分唯一索引保证每 `(key, ε)` 至多一个 active 任务；后来者成为跟随者只读轮询 |
| 统计不占库写事务，其他请求保持响应 | WAL + 短连接；统计走独立只读连接，写事务仅覆盖记账/状态变更 |
| 结果与预留转实扣共同持久化后才返回；响应丢失不重新扰动 | 结果插入、任务置成功、release+commit 在一个事务提交；重试命中缓存拿同一份噪声 |
| 失败无结果并退还预留 | 失败事务：任务置 failed/active=0 + release(-ε)；不写结果，重试才重新采样 |
| 执行进程退出后自动接续，无需等新查询 | 每个进程的后台恢复线程周期扫描「租约过期且拥有者已退出」的任务并接管执行；启动即扫一轮，全量重启后同样自动接续 |
| 挂起（SIGSTOP）未退出的执行者不被接管 | 进程存活锁（共享卷上的 flock）：退出即释放、挂起仍持有；租约过期但拥有者存活时一律不接管，SIGCONT 后由原执行者完成 |
| 恢复与新请求并发争抢不重复执行/扣费 | 接管是对**同一任务行**的条件 UPDATE（数据库串行化，至多一方成功），沿用原预留，不写新 reserve |
| 任务状态及预算查询 | `GET /tasks`、`GET /tasks/{id}`、`GET /budget`；交互页 `GET /`，Swagger `GET /docs` |

## 二、目录结构

```
app/
  config.py            环境变量配置（库路径、租约、心跳、恢复间隔、演示延迟等）
  db.py                SQLite 连接、WAL、schema、写事务助手
  locks.py             进程存活锁（flock）：区分「已退出」与「被挂起」
  privacy.py           Laplace 噪声
  budget.py            预算聚合（used/reserved/remaining）
  models.py            请求/响应模型与站点集合规范化
  importer.py          CSV 解析与一次性导入
  tasks.py             选举、租约、心跳、后台恢复线程、预算事务（核心）
  api.py               FastAPI 路由
  static/index.html    交互式演示页面（导入/查询/预算/任务）
tests/                 30 个测试（单元 + 单进程 + 双真实进程集成，
                       含 SIGKILL 自动恢复、SIGSTOP 挂起不接管、并发争抢）
Dockerfile  docker-compose.yml  nginx.conf  sample_rides.csv
```

## 三、启动（Docker Compose）

```bash
docker compose up --build
# 交互入口（nginx 轮询两个 API 进程，响应头 X-Worker-Id 标识实际进程）
#   演示页面:  http://localhost:8080/
#   Swagger:   http://localhost:8080/docs
```

拓扑：`api-a`、`api-b` 两个独立 uvicorn 进程，挂载同一个命名卷上的 SQLite 文件；
`nginx` 默认轮询负载均衡。数据库文件落在 `dp_data` 卷的 `/data/dp_transit.db`。

重置环境（清空数据与预算）：

```bash
docker compose down -v
```

## 四、快速演示（curl）

```bash
# 1. 导入（仅一次）
curl -X POST localhost:8080/import -F "file=@sample_rides.csv;type=text/csv"

# 2. 首次查询：预留并实扣 ε=0.2
curl -X POST localhost:8080/query -H 'Content-Type: application/json' \
  -d '{"stations":["人民广场","陆家嘴"],"epsilon":0.2}'

# 3. 逆序同集合同 ε：reused=true、charged_epsilon=0、噪声完全相同
curl -X POST localhost:8080/query -H 'Content-Type: application/json' \
  -d '{"stations":["陆家嘴","人民广场"],"epsilon":0.2}'

# 4. 预算与任务
curl localhost:8080/budget
curl localhost:8080/tasks
```

## 五、本地无 Docker 运行与测试

```bash
pip install -r requirements.txt
# 两个进程共享同一个 DB 文件
DP_DB_PATH=/tmp/dp.db uvicorn app.api:app --port 9101 &
DP_DB_PATH=/tmp/dp.db uvicorn app.api:app --port 9102 &

pytest -q          # 30 个测试，含双进程并发去重、SIGKILL 自动恢复、SIGSTOP 挂起不接管
```

测试/演示用环境变量：

- `DP_SIMULATED_EXEC_DELAY`：执行阶段人为延迟秒数，制造“已预留未提交”窗口，
  便于观察在途任务与崩溃接续；生产置 0（默认）。
- `DP_LEASE_SECONDS`（默认 15）、`DP_HEARTBEAT_SECONDS`（默认 3）。
- `DP_RECOVERY_INTERVAL`（默认 1）：后台恢复线程扫描间隔秒数。
- `DP_LOCK_DIR`：进程存活锁目录，默认与数据库同目录的 `locks/`（随之共享）。

## 六、关键设计说明

1. **协调只靠数据库**：无内存锁、无外部队列。跨进程互斥由
   `BEGIN IMMEDIATE` 写事务 + `tasks` 上的部分唯一索引
   （`WHERE active=1`）强制，并发 INSERT 竞态由 SQLite 约束拒绝并重试。
2. **预算账本不可变**：只追加带符号条目，`reserved = Σreserve + Σrelease`、
   `used = |Σcommit|`，任何状态都可重放审计；拒绝预留时不写任何条目。
3. **崩溃接续不重复扣费**：接管是对**同一任务行**的条件 UPDATE，沿用其预留；
   成功提交时只做一次 release+commit。恢复线程与新请求的接管争抢由同一条
   条件 UPDATE 串行化，至多一方成为执行者。
4. **退出与挂起的区分（进程存活锁）**：每个进程在共享卷的 `locks/` 目录持有
   一个 flock 排他锁。进程退出（含 SIGKILL）时内核自动释放锁，其他进程据此
   接管其遗留任务；进程被 SIGSTOP 挂起时锁仍持有，租约即使过期也**不**接管，
   SIGCONT 后原执行者续租并自己完成。锁检测异常时保守视为存活，绝不误接管。
5. **后台恢复线程**：每个进程周期（`DP_RECOVERY_INTERVAL`）扫描
   「active 且租约过期」的任务，确认拥有者锁已释放后才接管并在后台执行，
   没有新查询时存活服务也会完成退出进程遗留的任务；进程启动时立即扫描一轮，
   全部服务重启后同样自动接续。
6. **真值隔离**：`rides` 表只服务于瞬时 `COUNT(DISTINCT)` 统计，
   `query_results` 仅保存加噪结果，任何接口都不返回真值。

## 七、崩溃/挂起恢复演示（compose 环境）

```bash
# 终端 1：制造一个慢查询（执行窗口 20s），预留 ε=0.2 后任务在途
DP_SIMULATED_EXEC_DELAY=20 docker compose up -d --build   # 或给单个服务设置该环境变量
curl -X POST localhost:8080/query -H 'Content-Type: application/json' \
  -d '{"stations":["人民广场"],"epsilon":0.2}' &

# 终端 2：直接杀掉一个 API 容器（模拟崩溃）
docker compose kill -s SIGKILL api-a

# 观察：无需再发任何查询，存活进程会在租约过期后自动接续；
# 任务与预算变化可在 Swagger（/docs）或演示页（/）实时查看：
curl localhost:8080/tasks    # generation 递增、worker 切换、status -> succeeded
curl localhost:8080/budget   # reserved 归零、used 增加 0.2，全程只有一条 reserve

# 挂起而非退出（租约到期也不会被接管，SIGCONT 后由原进程完成）：
docker compose kill -s SIGSTOP api-a
curl localhost:8080/tasks    # 任务保持 running、generation 不变、预留保持
docker compose kill -s SIGCONT api-a
```
