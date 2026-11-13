"""FastAPI 路由与应用装配。"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from . import budget as budget_mod
from . import importer, locks, tasks
from .db import get_conn, init_db
from .models import BudgetResponse, ImportResponse, QueryRequest, QueryResponse, TaskOut
from .tasks import WORKER_ID

logger = logging.getLogger("dp_transit")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    # 进程存活锁：供其他进程区分「已退出」与「被挂起」，进程退出时内核自动释放。
    locks.acquire_process_lock(WORKER_ID)
    locks.cleanup_stale_locks()
    # 后台恢复线程：无新查询时也自动接续退出进程遗留的任务；
    # 启动即扫一轮，全部服务重启后同样自动接续。
    tasks.start_recovery()
    logger.info("worker %s started, db initialized", WORKER_ID)
    yield
    tasks.stop_recovery()


app = FastAPI(
    title="差分隐私公交客流查询 API",
    description=(
        "一次导入乘车 CSV 后只读；查询返回去重乘客数 + 独立 Laplace(0,1/ε) 噪声，"
        "不返回真值。总预算 1，先预留 ε 成功后转实扣；同集合同 ε 永久复用、不重复扣费；"
        "两个 API 进程共享 SQLite，相同查询复用在途任务；执行进程退出后由存活服务"
        "自动接续遗留任务（无需新查询，全量重启后同样恢复），被挂起的执行者不被接管。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def _identify_worker(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Worker-Id"] = WORKER_ID
    return response


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "worker_id": WORKER_ID}


@app.post("/import", response_model=ImportResponse)
async def import_csv(file: UploadFile) -> dict:
    content = await file.read()
    try:
        return importer.import_rides(content)
    except importer.AlreadyImported as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail=f"CSV 解析失败：{exc}") from exc


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest) -> dict:
    try:
        return tasks.submit_query(req.stations, req.epsilon)
    except tasks.NotImported as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except tasks.BudgetExhausted as exc:
        raise HTTPException(status_code=402, detail=str(exc)) from exc
    except tasks.QueryTimeout as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc


@app.get("/budget", response_model=BudgetResponse)
def budget() -> dict:
    # 纯读，不开写事务，不阻塞其他请求。
    with get_conn() as conn:
        return budget_mod.get_snapshot(conn).as_dict()


@app.get("/tasks", response_model=list[TaskOut])
def tasks_list(limit: int = 100) -> list[dict]:
    limit = max(1, min(limit, 1000))
    return tasks.list_tasks(limit)


@app.get("/tasks/{task_id}", response_model=TaskOut)
def task_detail(task_id: int) -> dict:
    out = tasks.get_task(task_id)
    if out is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return out


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(str(Path(__file__).parent / "static" / "index.html"))
