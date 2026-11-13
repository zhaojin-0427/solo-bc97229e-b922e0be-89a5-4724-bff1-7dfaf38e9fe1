"""单进程恢复语义：进程存活锁、遗留任务自动接续、挂起不接管、等待期间读响应。

这些测试不依赖真实多进程信号，直接用 flock 语义模拟：
- 无锁文件的 worker ≈ 已退出进程（内核已释放锁）；
- 测试进程亲自持有某 worker 的锁 ≈ 该进程仍存活（如被 SIGSTOP 挂起）。
跨进程真实 SIGSTOP/SIGKILL 场景见 test_two_processes.py。
"""
from __future__ import annotations

import fcntl
import io
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from app import locks, tasks
from app.db import get_conn, write_tx
from app.models import canonicalize, dumps_stations
from app.tasks import _utcnow

SAMPLE_CSV = (
    "匿名乘客号,上车站\n"
    "P1,A\nP2,A\nP1,B\nP3,B\nP4,A\nP2,C\nP5,C\nP1,C\nP6,A\nP3,A\n"
)


def _import(client):
    r = client.post(
        "/import",
        files={"file": ("rides.csv", io.BytesIO(SAMPLE_CSV.encode()), "text/csv")},
    )
    assert r.status_code == 200, r.text


def _insert_orphan_task(stations, epsilon, worker_id, lease_offset=-1.0):
    """伪造一个「进程退出后遗留」的在途任务：租约已过期 + 预留已入账。"""
    key, canon = canonicalize(stations)
    with get_conn() as conn:
        with write_tx(conn):
            conn.execute(
                "INSERT INTO tasks (canonical_key, epsilon, stations_json, status, active, "
                "worker_id, generation, reserved_epsilon, lease_expires_at, created_at, updated_at) "
                "VALUES (?, ?, ?, 'running', 1, ?, 1, ?, ?, ?, ?)",
                (key, epsilon, dumps_stations(canon), worker_id, epsilon,
                 time.time() + lease_offset, _utcnow(), _utcnow()),
            )
            task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute(
                "INSERT INTO budget_ledger (task_id, entry_type, epsilon, amount, created_at) "
                "VALUES (?, 'reserve', ?, ?, ?)",
                (task_id, epsilon, epsilon, _utcnow()),
            )
    return task_id, key


def _wait_task(task_id, pred, timeout=10.0, desc=""):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with get_conn() as conn:
            t = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if t is not None and pred(t):
            return t
        time.sleep(0.05)
    raise AssertionError(f"等待任务 {task_id} 状态超时: {desc}")


def _ledger_counts():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT entry_type, COUNT(*) c, SUM(amount) s FROM budget_ledger "
            "GROUP BY entry_type"
        ).fetchall()
    return {r["entry_type"]: (r["c"], r["s"]) for r in rows}


def _hold_worker_lock(worker_id):
    """在测试进程内持有某 worker 的锁，模拟「该进程仍存活（如被挂起）」。"""
    path = locks.lock_path_for(worker_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return fd


def test_worker_alive_lock_semantics(app_env):
    """flock 语义：持锁=存活（含挂起），无锁/锁可抢=已退出。"""
    client, _ = app_env
    # 从未注册过的 worker：无锁文件 → 视为已退出
    assert locks.worker_alive("ghost:0:none") is False

    wid = "fake-worker:4242:abcdef"
    fd = _hold_worker_lock(wid)
    try:
        assert locks.worker_alive(wid) is True  # 锁被持有 ≈ 进程存活/被挂起
    finally:
        os.close(fd)  # 关闭 fd ≈ 进程退出，内核释放锁
    assert locks.worker_alive(wid) is False

    # 本 API 进程自己的锁在 lifespan 中持有 → 自己永远判定自己存活
    assert locks.worker_alive(tasks.WORKER_ID) is True


def test_orphan_task_auto_recovered_without_new_query(app_env):
    """无新查询时，后台恢复线程自动完成退出进程遗留的任务并结算预算。"""
    client, _ = app_env
    _import(client)

    task_id, key = _insert_orphan_task(["A", "B"], 0.2, "dead-worker:1:aaaa")
    # 恢复线程周期 0.1s（conftest 补丁），无需任何新查询即应接续完成
    done = _wait_task(task_id, lambda t: t["status"] == "succeeded" and t["active"] == 0,
                      desc="orphan auto recovery")
    assert done["generation"] >= 2  # 被接管过一代
    assert done["worker_id"] == tasks.WORKER_ID  # 由本存活进程接续

    # 预算：沿用原预留结算，没有重复预留；reserved 归零，used=0.2
    ledger = _ledger_counts()
    assert ledger["reserve"] == (1, 0.2)
    assert ledger["release"] == (1, -0.2)
    assert ledger["commit"] == (1, -0.2)
    b = client.get("/budget").json()
    assert round(b["used"], 6) == 0.2 and b["reserved"] == 0

    # 结果已持久化：同集合（逆序）同 ε 永久复用，零扣费、同噪声
    with get_conn() as conn:
        row = conn.execute(
            "SELECT noisy_count FROM query_results WHERE canonical_key = ?", (key,)
        ).fetchone()
    assert row is not None
    r = client.post("/query", json={"stations": ["B", "A"], "epsilon": 0.2})
    assert r.status_code == 200
    body = r.json()
    assert body["reused"] is True and body["charged_epsilon"] == 0.0
    assert body["noisy_count"] == row["noisy_count"]


def test_suspended_owner_not_taken_over_then_resumes(app_env):
    """拥有者「仍存活」（锁被持有，模拟 SIGSTOP）时租约过期也不得接管；
    锁释放（≈进程退出或恢复）后才允许接续。"""
    client, _ = app_env
    _import(client)

    wid = "suspended-worker:7:bbbb"
    fd = _hold_worker_lock(wid)
    task_id, _ = _insert_orphan_task(["C"], 0.1, wid)
    try:
        # 给恢复线程足够多轮扫描机会（0.1s/轮）：始终不得接管
        time.sleep(1.0)
        with get_conn() as conn:
            t = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        assert t["status"] == "running" and t["active"] == 1
        assert t["generation"] == 1 and t["worker_id"] == wid
        # 预算仍被原预留占用（正确行为：执行者可能随时恢复并完成）
        b = client.get("/budget").json()
        assert b["reserved"] == 0.1 and b["used"] == 0
        assert _ledger_counts()["reserve"] == (1, 0.1)
    finally:
        os.close(fd)  # 释放锁 ≈ 拥有者退出，允许接续

    done = _wait_task(task_id, lambda t: t["status"] == "succeeded",
                      desc="takeover after owner exit")
    assert done["generation"] >= 2
    b = client.get("/budget").json()
    assert round(b["used"], 6) == 0.1 and b["reserved"] == 0
    ledger = _ledger_counts()
    assert ledger["reserve"] == (1, 0.1)  # 全程只有最初那一次预留
    assert ledger["commit"] == (1, -0.1)


def test_reads_stay_responsive_during_execution(app_env, monkeypatch):
    """执行窗口内（统计/等待期间），预算与任务查询保持响应，不被写阻塞。"""
    from app import config
    client, _ = app_env
    _import(client)
    monkeypatch.setattr(config, "SIMULATED_EXEC_DELAY", 2.0)

    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(client.post, "/query",
                          json={"stations": ["A"], "epsilon": 0.2})
        time.sleep(0.5)  # 确保查询已进入「已预留、未提交」的执行窗口
        t0 = time.monotonic()
        for _ in range(5):
            assert client.get("/budget").status_code == 200
            assert client.get("/tasks").status_code == 200
        elapsed = time.monotonic() - t0
        assert elapsed < 2.0, f"读请求被写路径阻塞: {elapsed:.2f}s"
        # 执行窗口内预算查询能看到在途预留
        b = client.get("/budget").json()
        assert b["reserved"] == 0.2
        assert fut.result(timeout=15).status_code == 200
