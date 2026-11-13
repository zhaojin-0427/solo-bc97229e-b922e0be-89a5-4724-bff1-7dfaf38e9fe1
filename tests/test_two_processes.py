"""跨进程集成测试：启动两个真实 uvicorn 进程共享同一个 SQLite 文件。

覆盖题目最关键的分布式语义：
1. 相同查询的并发请求只有一个执行者、一份噪声、一次扣费；
2. 任一进程都能复用另一个进程持久化的结果；
3. 执行进程退出（SIGKILL）后：无新查询时存活进程的后台恢复线程自动接续；
   全部服务重启后新进程同样自动接续；新查询路径的租约接管也保留；
4. 执行进程被 SIGSTOP 挂起（未退出）时租约过期也不被接管，SIGCONT 后由原
   执行者完成；
5. 恢复与新请求并发争抢同一任务时，只有一个实际执行，沿用原预留不重复扣费；
6. 存活执行的租约会被心跳续租，不会被其他进程接管。
"""
from __future__ import annotations

import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(port: int, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as r:
                if r.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError):
            time.sleep(0.2)
    raise RuntimeError(f"server on {port} not ready")


class Server:
    def __init__(self, db_path: Path, lease: float = 2.0, heartbeat: float = 0.4,
                 exec_delay: float = 0.0, recovery: float = 0.3):
        self.port = _free_port()
        self.db_path = db_path
        env = os.environ.copy()
        env.update({
            "DP_DB_PATH": str(db_path),
            "DP_LEASE_SECONDS": str(lease),
            "DP_HEARTBEAT_SECONDS": str(heartbeat),
            "DP_QUERY_WAIT_TIMEOUT": "40",
            "DP_SIMULATED_EXEC_DELAY": str(exec_delay),
            "DP_RECOVERY_INTERVAL": str(recovery),
            "PYTHONPATH": str(ROOT),
        })
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.api:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=str(ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        _wait_ready(self.port)

    def post(self, path: str, payload: dict) -> httpx.Response:
        return httpx.post(f"http://127.0.0.1:{self.port}{path}", json=payload, timeout=60)

    def get(self, path: str) -> httpx.Response:
        return httpx.get(f"http://127.0.0.1:{self.port}{path}", timeout=10)

    def stop(self) -> None:
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()

    def kill(self) -> None:
        """模拟崩溃：SIGKILL，没有任何清理机会。"""
        self.proc.kill()
        self.proc.wait(timeout=10)


def _import_csv(server: Server, tmp_path: Path) -> None:
    csv = (ROOT / "sample_rides.csv").read_bytes()
    f = tmp_path / "rides.csv"
    f.write_bytes(csv)
    with open(f, "rb") as fh:
        r = httpx.post(f"http://127.0.0.1:{server.port}/import",
                       files={"file": ("rides.csv", fh, "text/csv")}, timeout=30)
    assert r.status_code == 200, r.text


def _db_rows(db_path: Path, sql: str, args: tuple = ()) -> list[sqlite3.Row]:
    conn = sqlite3.connect(str(db_path), timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(sql, args).fetchall()
    finally:
        conn.close()


def _latest_task(db_path: Path) -> sqlite3.Row | None:
    rows = _db_rows(db_path, "SELECT * FROM tasks ORDER BY id DESC LIMIT 1")
    return rows[0] if rows else None


def _wait_task(db_path: Path, pred, timeout: float = 20.0, desc: str = "") -> sqlite3.Row:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            t = _latest_task(db_path)
        except sqlite3.OperationalError:
            t = None
        if t is not None and pred(t):
            return t
        time.sleep(0.1)
    raise AssertionError(f"等待任务状态超时: {desc}")


def _ledger_counts(db_path: Path) -> dict:
    rows = _db_rows(
        db_path,
        "SELECT entry_type, COUNT(*) c, SUM(amount) s FROM budget_ledger "
        "GROUP BY entry_type",
    )
    return {r["entry_type"]: (r["c"], r["s"]) for r in rows}


@pytest.fixture()
def cluster(tmp_path):
    db_path = tmp_path / "shared.db"
    a = Server(db_path)
    b = Server(db_path)
    _import_csv(a, tmp_path)
    yield a, b, db_path
    for s in (a, b):
        try:
            s.stop()
        except Exception:
            pass


def test_two_processes_share_cache_and_single_charge(cluster):
    a, b, _ = cluster
    payload = {"stations": ["人民广场", "陆家嘴"], "epsilon": 0.2}

    r1 = a.post("/query", payload)
    assert r1.status_code == 200
    first = r1.json()
    assert first["reused"] is False

    # 另一进程：同集合逆序 + 同 ε，必须复用同一噪声且不扣费
    r2 = b.post("/query", {"stations": ["陆家嘴", "人民广场"], "epsilon": 0.2})
    assert r2.status_code == 200
    second = r2.json()
    assert second["reused"] is True
    assert second["noisy_count"] == first["noisy_count"]
    assert second["charged_epsilon"] == 0.0

    ba, bb = a.get("/budget").json(), b.get("/budget").json()
    assert ba == bb and round(ba["used"], 6) == 0.2 and ba["reserved"] == 0


def test_concurrent_identical_queries_single_execution(cluster):
    a, b, _ = cluster
    payload = {"stations": ["静安寺", "南京西路"], "epsilon": 0.5}

    # 两个进程同时收到完全相同的查询：只能有一个执行者
    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(
            lambda srv: srv.post("/query", payload),
            [a, b, a, b, a, b, a, b],
        ))
    assert all(r.status_code == 200 for r in responses), [r.text for r in responses]
    noises = {r.json()["noisy_count"] for r in responses}
    keys = {r.json()["canonical_key"] for r in responses}
    assert len(noises) == 1 and len(keys) == 1  # 同一份噪声，没有重复采样
    leaders = [r for r in responses if r.json()["reused"] is False]
    assert len(leaders) == 1  # 只有一个请求真正执行并扣费

    budget = a.get("/budget").json()
    assert round(budget["used"], 6) == 0.5 and budget["reserved"] == 0
    tasks_ = a.get("/tasks").json()
    matching = [t for t in tasks_ if t["epsilon"] == 0.5 and t["status"] == "succeeded"]
    assert len(matching) == 1  # 只有一个成功任务


def test_budget_reservation_shared_between_processes(cluster):
    a, b, _ = cluster
    # 在 a 上消耗 0.5，b 视角必须立刻可见并继续受总预算约束
    assert a.post("/query", {"stations": ["人民广场"], "epsilon": 0.5}).status_code == 200
    assert round(b.get("/budget").json()["remaining"], 6) == 0.5
    assert b.post("/query", {"stations": ["陆家嘴"], "epsilon": 0.5}).status_code == 200
    r = a.post("/query", {"stations": ["静安寺"], "epsilon": 0.1})
    assert r.status_code == 402  # 两进程合计已用满 1.0


def test_crash_lease_takeover(tmp_path):
    """执行进程在计算阶段崩溃（SIGKILL）：新查询路径等租约过期后接管。

    本用例关闭 b 的自动恢复（recovery 间隔调到极大），专测「新查询触发
    接管」这条兼容路径；自动恢复由后续用例覆盖。
    """
    db_path = tmp_path / "crash.db"
    # 短租约便于测试：2s 过期，心跳 0.5s；a 的执行窗口拉长到 5s 确保崩溃时任务在途
    a = Server(db_path, lease=2.0, heartbeat=0.5, exec_delay=5.0)
    b = Server(db_path, lease=2.0, heartbeat=0.5, exec_delay=0.0, recovery=3600.0)
    try:
        _import_csv(a, tmp_path)
        payload = {"stations": ["人民广场", "静安寺", "陆家嘴"], "epsilon": 0.1}

        def crash_query():
            try:
                return a.post("/query", payload)
            except (httpx.ReadError, httpx.ConnectError, httpx.RemoteProtocolError):
                return None

        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(crash_query)
            # 等待任务在共享库中出现（预留已落库），随后杀死 a
            _wait_task(db_path, lambda t: t["active"] == 1 and t["status"] == "running",
                       timeout=10, desc="在途任务出现")
            a.kill()
            fut.result(timeout=10)

        # 此时预留仍占着预算；b 的新查询等租约过期后接管并完成
        t0 = time.time()
        r = b.post("/query", payload)
        elapsed = time.time() - t0
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["reused"] is False  # 崩溃者没提交结果，由 b 实际完成
        assert elapsed >= 1.2, f"应等待租约过期，实际 {elapsed:.2f}s"

        # 任务代次增加，且工作者属于 b
        tasks_ = b.get("/tasks").json()
        gens = [(t["generation"], t["status"], t["worker_id"]) for t in tasks_]
        assert any(g >= 2 for g, s, _ in gens if s == "succeeded"), gens

        budget = b.get("/budget").json()
        assert round(budget["used"], 6) == 0.1 and budget["reserved"] == 0
        # 接管沿用原预留：全程只有一条 reserve
        assert _ledger_counts(db_path)["reserve"][0] == 1
    finally:
        try:
            b.stop()
        except Exception:
            pass


def test_orphan_task_auto_recovered_without_new_query(tmp_path):
    """执行进程 SIGKILL 后：不发任何新查询，存活进程的恢复线程自动接续。"""
    db_path = tmp_path / "orphan.db"
    a = Server(db_path, lease=2.0, heartbeat=0.4, exec_delay=30.0, recovery=0.2)
    b = Server(db_path, lease=2.0, heartbeat=0.4, exec_delay=0.0, recovery=0.2)
    try:
        _import_csv(a, tmp_path)
        payload = {"stations": ["静安寺", "南京西路"], "epsilon": 0.1}

        def doomed_query():
            try:
                return a.post("/query", payload)
            except (httpx.ReadError, httpx.ConnectError, httpx.RemoteProtocolError):
                return None

        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(doomed_query)
            orphan = _wait_task(
                db_path, lambda t: t["active"] == 1 and t["status"] == "running",
                timeout=10, desc="在途任务出现")
            a.kill()  # 预留已入账，进程直接消失
            fut.result(timeout=10)

        # 预算被遗留预留占住；此刻不向任何进程发新查询
        assert _ledger_counts(db_path)["reserve"] == (1, 0.1)

        # b 的后台恢复线程应在租约过期后自动接管并完成（轮询共享库确认）
        done = _wait_task(
            db_path,
            lambda t: t["status"] == "succeeded" and t["active"] == 0
            and t["generation"] >= 2,
            timeout=20, desc="b 自动接续完成")
        assert done["id"] == orphan["id"]  # 同一任务行被接管，而非新建
        assert done["worker_id"] != orphan["worker_id"]  # 执行者已从 a 换成 b

        # 预算结算：沿用原预留转实扣，reserved 归零，无重复预留/扣费
        ledger = _ledger_counts(db_path)
        assert ledger["reserve"] == (1, 0.1)
        assert ledger["release"] == (1, -0.1)
        assert ledger["commit"] == (1, -0.1)
        budget = b.get("/budget").json()
        assert round(budget["used"], 6) == 0.1 and budget["reserved"] == 0

        # 结果已持久化：此后再发同查询只复用，不再扣费
        r = b.post("/query", payload)
        assert r.status_code == 200
        assert r.json()["reused"] is True and r.json()["charged_epsilon"] == 0.0
    finally:
        try:
            b.stop()
        except Exception:
            pass


def test_full_restart_auto_recovery(tmp_path):
    """全部服务都退出后，新启动的进程无需新查询即自动接续遗留任务。"""
    db_path = tmp_path / "restart.db"
    a = Server(db_path, lease=2.0, heartbeat=0.4, exec_delay=30.0, recovery=0.2)
    _import_csv(a, tmp_path)
    payload = {"stations": ["陆家嘴"], "epsilon": 0.2}

    def doomed_query():
        try:
            return a.post("/query", payload)
        except (httpx.ReadError, httpx.ConnectError, httpx.RemoteProtocolError):
            return None

    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(doomed_query)
        orphan = _wait_task(db_path,
                            lambda t: t["active"] == 1 and t["status"] == "running",
                            timeout=10, desc="在途任务出现")
        a.kill()  # 此时已没有任何在线服务
        fut.result(timeout=10)

    # 全新进程（新 worker_id）挂载同一库启动：启动即扫描并自动接续
    c = Server(db_path, lease=2.0, heartbeat=0.4, exec_delay=0.0, recovery=0.2)
    try:
        done = _wait_task(
            db_path,
            lambda t: t["status"] == "succeeded" and t["generation"] >= 2,
            timeout=20, desc="重启后自动接续")
        assert done["worker_id"] != orphan["worker_id"]

        ledger = _ledger_counts(db_path)
        assert ledger["reserve"] == (1, 0.2)   # 只有最初那一次预留
        assert ledger["commit"] == (1, -0.2)
        budget = c.get("/budget").json()
        assert round(budget["used"], 6) == 0.2 and budget["reserved"] == 0

        r = c.post("/query", payload)
        assert r.json()["reused"] is True and r.json()["charged_epsilon"] == 0.0
    finally:
        try:
            c.stop()
        except Exception:
            pass


def test_sigstop_suspended_leader_not_taken_over(tmp_path):
    """执行进程被 SIGSTOP 挂起（未退出）：租约过期也不得接管；
    SIGCONT 后仍由原执行者完成，预算只结算一次。"""
    db_path = tmp_path / "sigstop.db"
    a = Server(db_path, lease=2.0, heartbeat=0.3, exec_delay=10.0, recovery=0.2)
    b = Server(db_path, lease=2.0, heartbeat=0.3, exec_delay=0.0, recovery=0.2)
    try:
        _import_csv(a, tmp_path)
        payload = {"stations": ["人民广场", "陆家嘴"], "epsilon": 0.2}

        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_a = pool.submit(lambda: a.post("/query", payload))
            task = _wait_task(db_path,
                              lambda t: t["active"] == 1 and t["status"] == "running",
                              timeout=10, desc="在途任务出现")
            owner = task["worker_id"]

            # 挂起 a：进程仍存在（锁仍持有），只是停止调度
            os.kill(a.proc.pid, signal.SIGSTOP)
            try:
                time.sleep(2.5)  # 租约（2s）明确过期，b 的恢复线程已扫描多轮
                # 租约过期后再从 b 发同查询：也只能跟随等待，不得接管
                fut_b = pool.submit(lambda: b.post("/query", payload))
                time.sleep(2.0)  # 给 b 的恢复线程与跟随者重选举充分争抢窗口

                t = _latest_task(db_path)
                assert t["status"] == "running" and t["active"] == 1
                assert t["generation"] == 1, "挂起的执行者不得被接管"
                assert t["worker_id"] == owner
                # 预算仍被原预留占用，且绝无重复预留
                assert _ledger_counts(db_path)["reserve"] == (1, 0.2)
                assert not fut_b.done() or fut_b.exception() is None
            finally:
                os.kill(a.proc.pid, signal.SIGCONT)  # 恢复：原执行者继续

            ra = fut_a.result(timeout=40)
            rb = fut_b.result(timeout=40)

        assert ra.status_code == 200, ra.text
        assert rb.status_code == 200, rb.text
        # 原执行者完成：a 是 leader，b 复用同一份噪声
        assert ra.json()["reused"] is False
        assert rb.json()["noisy_count"] == ra.json()["noisy_count"]

        t = _latest_task(db_path)
        assert t["status"] == "succeeded" and t["generation"] == 1
        assert t["worker_id"] == owner  # 自始至终归属未变
        ledger = _ledger_counts(db_path)
        assert ledger["reserve"] == (1, 0.2)
        assert ledger["release"] == (1, -0.2)
        assert ledger["commit"] == (1, -0.2)
        budget = b.get("/budget").json()
        assert round(budget["used"], 6) == 0.2 and budget["reserved"] == 0
    finally:
        for s in (a, b):
            try:
                s.stop()
            except Exception:
                pass


def test_recovery_races_new_request_single_execution(tmp_path):
    """恢复线程与新请求并发争抢同一遗留任务：只有一个实际执行，
    沿用原预留，不重复预留或扣费。"""
    db_path = tmp_path / "race.db"
    a = Server(db_path, lease=1.5, heartbeat=0.3, exec_delay=30.0, recovery=0.2)
    b = Server(db_path, lease=1.5, heartbeat=0.3, exec_delay=0.0, recovery=0.2)
    c = Server(db_path, lease=1.5, heartbeat=0.3, exec_delay=0.0, recovery=0.2)
    try:
        _import_csv(a, tmp_path)
        payload = {"stations": ["南京西路", "静安寺"], "epsilon": 0.5}

        def doomed_query():
            try:
                return a.post("/query", payload)
            except (httpx.ReadError, httpx.ConnectError, httpx.RemoteProtocolError):
                return None

        with ThreadPoolExecutor(max_workers=3) as pool:
            fut_a = pool.submit(doomed_query)
            orphan = _wait_task(db_path,
                                lambda t: t["active"] == 1 and t["status"] == "running",
                                timeout=10, desc="在途任务出现")
            a.kill()
            fut_a.result(timeout=10)

            # 租约过期前同时向 b、c 发同查询（先成为跟随者）；
            # 过期瞬间：b/c 的恢复线程 + 两个请求的重新选举四方争抢同一任务。
            fut_b = pool.submit(lambda: b.post("/query", payload))
            fut_c = pool.submit(lambda: c.post("/query", payload))
            rb, rc = fut_b.result(timeout=40), fut_c.result(timeout=40)

        assert rb.status_code == 200 and rc.status_code == 200
        assert rb.json()["noisy_count"] == rc.json()["noisy_count"]  # 同一份噪声

        # 同一任务行被恰好接管一次（generation 1→2），没有新建任务、没有双执行
        rows = _db_rows(db_path, "SELECT * FROM tasks WHERE canonical_key = ?",
                        (orphan["canonical_key"],))
        assert len(rows) == 1
        assert rows[0]["id"] == orphan["id"]
        assert rows[0]["generation"] == 2
        assert rows[0]["status"] == "succeeded"

        # 账本：只有最初一次预留；接管不新增预留；成功只结算一次
        ledger = _ledger_counts(db_path)
        assert ledger["reserve"] == (1, 0.5)
        assert ledger["release"] == (1, -0.5)
        assert ledger["commit"] == (1, -0.5)
        budget = b.get("/budget").json()
        assert round(budget["used"], 6) == 0.5 and budget["reserved"] == 0
    finally:
        for s in (b, c):
            try:
                s.stop()
            except Exception:
                pass


def test_live_leader_is_not_taken_over(cluster):
    """存活执行者持续心跳，租约不断续期；另一进程只能跟随，不能接管。"""
    a, b, _ = cluster
    # 先在 a 上完成一次普通查询建立基线
    base = a.post("/query", {"stations": ["南京西路"], "epsilon": 0.2}).json()
    assert base["reused"] is False

    # 新查询并发打到两个进程；检查最终任务只有一代（generation=1），
    # 没有发生租约接管
    payload = {"stations": ["人民广场", "南京西路", "静安寺"], "epsilon": 0.1}
    with ThreadPoolExecutor(max_workers=2) as pool:
        rs = list(pool.map(lambda s: s.post("/query", payload), [a, b]))
    assert all(r.status_code == 200 for r in rs)
    assert rs[0].json()["noisy_count"] == rs[1].json()["noisy_count"]

    tasks_ = a.get("/tasks?limit=10").json()
    target = [t for t in tasks_ if t["canonical_key"] == rs[0].json()["canonical_key"]
              and t["epsilon"] == 0.1]
    assert len(target) == 1 and target[0]["generation"] == 1
    assert target[0]["status"] == "succeeded" and target[0]["active"] is False
