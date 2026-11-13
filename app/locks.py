"""进程存活锁：区分「进程已退出」与「进程被挂起（SIGSTOP）」。

每个 API 进程启动时在共享目录（与 SQLite 库同一文件系统；compose 中两个
容器挂载同一个命名卷）创建自己的锁文件，并对该文件持有 flock 排他锁，
直至进程退出：

- 进程退出（正常退出、崩溃、SIGKILL）：内核回收文件描述符，锁自动释放，
  其他进程据此判定其遗留任务可以接管；
- 进程被 SIGSTOP 挂起：锁仍然持有，租约即使过期其他进程也不得接管，
  SIGCONT 之后由原执行者自己续租并完成。

flock 是文件系统级语义，对挂载同一卷的多个容器/进程同样有效（本地卷、
命名卷均为本地文件系统，语义可靠）。

判定失败时一律保守返回「存活」：宁可暂时不接管，也绝不误接管，
保证「同一查询至多一个实际执行」不被破坏。
"""
from __future__ import annotations

import fcntl
import hashlib
import logging
import os
from pathlib import Path
from typing import Optional

from . import config

logger = logging.getLogger("dp_transit.locks")

_lock_fd: Optional[int] = None
_lock_path: Optional[Path] = None


def lock_dir() -> Path:
    """锁目录：默认与数据库文件同目录（随之共享/隔离），可用 DP_LOCK_DIR 覆盖。"""
    env = os.getenv("DP_LOCK_DIR")
    return Path(env) if env else config.DB_PATH.parent / "locks"


def lock_path_for(worker_id: str) -> Path:
    """worker_id 含冒号等字符，统一散列为安全的文件名。"""
    digest = hashlib.sha256(worker_id.encode("utf-8")).hexdigest()[:24]
    return lock_dir() / f"worker_{digest}.lock"


def acquire_process_lock(worker_id: str) -> None:
    """持有本进程的 worker 锁直到进程退出（幂等；路径变化时先释放旧锁）。"""
    global _lock_fd, _lock_path
    path = lock_path_for(worker_id)
    if _lock_fd is not None and _lock_path == path:
        return
    release_process_lock()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        os.close(fd)
        raise RuntimeError(f"worker 锁已被其他进程持有: {path}") from exc
    os.ftruncate(fd, 0)
    os.write(fd, f"{worker_id}\n".encode("utf-8"))
    _lock_fd, _lock_path = fd, path
    logger.info("worker lock acquired: %s (%s)", path, worker_id)


def release_process_lock() -> None:
    """显式释放（进程退出时内核也会自动释放，此处用于测试与路径切换）。"""
    global _lock_fd, _lock_path
    if _lock_fd is not None:
        try:
            os.close(_lock_fd)
        except OSError:
            pass
    _lock_fd, _lock_path = None, None


def worker_alive(worker_id: str) -> bool:
    """检测 worker 进程是否仍存活（被 SIGSTOP 挂起也算存活）。

    锁被持有 → 存活；锁可获取或锁文件不存在 → 已退出。
    检测过程出现任何异常都保守返回 True（视为存活，暂不接管）。
    """
    try:
        path = lock_path_for(worker_id)
        if not path.exists():
            return False
        fd = os.open(path, os.O_RDWR)
    except OSError:
        logger.exception("worker_alive: open lock file failed for %s", worker_id)
        return True
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True  # 锁被持有：进程仍存活（可能只是被挂起）
        # 抢到锁说明原持有者已退出；立即归还，不长期占用。
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def cleanup_stale_locks() -> None:
    """清理已退出进程残留的锁文件（本进程持有中的锁除外）。

    仅影响目录整洁：残留文件不清理也不影响正确性（worker_alive 按锁状态判定）。
    """
    directory = lock_dir()
    if not directory.exists():
        return
    for path in directory.glob("worker_*.lock"):
        if _lock_path is not None and path == _lock_path:
            continue
        try:
            fd = os.open(path, os.O_RDWR)
        except OSError:
            continue
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                continue  # 仍被存活进程持有
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
