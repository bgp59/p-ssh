#! /usr/bin/env python3

"""
asyncio execution of parallel ssh/rsync commands
"""

import asyncio
import os
import pwd
import time
from typing import Dict, Iterable, List, Optional, Tuple, Union

from .log import AuditLogger
from .p_task import PRemoteTask, PTask, PTaskOutDisp, PTaskResult

# The following placeholders may appear in work_dir:
LOCAL_HOSTNAME_PLACEHOLDER = "%n%"  # -> uname -n, lowercase, stripped of domain
PID_PLACEHODLER = "%p%"  # -> PID

# Env var with the default working dir root:
P_SSH_WORKING_DIR_ROOT_ENV_VAR = "P_SSH_WORKING_DIR_ROOT"


async def _run_p_batch(
    p_tasks: List[PTask],
    n_parallel: int = 1,
    batch_timeout: float = 0,
    batch_cancel_max_wait: float = 0,
) -> Dict[PTask, PTaskResult]:

    results: Dict[PTask, PTaskResult] = dict()
    p_task_by_task: Dict[asyncio.Task, PTask] = dict()
    pending = set()
    i = 0
    start_ts = time.time()
    if batch_timeout is not None and batch_timeout <= 0:
        batch_timeout = None
    while True:
        while i < len(p_tasks) and (n_parallel <= 0 or len(pending) < n_parallel):
            p_task = p_tasks[i]
            i += 1
            task = p_task.task
            p_task_by_task[task] = p_task
            pending.add(task)
        if not pending:
            break
        if batch_timeout is not None:
            batch_timeout -= time.time() - start_ts
            if batch_timeout <= 0:
                break
        done, pending = await asyncio.wait(
            pending, timeout=batch_timeout, return_when="FIRST_COMPLETED"
        )
        for task in done:
            p_task = p_task_by_task[task]
            results[p_task] = task.result()
    # Everything left in pending should be cancelled at this point. Issue
    # cancellation twice since the first one relies on SIGTERM which may be
    # ignored.
    for timeout in [
        max(batch_cancel_max_wait, 0),
        None,
    ]:
        if not pending:
            break
        for task in pending:
            task.cancel()
            done, pending = await asyncio.wait(
                pending, timeout=timeout, return_when="ALL_COMPLETED"
            )
            for task in done:
                p_task = p_task_by_task[task]
                results[p_task] = task.result()
    # If there is anything left at this point (there shouldn't be really)
    # declare them as lingering:
    for task in pending:
        p_task = p_task_by_task[task]
        p_task.log_completion(force_linger=True)
        results[p_task] = p_task.result

    return results


def run_p_batch(
    p_tasks: List[PTask],
    n_parallel: int = 1,
    batch_timeout: float = 0,
    batch_cancel_max_wait: float = 0,
) -> Dict[PTask, PTaskResult]:
    """
    Run a batch of tasks in parallel.

    Args:
        p_tasks (list): List of PTask objects

        n_parallel (int): Number tasks to run in parallel, use 0 for unlimited

        batch_timeout (float): Timeout in seconds for the whole batch, use 0 for
            no timeout

        batch_cancel_max_wait (float): How long to wait, in seconds, for a batch
            cancellation to complete. Use 0 to not wait at all.

    Return:
        dict: key = PTask, value = PTaskResult

    """

    return asyncio.run(
        _run_p_batch(p_tasks, n_parallel, batch_timeout, batch_cancel_max_wait)
    )


def _uname():
    return os.uname().nodename.lower().split(".", 1)[0]


def expand_working_dir(working_dir: str) -> str:
    """Expand env vars timestamp and placeholders"""

    working_dir = os.path.expandvars(working_dir)
    for ph, val in [
        (LOCAL_HOSTNAME_PLACEHOLDER, _uname()),
        (PID_PLACEHODLER, str(os.getpid())),
    ]:
        working_dir = working_dir.replace(ph, val)
    working_dir = os.path.expandvars(working_dir)
    working_dir = time.strftime(working_dir)
    return working_dir


def get_default_working_dir(
    working_dir_root: Optional[str] = None,
    comp: Optional[str] = None,
) -> str:
    if working_dir_root is None:
        working_dir_root = os.environ.get(P_SSH_WORKING_DIR_ROOT_ENV_VAR)
    if working_dir_root is None:
        root_dir = os.environ.get("HOME")
        if root_dir is None and not os.path.isdir(root_dir):
            uid = os.getuid()
            try:
                user = pwd.getpwuid(uid).pw_name
            except KeyError:
                user = os.environ.get("USER") or os.environ.get("LOGIN") or f"uid-{uid}"
            root_dir = os.path.join("/tmp", user)
        working_dir_root = os.path.join(root_dir, __package__, "work")
    return os.path.join(
        working_dir_root,
        _uname(),
        comp or "",
        time.strftime(f"%Y-%m-%dT%H:%M:%S%z-{os.getpid()}"),
    )


def run_p_remote_batch(
    cmd: str,
    host_spec_list: Iterable[str],
    args: Optional[Union[Iterable, str, int, float]] = None,
    timeout: Optional[float] = None,
    term_max_wait: Optional[float] = None,
    input_fname: Optional[str] = None,
    n_parallel: int = 1,
    batch_timeout: float = 0,
    batch_cancel_max_wait: float = 0,
    working_dir: Optional[str] = None,
    verbose: Optional[bool] = None,
) -> Tuple[Dict[PRemoteTask, PTaskResult], Optional[str]]:
    """Run a remote command on a set of target hosts.

    Args:
        cmd (str): the command to execute

        args (tuple): optional args, subject to host specification placeholder
            replacement (see p_task..._PLACEHOLDER)

        timeout (float): if not None, the max time, in seconds, to wait for the
            command completion.

        term_max_wait (float): if not None, the max time, in seconds, to
            wait for command termination via SIGTERM. The latter will be sent in
            case of timeout or cancellation. Should term_max_wait expire, the
            command will be killed (via SIGKILL).

        input_fname (str): Optional file name to be used as stdin to the
            command, rather than devnull.

        n_parallel (int): Number tasks to run in parallel, use 0 for unlimited

        batch_timeout (float): Timeout in seconds for the whole batch, use 0 for
            no timeout

        batch_cancel_max_wait (float): How long to wait, in seconds, for a batch
            cancellation to complete. Use 0 to not wait at all.

        working_dir (str): If not None, the top root dir for audit trail and
            output collection. The audit trail will be working_dir/audit.jsonl
            and the out dir will be working_dir/out.

        out_disp (PTaskOutDisp): If not None, set the output disposition.
            If None, the output disposition will be set to record if working_dir
            is set or to PASS_THRU otherwise.

    Returns:
        results, audit_trail_fname

    """

    if working_dir is not None:
        out_dir = os.path.join(working_dir, "out")
        os.makedirs(out_dir, exist_ok=True)
        audit_trail_fname = os.path.join(working_dir, "audit.jsonl")
        logger = AuditLogger(fh_or_fname=audit_trail_fname)
    else:
        logger = None
        audit_trail_fname = None

    if out_disp is None:
        out_disp = (
            PTaskOutDisp.PASS_THRU if working_dir is None else PTaskOutDisp.RECORD
        )

    p_tasks = [
        PRemoteTask(
            cmd,
            args=args,
            host_spec=host_spec,
            input_fname=input_fname,
            timeout=timeout,
            term_max_wait=term_max_wait,
            out_disp=out_disp,
            out_dir=out_dir,
            logger=logger,
        )
        for host_spec in host_spec_list
    ]

    results = asyncio.run(
        _run_p_batch(p_tasks, n_parallel, batch_timeout, batch_cancel_max_wait)
    )

    return results, audit_trail_fname
