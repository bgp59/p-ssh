#! /usr/bin/env python3

"""
asyncio execution of parallel ssh/rsync commands
"""

import asyncio
import io
import os
import pwd
import shlex
import sys
import time
from typing import Callable, Dict, Iterable, List, Optional, Set, TextIO, Tuple, Union

from .log import AuditLogger, format_log_ts
from .p_task import PRemoteTask, PTask, PTaskCond, PTaskOutDisp, PTaskResult

# The following placeholders may appear in work_dir:
LOCAL_HOSTNAME_PLACEHOLDER = "{n}"  # -> uname -n, lowercase, stripped of domain
PID_PLACEHOLDER = "{p}"  # -> PID

# Env var with the default working dir root:
P_SSH_WORKING_DIR_ROOT_ENV_VAR = "P_SSH_WORKING_DIR_ROOT"


def _uname():
    return os.uname().nodename.lower().split(".", 1)[0]


def expand_working_dir(working_dir: str) -> str:
    """Expand env vars timestamp and placeholders"""

    working_dir = os.path.expandvars(working_dir)
    for ph, val in [
        (LOCAL_HOSTNAME_PLACEHOLDER, _uname()),
        (PID_PLACEHOLDER, str(os.getpid())),
    ]:
        working_dir = working_dir.replace(ph, val)
    working_dir = time.strftime(working_dir)
    return working_dir


def get_default_working_dir(
    working_dir_root: Optional[str] = None,
    comp: Optional[str] = None,
) -> str:
    if working_dir_root is None:
        working_dir_root = expand_working_dir(
            os.environ.get(P_SSH_WORKING_DIR_ROOT_ENV_VAR)
        )
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


class DisplayTaskResultCB:
    """Standard Task Completion Callback

    Class constructor

    Args:
        fh (TextIO): where to display the results

        ignore_cond (set): the set of conditions that would cause the task
            to be ignored from the results tally.

    """

    def __init__(
        self, fh: TextIO = sys.stdout, ignore_cond: Optional[Set[PTaskCond]] = None
    ):
        self._fh = fh
        self._ignore_cond = ignore_cond

    def __call__(self, p_task: PTask) -> bool:
        """The actual callback invoked w the completed task

        Returns (bool): True if task's condition not in ignore_cond
        """
        cmd_and_args = " ".join(
            map(shlex.quote, (p_task.cmd,) + (p_task.args or tuple()))
        )
        retcode = p_task.retcode
        cond = p_task.cond.name if p_task.cond is not None else None
        out_disp = p_task.out_disp
        start_ts = (
            format_log_ts(p_task.start_ts) if p_task.start_ts is not None else None
        )
        end_ts = format_log_ts(p_task.end_ts) if p_task.end_ts is not None else None
        runtime = (
            f"{p_task.end_ts - p_task.start_ts:0.6f}"
            if start_ts is not None and p_task.end_ts is not None
            else None
        )
        with p_task._lck:
            print(
                f"{format_log_ts()} - cmd: {cmd_and_args}, pid: {p_task.pid}"
                + f", start_ts: {start_ts}, end_ts: {end_ts}, runtime: {runtime}"
                + f", retcode: {retcode}, cond: {cond!r}",
                file=self._fh,
            )
            if out_disp != PTaskOutDisp.IGNORE:
                for what, data_or_path in [
                    ("stdout", p_task._stdout),
                    ("stderr", p_task._stderr),
                ]:
                    ends_with_nl = True
                    if p_task._out_disp in {PTaskOutDisp.COLLECT, PTaskOutDisp.AUDIT}:
                        print(f"{what}:", file=self._fh)
                        if data_or_path:
                            self._fh.write(str(data_or_path, "utf-8"))
                            ends_with_nl = data_or_path.endswith(b"\n")
                    elif p_task._out_disp == PTaskOutDisp.RECORD:
                        print(f"{what} ({data_or_path!r}):", file=self._fh)
                        with open(data_or_path, "rt") as f:
                            while True:
                                buf = f.read(io.DEFAULT_BUFFER_SIZE)
                                if len(buf) == 0:
                                    break
                                self._fh.write(buf)
                                ends_with_nl = buf.endswith("\n")
                    if not ends_with_nl:
                        self._fh.write("\n")
            self._fh.flush()
        return self._ignore_cond is None or cond not in self._ignore_cond


async def _run_p_batch(
    p_tasks: List[PTask],
    n_parallel: int = 1,
    batch_timeout: float = 0,
    batch_cancel_max_wait: float = 0,
    cb: Optional[Callable[[PTask], bool]] = None,
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
            if cb is None or cb(p_task):
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
                if cb is None or cb(p_task):
                    results[p_task] = task.result()
    # If there is anything left at this point (there shouldn't be really)
    # declare them as lingering:
    for task in pending:
        p_task = p_task_by_task[task]
        p_task.log_completion(force_linger=True)
        if cb is None or cb(p_task):
            results[p_task] = p_task.result()

    return results


def run_p_batch(
    p_tasks: List[PTask],
    n_parallel: int = 1,
    batch_timeout: float = 0,
    batch_cancel_max_wait: float = 0,
    cb: Optional[Callable[[PTask], bool]] = None,
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

        cb (callable): A callback to invoke with the task upon its completion.
            If it returns True the task should be recorded in the results,
            otherwise it should be ignored.

    Return:
        dict: key = PTask, value = PTaskResult

    """

    return asyncio.run(
        _run_p_batch(p_tasks, n_parallel, batch_timeout, batch_cancel_max_wait, cb)
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
    out_disp: Optional[PTaskOutDisp] = None,
    cb: Optional[Callable[[PTask], bool]] = None,
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
            is set or to COLLECT otherwise.

        cb (callable): A callback to invoke with the task upon its completion.
            If it returns True the task should be recorded in the results,
            otherwise it should be ignored.


    Returns:
        results, audit_trail_fname

    """

    if working_dir is not None:
        working_dir = os.path.abspath(working_dir)
        out_dir = os.path.join(working_dir, "out")
        os.makedirs(out_dir, exist_ok=True)
        audit_trail_fname = os.path.join(working_dir, "audit.jsonl")
        logger = AuditLogger(fh_or_fname=audit_trail_fname)
    else:
        logger = None
        audit_trail_fname = None
        out_dir = None

    if out_disp is None:
        out_disp = PTaskOutDisp.COLLECT if working_dir is None else PTaskOutDisp.RECORD

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
        _run_p_batch(p_tasks, n_parallel, batch_timeout, batch_cancel_max_wait, cb)
    )

    return results, audit_trail_fname
