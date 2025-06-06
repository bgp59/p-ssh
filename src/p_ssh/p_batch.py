#! /usr/bin/env python3

"""
asyncio execution of parallel ssh/rsync commands
"""

import asyncio
import io
import os
import pwd
import shlex
import signal
import sys
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, Iterable, List, Optional, TextIO, Tuple, Union

from .log import AuditLogger, Level, format_log_ts
from .p_task import PRemoteTask, PTask, PTaskOutDisp

# The following placeholders may appear in work_dir:
LOCAL_HOSTNAME_PLACEHOLDER = "{lh}"  # -> uname -n, lowercase, stripped of domain
PID_PLACEHOLDER = "{pid}"  # -> PID
LOCAL_USER_PLACEHOLDER = "{lu}"  # -> username

# Env var with the default working dir root:
P_SSH_WORKING_DIR_ROOT_ENV_VAR = "P_SSH_WORKING_DIR_ROOT"

# Padding, in seconds, for max task termination wait -> max batch cancel wait.
BATCH_CANCEL_MAX_WAIT_PAD_SEC = 1

# Batch cancel max wait if none of the tasks has max task termination wait
# defined:
BATCH_CANCEL_MAX_WAIT_SEC = 2


def load_host_spec_file(fname_or_names: Union[str, Iterable[str]]) -> Iterable[str]:
    host_specs = OrderedDict()

    if isinstance(fname_or_names, str):
        fname_or_names = [fname_or_names]

    for fname in fname_or_names:
        with open(fname, "rt") as f:
            for line in f:
                line = line.strip()
                if not line or line[0] == "#":
                    continue
                for host_spec in line.split():
                    host_specs[host_spec] = None
    return list(host_specs)


def uname_n():
    return os.uname().nodename.lower().split(".", 1)[0]


def expand_working_dir(working_dir: str) -> str:
    """Expand env vars timestamp and placeholders"""

    working_dir = os.path.expandvars(working_dir)
    uid = os.getuid()
    try:
        user = pwd.getpwuid(uid).pw_name
    except KeyError:
        user = f"uid-{uid}"
    for ph, val in [
        (LOCAL_HOSTNAME_PLACEHOLDER, uname_n()),
        (PID_PLACEHOLDER, str(os.getpid())),
        (LOCAL_USER_PLACEHOLDER, user),
    ]:
        working_dir = working_dir.replace(ph, val)
    working_dir = time.strftime(working_dir)
    return working_dir


def get_default_working_dir_root() -> str:
    return os.environ.get(
        P_SSH_WORKING_DIR_ROOT_ENV_VAR,
        os.path.join("/tmp", LOCAL_USER_PLACEHOLDER, __package__, "work"),
    )


def get_default_working_dir(
    working_dir_root: Optional[str] = None,
    comp: Optional[str] = None,
) -> str:
    if working_dir_root is None:
        working_dir_root = get_default_working_dir_root()
    return os.path.join(
        working_dir_root,
        comp or "",
        f"%Y-%m-%dT%H:%M:%S%z-{PID_PLACEHOLDER}",
    )


class DisplayTaskResultCB:
    """Standard Task Completion Callback

    Args:
        fh (TextIO): where to display the results
    """

    def __init__(self, fh: TextIO = sys.stdout):
        self._fh = fh

    def __call__(self, p_task: PTask):
        """The actual callback invoked w/ the completed task"""
        cmd_and_args = " ".join(
            map(shlex.quote, (p_task.cmd,) + (p_task.args or tuple()))
        )
        result = p_task.result
        if result is not None:
            retcode = result.retcode
            cond = result.cond.name
            start_ts = format_log_ts(p_task.start_ts)
            end_ts = format_log_ts(p_task.end_ts)
            runtime = f"{p_task.end_ts - p_task.start_ts:0.6f}"
        sep = "-" * 5
        with p_task._lck:
            result = p_task.result
            if result is None:
                print(
                    f"{format_log_ts()} - cmd: {cmd_and_args}, pid: {p_task.pid} not completed",
                    file=self._fh,
                )
            else:
                retcode = result.retcode
                cond = result.cond.name
                start_ts = format_log_ts(p_task.start_ts)
                end_ts = format_log_ts(p_task.end_ts)
                runtime = f"{p_task.end_ts - p_task.start_ts:0.6f}"
                print(
                    f"{format_log_ts()} - cmd: {cmd_and_args}, pid: {p_task.pid}"
                    + f", start_ts: {start_ts}, end_ts: {end_ts}, runtime: {runtime}"
                    + f", retcode: {retcode}, cond: {cond!r}",
                    file=self._fh,
                )
                had_data = False
                if p_task.out_disp != PTaskOutDisp.IGNORE:
                    for what, data_or_path in [
                        ("stdout", result.stdout),
                        ("stderr", result.stderr),
                    ]:
                        ends_with_nl, has_data = True, False
                        if p_task._out_disp in {
                            PTaskOutDisp.COLLECT,
                            PTaskOutDisp.AUDIT,
                        }:
                            if data_or_path:
                                if had_data:
                                    self._fh.write("\n")
                                has_data = True
                                print(f"{sep} {what} {sep}", file=self._fh)
                                self._fh.write(str(data_or_path, "utf-8"))
                                ends_with_nl = data_or_path.endswith(b"\n")
                        elif p_task._out_disp == PTaskOutDisp.RECORD:
                            with open(data_or_path, "rt") as f:
                                while True:
                                    buf = f.read(io.DEFAULT_BUFFER_SIZE)
                                    if len(buf) == 0:
                                        break
                                    if not has_data:
                                        if had_data:
                                            self._fh.write("\n")
                                        print(
                                            f"{sep} {what} ({data_or_path!r}) {sep}",
                                            file=self._fh,
                                        )
                                        has_data = True
                                    self._fh.write(buf)
                                    ends_with_nl = buf.endswith("\n")
                        if not ends_with_nl:
                            self._fh.write("\n")
                        if has_data:
                            print(f"{sep} {what} {sep}", file=self._fh)
                        had_data = has_data
            self._fh.write("\n")
            self._fh.flush()


async def _run_p_batch(
    p_tasks: List[PTask],
    n_parallel: int = 1,
    batch_timeout: float = 0,
    cb: Optional[Callable[[PTask], Any]] = None,
    logger: Optional[AuditLogger] = None,
) -> Optional[signal.Signals]:

    # Context for interrupting signal handler:
    class InterruptingSigHandler:
        def __init__(
            self,
            sigs: Iterable[signal.Signals] = [
                signal.SIGABRT,
                signal.SIGHUP,
                signal.SIGINT,
                signal.SIGTERM,
            ],
            logger: Optional[AuditLogger] = None,
        ):
            self._rcvd_sig = None
            self.during_batch_run = False
            self._logger = logger
            loop = asyncio.get_running_loop()
            for sig in sigs:
                loop.add_signal_handler(sig, self._handler, sig)

        def _warn(self, txt: str):
            print(txt, file=sys.stderr)
            if self._logger is not None:
                self._logger.log(lvl=Level.WARN, comp=__name__, txt=txt)

        def _handler(self, sig: signal.Signals):
            if self._rcvd_sig is not None:
                self._warn(
                    f"Received {sig.name}, ignored due to previously received {self._rcvd_sig.name}"
                )
                return
            self._rcvd_sig = sig
            self._warn(f"Received {self._rcvd_sig.name}")
            if self.during_batch_run:
                for task in asyncio.all_tasks():
                    coro = task.get_coro()
                    if getattr(coro, "__name__") == "_run_p_batch":
                        task.cancel()
                        break

        @property
        def rcvd_sig(self):
            return self._rcvd_sig

    ish = InterruptingSigHandler(logger=logger)
    p_task_by_task: Dict[asyncio.Task, PTask] = dict()
    pending = set()
    i = 0
    if batch_timeout is not None and batch_timeout <= 0:
        batch_timeout = None
    start_ts = time.time()
    try:
        ish.during_batch_run = True
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
                if cb is not None:
                    cb(p_task)
    except asyncio.CancelledError:
        pass
    finally:
        ish.during_batch_run = False

    # Everything left in pending should be cancelled at this point. Issue
    # cancellation twice since the first one relies on SIGTERM which may be
    # ignored.
    cancel_timeout = None
    for pass_no in range(2):
        if not pending:
            break
        for task in pending:
            task.cancel()
            if pass_no == 0:
                term_max_wait = p_task_by_task[task].term_max_wait
                if term_max_wait is None:
                    continue
                term_max_wait += BATCH_CANCEL_MAX_WAIT_PAD_SEC
                if cancel_timeout is None or term_max_wait > cancel_timeout:
                    cancel_timeout = term_max_wait
        done, pending = await asyncio.wait(
            pending,
            timeout=(
                cancel_timeout or BATCH_CANCEL_MAX_WAIT_SEC if pass_no == 0 else None
            ),
            return_when="ALL_COMPLETED",
        )
        for task in done:
            p_task = p_task_by_task[task]
            if cb is not None:
                cb(p_task)
    # If there is anything left at this point (there shouldn't be really)
    # declare them as lingering:
    for task in pending:
        p_task = p_task_by_task[task]
        p_task.log_completion(force_linger=True)
        if cb is not None:
            cb(p_task)

    return ish.rcvd_sig


def run_p_batch(
    p_tasks: List[PTask],
    n_parallel: int = 1,
    batch_timeout: float = 0,
    cb: Optional[Callable[[PTask], bool]] = None,
    logger: Optional[AuditLogger] = None,
):
    """
    Run a batch of tasks in parallel.

    Args:
        p_tasks (list): List of PTask objects

        n_parallel (int): Number tasks to run in parallel, use 0 for unlimited

        batch_timeout (float): Timeout in seconds for the whole batch, use 0 for
            no timeout

        cb (callable): A callback to invoke with the task upon its completion.
            If it returns True the task should be recorded in the results,
            otherwise it should be ignored.

        logger (AuditLogger): audit logger
    """

    rcvd_sig = asyncio.run(_run_p_batch(p_tasks, n_parallel, batch_timeout, cb, logger))
    if rcvd_sig is not None:
        os.kill(0, rcvd_sig)


def run_p_remote_batch(
    cmd: str,
    host_spec_list: Iterable[str],
    args: Optional[Union[Iterable, str, int, float]] = None,
    setpgid: bool = True,
    timeout: Optional[float] = None,
    term_max_wait: Optional[float] = None,
    input_fname: Optional[str] = None,
    n_parallel: int = 1,
    batch_timeout: float = 0,
    working_dir: Optional[str] = None,
    out_disp: Optional[PTaskOutDisp] = None,
    cb: Optional[Callable[[PTask], bool]] = None,
) -> Tuple[List[PRemoteTask], Optional[str]]:
    """Run a remote command on a set of target hosts.

    Args:
        cmd (str): the command to execute

        args (tuple): optional args, subject to host specification placeholder
            replacement (see p_task..._PLACEHOLDER)

        setpgid (bool: whether the process should run as session leader or not

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
        tuple: list of p_tasks, audit trail file name

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
            setpgid=setpgid,
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

    run_p_batch(p_tasks, n_parallel, batch_timeout, cb, logger)

    return p_tasks, audit_trail_fname
