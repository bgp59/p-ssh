#! /usr/bin/env python3

"""
asyncio compatible task for parallel execution of commands.
"""

import asyncio
import enum
import os
import pwd
import signal
import time
from collections import OrderedDict
from dataclasses import dataclass
from functools import lru_cache
from threading import RLock
from typing import Awaitable, Iterable, Optional, Tuple, Union

from . import log

# The following placeholders will be replaced by the full host specification,
# host only or user only in every command argument; in ssh/rsync style, the spec
# may be [USER@]HOST.
HOST_SPEC_PLACEHOLDER = "{s}"  # -> USER@HOST
HOST_PLACEHOLDER = "{h}"  # -> HOST
USER_PLACEHOLDER = "{u}"  # -> USER

# Env var with the root of the out dir, where the stdout/stderr from the task
# are recorded:
P_SSH_OUT_DIR_ENV_VAR = "P_SSH_OUT_DIR"


class PTaskOutDisp(enum.IntEnum):
    IGNORE = 0
    COLLECT = 1
    AUDIT = 2
    RECORD = 3


class PTaskEvent(enum.Enum):
    START = 1
    TIMEOUT = 2
    CANCEL = 3
    TERMINATE = 4
    KILL = 5
    ERROR = 6
    COMPLETE = 7


event_log_level_map = {
    PTaskEvent.TIMEOUT: log.Level.WARN,
    PTaskEvent.CANCEL: log.Level.WARN,
    PTaskEvent.TERMINATE: log.Level.WARN,
    PTaskEvent.KILL: log.Level.WARN,
    PTaskEvent.ERROR: log.Level.ERROR,
}


class PTaskCond(enum.IntEnum):
    OK = 0
    ERROR = 1
    TIMEOUT = 2
    CANCELLED = 3
    LINGER = 4
    INCOMPLETE = 5


# The name of fields used for audit trail:
P_TASK_AUDIT_ARGS_FIELD = "args"
P_TASK_AUDIT_CMD_FIELD = "cmd"
P_TASK_AUDIT_COND_FIELD = "cond"
P_TASK_AUDIT_EVENT_FIELD = "event"
P_TASK_AUDIT_GEN_NUM_FIELD = "gen#"
P_TASK_AUDIT_HOST_FIELD = "host"
P_TASK_AUDIT_PID_FIELD = "pid"
P_TASK_AUDIT_RETCODE_FIELD = "retcode"
P_TASK_AUDIT_STDERR_FIELD = "stderr"
P_TASK_AUDIT_STDERR_FILE_FIELD = "stderr_file"
P_TASK_AUDIT_STDOUT_FIELD = "stdout"
P_TASK_AUDIT_STDOUT_FILE_FIELD = "stdout_file"
P_TASK_AUDIT_USER_FIELD = "user"


@dataclass
class PTaskResult:
    retcode: int = 255
    stdout: Union[bytes, str, None] = None
    stderr: Union[bytes, str, None] = None
    cond: Optional[PTaskCond] = None


def get_default_out_dir() -> str:
    out_dir = os.environ.get(P_SSH_OUT_DIR_ENV_VAR)
    if out_dir is not None:
        return out_dir
    uid = os.getuid()
    try:
        user = pwd.getpwuid(uid).pw_name
    except KeyError:
        user = os.environ.get("USER") or os.environ.get("LOGIN") or f"uid-{uid}"
    return os.path.join("/tmp", user, __package__, "out")


@lru_cache(None)
def get_user_host(host_spec: str) -> Tuple[str, str]:
    """Extract user, hostname from spec"""
    if host_spec is None:
        return "", ""
    i = host_spec.find("://")  # scheme
    if i >= 0:
        host_spec = host_spec[i + 3 :]
        i = host_spec.find(":")  # port
        if i >= 0:
            host_spec = host_spec[:i]
    i = host_spec.find("@")
    if i >= 0:
        user, host = host_spec[:i], host_spec[i + 1 :]
    else:
        user, host = "", host_spec
    return user, host.lower()


def replace_placeholders(arg: str, host_spec: Optional[str]) -> str:
    if host_spec is None:
        host_spec = ""
    user, host = get_user_host(host_spec)
    for ph, val in [
        (HOST_SPEC_PLACEHOLDER, host_spec),
        (USER_PLACEHOLDER, user),
        (HOST_PLACEHOLDER, host),
    ]:
        arg = arg.replace(ph, val)
    return arg


class PTask:
    """An asyncio compatible task for parallel execution.

    This object  will execute a command and it will wait for its completion,
    timeout or cancellation. The execution is wrapped in an asyncio.Task to fit
    into the Python framework.

    The object will maintain an audit trail w/ events and output from the
    command execution.

    Constructor args:

        cmd (str): the command to execute

        args (tuple): optional args

        setpgid (bool: whether the process should run as session leader or not

        timeout (float): if not None, the max time, in seconds, to wait for the
            command completion.

        term_max_wait (float): if not None, the max time, in seconds, to
            wait for command termination via SIGTERM. The latter will be sent in
            case of timeout or cancellation. Should term_max_wait expire, the
            command will be killed (via SIGKILL).

        input_fname (str): Optional file name to be used as stdin to the
            command, rather than devnull.

        out_disp (PTaskOutDisp): how to handle command's
            stdout/stderr

            IGNORE: discard to devnull and return None with the result

            COLLECT: collect and return with the result

            AUDIT: collect, log into audit file and return with the result

            RECORD: record to .out/.err files under out_dir and return the
                full paths with the result

        out_dir (str): The directory for the recording. The output will be
            recorded into OUT_DIR/TIMESTAMP-PID-N.out and .err files (N is a
            generation number, incremented with each utilization).

        logger (log.AuditLogger): the logger to use

    """

    # The class lock protection for output, audit, etc:
    _lck = RLock()
    # The generation number used to disambiguate recording file names (part of the prefix):
    _gen_num = 0

    # Init state:
    _start_ts = None
    _end_ts = None
    _task = None
    _pid = None
    _n = None
    _retcode = 255
    _stdout = None
    _stderr = None
    _cond = None
    _result = None
    _extra_start_args = None

    def __init__(
        self,
        cmd: str,
        setpgid: bool = True,
        args: Optional[Union[Iterable, str, int, float]] = None,
        input_fname: Optional[str] = None,
        timeout: Optional[float] = None,
        term_max_wait: Optional[float] = None,
        out_disp: PTaskOutDisp = None,
        out_dir: Optional[str] = None,
        logger: Optional[log.AuditLogger] = None,
    ):
        self._cmd = cmd
        self._args = args
        self._setpgid = setpgid
        self._proc_stdin = open(input_fname, "rb") if input_fname is not None else None
        self._timeout = timeout
        self._term_max_wait = term_max_wait
        self._out_disp = out_disp if out_disp is not None else PTaskOutDisp.IGNORE
        with self._lck:
            self.__class__._gen_num += 1
            self._n = self.__class__._gen_num
        if self._out_disp == PTaskOutDisp.RECORD:
            if out_dir is None:
                out_dir = get_default_out_dir()
            os.makedirs(out_dir, exist_ok=True)
            self._out_file_prefix = os.path.join(
                out_dir, time.strftime(f"%Y-%m-%d-%H-%M-%S%z-{os.getpid()}-{self._n}")
            )
        else:
            self._out_file_prefix = None
        self._logger = logger

    @property
    def task(self) -> asyncio.Task:
        if self._task is None:
            self._task = asyncio.create_task(self._run_exec())
        return self._task

    @property
    def cmd(self):
        return self._cmd

    @property
    def args(self):
        return self._args

    @property
    def timeout(self):
        return self._timeout

    @property
    def term_max_wait(self):
        return self._term_max_wait

    @property
    def out_disp(self):
        return self._out_disp

    @property
    def pid(self):
        return self._pid

    @property
    def start_ts(self):
        return self._start_ts

    @property
    def end_ts(self):
        return self._end_ts

    @property
    def retcode(self):
        return self._retcode

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stderr

    @property
    def cond(self):
        return self._cond

    @property
    def result(self):
        return self._result

    def log_event(
        self, event: Optional[PTaskEvent] = None, txt: Optional[str] = None, **kwargs
    ):
        if self._logger is None:
            return
        log_kwargs = OrderedDict(
            [
                (P_TASK_AUDIT_PID_FIELD, self._pid),
                (P_TASK_AUDIT_GEN_NUM_FIELD, self._n),
                (P_TASK_AUDIT_EVENT_FIELD, event.name if event is not None else None),
            ]
        )
        log_kwargs.update(kwargs)
        self._logger.log(
            txt=txt,
            lvl=event_log_level_map.get(event, log.Level.INFO),
            comp=__name__,
            **log_kwargs,
        )

    def log_completion(self, force_linger: bool = False):
        if force_linger:
            self._retcode = 255
            self._stdout = None
            self._stderr = None
            self._cond = PTaskCond.LINGER
        self._result = PTaskResult(
            self._retcode, self._stdout, self._stderr, self._cond
        )
        if self._logger is None:
            return
        log_kwargs = OrderedDict()
        log_kwargs[P_TASK_AUDIT_RETCODE_FIELD] = self._retcode
        if self._out_disp == PTaskOutDisp.AUDIT:
            log_kwargs[P_TASK_AUDIT_STDOUT_FIELD] = (
                str(self._stdout, "utf-8") if self._stdout is not None else None
            )
            log_kwargs[P_TASK_AUDIT_STDERR_FIELD] = (
                str(self._stderr, "utf-8") if self._stderr is not None else None
            )
        elif self._out_disp == PTaskOutDisp.RECORD:
            log_kwargs[P_TASK_AUDIT_STDOUT_FILE_FIELD] = self._stdout
            log_kwargs[P_TASK_AUDIT_STDERR_FILE_FIELD] = self._stderr
        log_kwargs[P_TASK_AUDIT_COND_FIELD] = (
            self._cond.name if self._cond is not None else None
        )
        self.log_event(
            event=PTaskEvent.COMPLETE,
            **log_kwargs,
        )
        self._logger = None

    async def _run_exec(self) -> Awaitable:
        try:
            stdout_path, stderr_path = None, None
            use_communicate = False

            if self._out_disp in {
                PTaskOutDisp.COLLECT,
                PTaskOutDisp.AUDIT,
            }:
                proc_stdout = asyncio.subprocess.PIPE
                proc_stderr = asyncio.subprocess.PIPE
                use_communicate = True
            elif self._out_disp == PTaskOutDisp.RECORD:
                stdout_path = self._out_file_prefix + ".out"
                proc_stdout = open(stdout_path, "wb")
                stderr_path = self._out_file_prefix + ".err"
                proc_stderr = open(stderr_path, "wb")
            else:
                proc_stdout = asyncio.subprocess.DEVNULL
                proc_stderr = asyncio.subprocess.DEVNULL

            proc = await asyncio.create_subprocess_exec(
                self._cmd,
                *self._args,
                start_new_session=self._setpgid,
                stdin=(
                    self._proc_stdin
                    if self._proc_stdin is not None
                    else asyncio.subprocess.DEVNULL
                ),
                stdout=proc_stdout,
                stderr=proc_stderr,
            )
            self._start_ts = time.time()
            self._pid = proc.pid

            proc_task = asyncio.Task(
                proc.communicate() if use_communicate else proc.wait()
            )
            for sig, event, timeout in [
                (None, PTaskEvent.START, self._timeout),
                (signal.SIGTERM, PTaskEvent.TERMINATE, self._term_max_wait),
                (signal.SIGKILL, PTaskEvent.KILL, None),
            ]:
                try:
                    if sig is None:
                        waiting_for = "pending normal execution"
                        log_kwargs = OrderedDict(
                            [
                                (P_TASK_AUDIT_CMD_FIELD, self._cmd),
                                (P_TASK_AUDIT_ARGS_FIELD, self._args),
                            ]
                        )
                        if self._extra_start_args is not None:
                            log_kwargs.update(self._extra_start_args)
                        self.log_event(
                            event=PTaskEvent.START,
                            **log_kwargs,
                        )
                    else:
                        waiting_for = f"pending {sig.name}"
                        self.log_event(
                            event=event,
                            txt=f"send {sig.name} to {'pgroup' if self._setpgid else 'pid'} {proc.pid}",
                        )
                        if self._setpgid:
                            os.killpg(self._pid, sig)
                        else:
                            os.kill(self._pid, sig)
                    done, _ = await asyncio.wait([proc_task], timeout=timeout)
                    if done:
                        if use_communicate:
                            self._retcode = proc.returncode
                            self._stdout, self._stderr = proc_task.result()
                        else:
                            self._retcode, self._stdout, self._stderr = (
                                proc_task.result(),
                                stdout_path,
                                stderr_path,
                            )
                        if self._cond is None:
                            self._cond = (
                                PTaskCond.OK if self._retcode == 0 else PTaskCond.ERROR
                            )
                        break
                    else:
                        if self._cond is None:
                            self._cond = PTaskCond.TIMEOUT
                        self.log_event(
                            event=PTaskEvent.TIMEOUT,
                            txt=f"{waiting_for} timed out after {timeout} sec",
                        )
                except asyncio.CancelledError:
                    if self._cond is None:
                        self._cond = PTaskCond.CANCELLED
                    self.log_event(
                        event=PTaskEvent.CANCEL,
                        txt=f"cancel {waiting_for}",
                    )
        except Exception as e:
            if self._cond is None:
                self._cond = PTaskCond.ERROR
            e_bytes = bytes(str(e), "utf-8")
            if stderr_path is not None and proc_stderr is not None:
                proc_stderr.write(e_bytes)
            else:
                self._stderr = e_bytes

            self.log_event(event=PTaskEvent.ERROR, txt=str(e))
        finally:
            self._end_ts = time.time()
            if self._proc_stdin is not None:
                self._proc_stdin.close()
            self.log_completion()
            return self.result

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            + ", ".join(
                [
                    f"{attr[1:]}={getattr(self, attr)!r}"
                    for attr in [
                        "_cmd",
                        "_args",
                        "_timeout",
                        "_term_max_wait",
                        "_out_disp",
                    ]
                ]
            )
            + ")"
        )

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            + ", ".join(
                [
                    f"{attr[1:]}={getattr(self, attr)!r}"
                    for attr in [
                        "_cmd",
                        "_args",
                    ]
                ]
            )
            + ")"
        )


class PRemoteTask(PTask):
    """An asyncio compatible task for parallel execution of remote commands
    (ssh/rsync).

    This derived class has the following extras compared to PTask:

        host_spec (str): the host specification for the command
            ssh/rsync style: [USER@]HOST

        args: optional args
            For each arg which is a string, the placeholders (see
            .._PLACEHOLDER) will be replaced accordingly.

    """

    def __init__(
        self,
        cmd,
        args: Optional[Union[Iterable, str, int, float]] = None,
        setpgid: bool = True,
        host_spec: Optional[str] = None,
        input_fname: Optional[str] = None,
        timeout: Optional[float] = None,
        term_max_wait: Optional[float] = None,
        out_disp: PTaskOutDisp = None,
        out_dir: Optional[str] = None,
        logger: Optional[log.AuditLogger] = log.default_logger,
    ):

        if args is None:
            args = tuple()
        elif isinstance(args, str):
            args = (replace_placeholders(args, host_spec),)
        elif isinstance(args, (int, float)):
            args = (str(args),)
        else:
            args = tuple(
                (
                    replace_placeholders(arg, host_spec)
                    if isinstance(arg, str)
                    else str(arg)
                )
                for arg in args
            )

        self._host_spec = host_spec
        user, host = get_user_host(host_spec)
        if out_disp == PTaskOutDisp.RECORD:
            if out_dir is None:
                out_dir = get_default_out_dir()
            if host:
                out_dir = os.path.join(out_dir, host)
        super().__init__(
            cmd,
            args=args,
            setpgid=setpgid,
            input_fname=input_fname,
            timeout=timeout,
            term_max_wait=term_max_wait,
            out_disp=out_disp,
            out_dir=out_dir,
            logger=logger,
        )
        self._extra_start_args = [
            (P_TASK_AUDIT_HOST_FIELD, host),
            (P_TASK_AUDIT_USER_FIELD, user),
        ]

    @property
    def host_spec(self):
        return self._host_spec
