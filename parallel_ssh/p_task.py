#! /usr/bin/env python3

"""
asyncio compatible task for parallel execution of commands.
"""

import asyncio
import enum
import os
import pwd
import signal
import sys
import time
from collections import OrderedDict
from threading import RLock
from typing import Awaitable, Iterable, Optional, Tuple, Union

from . import log

# The following placeholder will be replaced by the host specification in every
# command argument; in ssh/rsync style, the spec may be USER@HOST.
HOST_SPEC_PLACEHOLDER = "%H%"

# The placeholder will be replaced by the host part in every command argument;
# in ssh/rsync style, the spec may be USER@HOST, only HOST will be used here.
HOST_PLACEHOLDER = "%h%"


class PTaskOutDisposition(enum.IntEnum):
    IGNORE = 0
    COLLECT = 1
    PASS_THRU = 2
    RECORD = 3


class PTaskEvent(enum.Enum):
    START = 1
    TIMEOUT = 2
    CANCEL = 3
    TERMINATION = 4
    KILL = 5
    ERROR = 6
    STDOUT = 7
    STDERR = 8
    RETCODE = 9
    CONDITION = 10


event_log_level_map = {
    PTaskEvent.TIMEOUT: log.Level.WARN,
    PTaskEvent.CANCEL: log.Level.WARN,
    PTaskEvent.TERMINATION: log.Level.WARN,
    PTaskEvent.KILL: log.Level.WARN,
    PTaskEvent.ERROR: log.Level.ERROR,
}


class PTaskCondition(enum.IntEnum):
    OK = 0
    ERROR = 1
    TIMEOUT = 2
    CANCELLED = 3
    LINGER = 4


# The name of fields used for audit trail:xs
P_TASK_AUDIT_ARGS_FIELD = "args"
P_TASK_AUDIT_CMD_FIELD = "cmd"
P_TASK_AUDIT_CONDITION_FIELD = "cond"
P_TASK_AUDIT_EVENT_FIELD = "event"
P_TASK_AUDIT_HOST_FIELD = "host"
P_TASK_AUDIT_OUT_LEN_FIELD = "len"
P_TASK_AUDIT_OUT_PATH_FIELD = "path"
P_TASK_AUDIT_PID_FIELD = "pid"
P_TASK_AUDIT_USER_FIELD = "user"
P_TASK_AUDIT_VAL_FIELD = "val"


def get_default_out_dir() -> str:
    out_dir = os.environ.get("PARALLEL_SSH_OUT_DIR")
    if out_dir is not None:
        return out_dir
    uid = os.getuid()
    try:
        user = pwd.getpwuid(uid).pw_name
    except KeyError:
        user = os.environ.get("USER") or os.environ.get("LOGIN") or f"uid-{uid}"
    return os.path.join("/tmp", user, __package__)


def parse_host_spec(host_spec: str) -> Tuple[str, str]:
    if host_spec is None:
        return "", ""
    i = host_spec.find("@")
    if i >= 0:
        return host_spec[:i], host_spec[i + 1 :]
    return "", host_spec


def replace_host_placeholders(arg: str, host_spec: Optional[str]) -> str:
    if host_spec is None:
        host_spec = ""
    return arg.replace(HOST_SPEC_PLACEHOLDER, host_spec).replace(
        HOST_PLACEHOLDER, parse_host_spec(host_spec)[1]
    )


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

        timeout (float): if not None, the max time, in seconds, to wait for the
            command completion.

        termination_max_wait (float): if not None, the max time, in seconds, to
            wait for command termination via SIGTERM. The latter will be sent in
            case of timeout or cancellation. Should termination_max_wait expire,
            the command will be killed (via SIGKILL).

        input_fname (str): Optional file name to be used as stdin to the
            command, rather than devnull.

        output_disposition (PTaskOutDisposition): how to handle command's
            stdout/stderr

            IGNORE: discard to devnull

            COLLECT: collect and return with result

            PASS_THRU: collect, pass to the stdout/stderr of the invoking
                process and return with the result

            RECORD: record to .out/.err files under out_dir and return the
                full paths with the result

        out_dir (str): The directory for the recording. The output will be
            recorded into OUT_DIR/TIMESTAMP-PID-N.out and .err files (N is
            a generation number, incremented with each utilization).

        logger (log.MultiLogger): the logger to use

    """

    # The class lock protection for output, audit, etc:
    _lck = RLock()
    # The generation number used to disambiguate recording file names (part of the prefix):
    _gen_num = 0

    def __init__(
        self,
        cmd,
        args: Optional[Union[Iterable, str, int, float]] = None,
        input_fname: Optional[str] = None,
        timeout: Optional[float] = None,
        termination_max_wait: Optional[float] = None,
        output_disposition: PTaskOutDisposition = None,
        out_dir: Optional[str] = None,
        logger: Optional[log.MultiLogger] = log.default_logger,
    ):
        self._cmd = cmd
        self._args = args
        self._proc_stdin = open(input_fname, "rb") if input_fname is not None else None
        self._timeout = timeout
        self._termination_max_wait = termination_max_wait
        self._output_disposition = (
            output_disposition
            if output_disposition is not None
            else PTaskOutDisposition.IGNORE
        )
        if self._output_disposition == PTaskOutDisposition.RECORD:
            if out_dir is None:
                out_dir = get_default_out_dir()
            os.makedirs(out_dir, exist_ok=True)
            self._out_file_prefix = os.path.join(out_dir, self._make_out_file_prefix())
        else:
            self._out_file_prefix = None
        self._logger = logger
        self._task = None
        self._pid = None
        self._retcode, self._stdout, self._stderr, self._condition = (
            255,
            None,
            None,
            None,
        )

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
    def host(self):
        return self._host

    @property
    def timeout(self):
        return self._timeout

    @property
    def termination_max_wait(self):
        return self._termination_max_wait

    @property
    def name(self):
        return self._name

    @property
    def pid(self):
        return self._pid

    def _make_out_file_prefix(self) -> str:
        with self._lck:
            self.__class__._gen_num += 1
            return time.strftime(
                f"%Y-%m-%d-%H-%M-%S%z-{os.getpid()}-{self.__class__._gen_num}"
            )

    def log_event(
        self, event: Optional[PTaskEvent] = None, txt: Optional[str] = None, **kwargs
    ):
        if self._logger is None:
            return
        log_kwargs = OrderedDict()
        if hasattr(self, "_log_args"):
            log_kwargs.update(self._log_args)
        log_kwargs.update(
            [
                (P_TASK_AUDIT_PID_FIELD, self._pid),
                (P_TASK_AUDIT_EVENT_FIELD, event.name if event is not None else None),
            ]
        )
        log_kwargs.update(kwargs)
        self._logger.log(
            txt=txt,
            lvl=event_log_level_map.get(event, log.Level.INFO),
            comp=self.__class__.__name__,
            **log_kwargs,
        )

    async def _run_exec(self) -> Awaitable:
        try:
            stdout_path, stderr_path = None, None
            use_communicate = False

            if (
                self._output_disposition == PTaskOutDisposition.COLLECT
                or self._output_disposition == PTaskOutDisposition.PASS_THRU
            ):
                proc_stdout = asyncio.subprocess.PIPE
                proc_stderr = asyncio.subprocess.PIPE
                use_communicate = True
            elif self._output_disposition == PTaskOutDisposition.RECORD:
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
                start_new_session=True,
                stdin=(
                    self._proc_stdin
                    if self._proc_stdin is not None
                    else asyncio.subprocess.DEVNULL
                ),
                stdout=proc_stdout,
                stderr=proc_stderr,
            )
            self._pid = proc.pid

            proc_task = asyncio.Task(
                proc.communicate() if use_communicate else proc.wait()
            )
            for sig, event, timeout in [
                (None, PTaskEvent.START, self._timeout),
                (signal.SIGTERM, PTaskEvent.TERMINATION, self._termination_max_wait),
                (signal.SIGKILL, PTaskEvent.KILL, None),
            ]:
                try:
                    if sig is None:
                        waiting_for = "pending normal execution"
                        self.log_event(
                            event=PTaskEvent.START,
                            **{
                                P_TASK_AUDIT_CMD_FIELD: self._cmd,
                                P_TASK_AUDIT_ARGS_FIELD: self._args,
                            },
                        )
                    else:
                        waiting_for = f"pending {sig!r}"
                        self.log_event(
                            event=event,
                            txt=f"send {sig!r} to pgroup {proc.pid}",
                        )
                        os.killpg(self._pid, sig)
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
                        if self._condition is None:
                            self._condition = (
                                PTaskCondition.OK
                                if self._retcode == 0
                                else PTaskCondition.ERROR
                            )
                        break
                    else:
                        if self._condition is None:
                            self._condition = PTaskCondition.TIMEOUT
                        self.log_event(
                            event=PTaskEvent.TIMEOUT,
                            txt=f"{waiting_for} timed out after {timeout} sec",
                        )
                except asyncio.CancelledError:
                    if self._condition is None:
                        self._condition = PTaskCondition.CANCELLED
                    self.log_event(
                        event=PTaskEvent.CANCEL,
                        txt=f"cancel {waiting_for}",
                    )
        except (FileNotFoundError, PermissionError, ProcessLookupError) as e:
            if self._condition is None:
                self._condition_ = PTaskCondition.ERROR
            self.log_event(event=PTaskEvent.ERROR, txt=str(e))
        finally:
            if self._proc_stdin is not None:
                self._proc_stdin.close()
            lck = self._logger.lck if self._logger is not None else self._lck
            with lck:
                for data_or_path, event, fh in [
                    (self._stdout, PTaskEvent.STDOUT, sys.stdout),
                    (self._stderr, PTaskEvent.STDERR, sys.stderr),
                ]:
                    if isinstance(data_or_path, bytes):
                        self.log_event(
                            event=event,
                            **{P_TASK_AUDIT_OUT_LEN_FIELD: len(data_or_path)},
                        )
                        if self._output_disposition == PTaskOutDisposition.PASS_THRU:
                            os.write(fh.fileno(), data_or_path)
                    elif isinstance(data_or_path, str):
                        self.log_event(
                            event=event, **{P_TASK_AUDIT_OUT_PATH_FIELD: data_or_path}
                        )
                self.log_event(
                    event=PTaskEvent.RETCODE,
                    **{P_TASK_AUDIT_VAL_FIELD: self._retcode},
                )
                self.log_event(
                    event=PTaskEvent.CONDITION,
                    **{
                        P_TASK_AUDIT_CONDITION_FIELD: self._condition.name,
                    },
                )
            return self._retcode, self._stdout, self._stderr, self._condition

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
                        "_termination_max_wait",
                        "_output_disposition",
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


class PSshRsyncTask(PTask):
    """An asyncio compatible task for parallel execution of ssh/rsync commands.

    This derived class is specialized for ssh/rsync commands with the following extras compared to PTask:

        host_spec (str): the host specification for the command
            ssh/rsync style: [USER@]HOST

        args: optional args
            For each arg which is a string, the HOST_SPEC_PLACEHOLDER will be
            replaced by the host specification, as is (i.e. USER@HOST), whereas
            HOST_PLACEHOLDER will be replaced by the host part only.

    """

    def __init__(
        self,
        cmd,
        args: Optional[Union[Iterable, str, int, float]] = None,
        host_spec: Optional[str] = None,
        input_fname: Optional[str] = None,
        timeout: Optional[float] = None,
        termination_max_wait: Optional[float] = None,
        output_disposition: PTaskOutDisposition = None,
        out_dir: Optional[str] = None,
        logger: Optional[log.MultiLogger] = log.default_logger,
    ):

        if args is None:
            args = tuple()
        elif isinstance(args, str):
            args = (replace_host_placeholders(args, host_spec),)
        elif isinstance(args, (int, float)):
            args = (str(args),)
        else:
            args = tuple(
                (
                    replace_host_placeholders(arg, host_spec)
                    if isinstance(arg, str)
                    else str(arg)
                )
                for arg in args
            )

        user, host = parse_host_spec(host_spec)
        if output_disposition == PTaskOutDisposition.RECORD:
            if out_dir is None:
                out_dir = get_default_out_dir()
            if host:
                out_dir = os.path.join(out_dir, host)
        super().__init__(
            cmd,
            args=args,
            input_fname=input_fname,
            timeout=timeout,
            termination_max_wait=termination_max_wait,
            output_disposition=output_disposition,
            out_dir=out_dir,
            logger=logger,
        )
        self._log_args = [
            (P_TASK_AUDIT_HOST_FIELD, host),
            (P_TASK_AUDIT_USER_FIELD, user),
        ]
