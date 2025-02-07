#! /usr/bin/env python3


"""Unit tests for PTask"""

import asyncio
import json
import os
import shutil
import signal
import sys
import tempfile
import unittest
from unittest.mock import call, patch

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)

from p_ssh.log import AuditLogger
from p_ssh.p_task import (
    HOST_PLACEHOLDER,
    HOST_SPEC_PLACEHOLDER,
    P_TASK_AUDIT_EVENT_FIELD,
    P_TASK_AUDIT_HOST_FIELD,
    P_TASK_AUDIT_STDERR_FIELD,
    P_TASK_AUDIT_STDERR_FILE_FIELD,
    P_TASK_AUDIT_STDOUT_FIELD,
    P_TASK_AUDIT_STDOUT_FILE_FIELD,
    P_TASK_AUDIT_USER_FIELD,
    USER_PLACEHOLDER,
    PRemoteTask,
    PTask,
    PTaskCond,
    PTaskEvent,
    PTaskOutDisp,
)

os_killpg = os.killpg  # i.e. pre-patch reference


def os_killpg_mock(*args, **kwargs):
    """Simple pass through, used to capture the call for the mock"""
    return os_killpg(*args, **kwargs)


def tear_down_class_event_loop_hack():
    """Running a mix of IsolatedAsyncioTestCase and TestCase yields the
        following warning:

      _warn(f"unclosed event loop {self!r}", ResourceWarning, source=self)
    ResourceWarning: Enable tracemalloc to get the object allocation traceback

    Explicitly close the event loop upon class cleanup

    """

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
    except RuntimeError:
        pass


class TestPRemoteTask(unittest.TestCase):
    def test_host_placeholder_replacement(self):
        for host_spec, host, user in [
            (None, "", ""),
            ("", "", ""),
            ("host", "host", ""),
            ("user@host", "host", "user"),
        ]:
            p_task = PRemoteTask(
                cmd="cmd",
                args=(
                    "no_host_placeholder",
                    f"host_placeholder={HOST_PLACEHOLDER}",
                    f"user_placeholder={USER_PLACEHOLDER}",
                    f"host_spec_placeholder={HOST_SPEC_PLACEHOLDER}",
                ),
                host_spec=host_spec,
            )
            self.assertEqual(
                [(P_TASK_AUDIT_HOST_FIELD, host), (P_TASK_AUDIT_USER_FIELD, user)],
                p_task._extra_start_args,
                msg="_extra_start_args (want != got)",
            )
            self.assertEqual(
                (
                    "no_host_placeholder",
                    f"host_placeholder={host}",
                    f"user_placeholder={user}",
                    f"host_spec_placeholder={host_spec or ''}",
                ),
                p_task._args,
                msg="_args (want != got)",
            )


class TestPTask(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls._stdout_logger = AuditLogger(fh_or_fname=sys.stdout)
        cls._temp_dir = tempfile.mkdtemp()
        return super().setUpClass()

    def setUp(self):
        print()  # add "\n" after test_ ...
        return super().setUp()

    @patch("p_ssh.p_task.os.killpg")
    async def test_p_task_ok(self, killpg_mock):
        killpg_mock.side_effect = os_killpg_mock

        p_task = PTask(
            cmd="bash",
            args=(
                "-c",
                "\n".join(
                    [
                        "set -x",
                        "echo before sleep",
                        "sleep 1",
                        "echo after sleep",
                    ]
                ),
            ),
            timeout=2,
            out_disp=PTaskOutDisp.COLLECT,
            logger=self._stdout_logger,
        )
        done, _ = await asyncio.wait([p_task.task])

        self.assertEqual(1, len(done), msg="len(done) (want != got))")
        p_task_result = list(done)[0].result()
        self.assertEqual(0, p_task_result.retcode, msg="retcode (want != got)")
        self.assertEqual(
            b"before sleep\nafter sleep\n",
            p_task_result.stdout,
            msg="stdout (want != got)",
        )
        self.assertEqual(
            b"+ echo before sleep\n+ sleep 1\n+ echo after sleep\n",
            p_task_result.stderr,
            msg="stderr (want != got)",
        )
        self.assertEqual(PTaskCond.OK, p_task_result.cond, "condition (want != got)")
        killpg_mock.assert_not_called()

    @patch("p_ssh.p_task.os.killpg")
    async def test_p_task_audit_out_disp(self, killpg_mock):
        killpg_mock.side_effect = os_killpg_mock

        tmp_dir = os.path.join(self._temp_dir, "test_p_task_audit_out_disp")

        n_lines = 13

        for out_disp in PTaskOutDisp:
            work_dir = os.path.join(tmp_dir, out_disp.name)
            audit_file = os.path.join(work_dir, "audit.jsonl")
            out_dir = os.path.join(work_dir, "out")
            print(f"audit_file={audit_file!r}, out_disp={out_disp!r}")

            os.makedirs(work_dir, exist_ok=True)
            os.makedirs(out_dir, exist_ok=True)
            logger = AuditLogger(fh_or_fname=audit_file)

            p_task = PTask(
                cmd="bash",
                args=(
                    "-c",
                    "\n".join(
                        [
                            f"for ((k=0; k<{n_lines}; k++)) do echo stdout $k; done",
                            f"for ((k=0; k<{n_lines}; k++)) do echo stderr $k; done >&2",
                        ]
                    ),
                ),
                out_disp=out_disp,
                logger=logger,
                out_dir=out_dir,
            )
            expect_stdout = bytes(
                "\n".join([f"stdout {k}" for k in range(n_lines)]) + "\n", "utf-8"
            )
            expect_stderr = bytes(
                "\n".join([f"stderr {k}" for k in range(n_lines)]) + "\n", "utf-8"
            )
            done, _ = await asyncio.wait([p_task.task])
            self.assertEqual(1, len(done), msg="len(done) (want != got))")
            p_task_result = list(done)[0].result()
            self.assertEqual(0, p_task_result.retcode, msg="retcode (want != got)")
            if out_disp == PTaskOutDisp.IGNORE:
                self.assertIs(None, p_task_result.stdout, msg="stdout (want != got)")
                self.assertIs(None, p_task_result.stderr, msg="stderr (want != got)")
            if out_disp in {PTaskOutDisp.COLLECT, PTaskOutDisp.AUDIT}:
                self.assertEqual(
                    expect_stdout,
                    p_task_result.stdout,
                    msg="stdout (want != got)",
                )
                self.assertEqual(
                    expect_stderr,
                    p_task_result.stderr,
                    msg="stderr (want != got)",
                )
            self.assertEqual(
                PTaskCond.OK, p_task_result.cond, "condition (want != got)"
            )
            killpg_mock.assert_not_called()

            audit_complete_event = None
            with open(audit_file, "rt") as f:
                for line in f:
                    audit_line = json.loads(line)
                    if (
                        audit_line.get(P_TASK_AUDIT_EVENT_FIELD)
                        == PTaskEvent.COMPLETE.name
                    ):
                        audit_complete_event = audit_line
                        break
            self.assertIsNot(None, audit_complete_event)
            if out_disp == PTaskOutDisp.AUDIT:
                for want, field in [
                    (expect_stdout, P_TASK_AUDIT_STDOUT_FIELD),
                    (expect_stderr, P_TASK_AUDIT_STDERR_FIELD),
                ]:
                    self.assertIn(field, audit_complete_event)
                    got = bytes(audit_complete_event[field], "utf-8")
                    self.assertEqual(
                        want,
                        got,
                        msg=f"audit {field!r} field (want != got)",
                    )
            elif out_disp == PTaskOutDisp.RECORD:
                for want, field in [
                    (expect_stdout, P_TASK_AUDIT_STDOUT_FILE_FIELD),
                    (expect_stderr, P_TASK_AUDIT_STDERR_FILE_FIELD),
                ]:
                    self.assertIn(field, audit_complete_event)
                    with open(audit_complete_event[field], "rb") as f:
                        got = f.read()
                    self.assertEqual(
                        want,
                        got,
                        msg=f"audit {field!r} field (want != got)",
                    )

    @patch("p_ssh.p_task.os.killpg")
    async def test_p_task_timeout(self, killpg_mock):
        killpg_mock.side_effect = os_killpg_mock

        p_task = PTask(
            cmd="bash",
            args=(
                "-c",
                "\n".join(
                    [
                        "set -x",
                        "echo before sleep",
                        "sleep 1",
                        "echo after sleep",
                    ]
                ),
            ),
            timeout=0.5,
            out_disp=PTaskOutDisp.COLLECT,
            logger=self._stdout_logger,
        )
        done, _ = await asyncio.wait([p_task.task])
        p_task_result = list(done)[0].result()
        self.assertEqual(1, len(done), msg="len(done) (want != got))")

        self.assertEqual(
            -signal.SIGTERM, p_task_result.retcode, msg="retcode (want != got)"
        )
        self.assertEqual(
            b"before sleep\n", p_task_result.stdout, msg="stdout (want != got)"
        )
        self.assertEqual(
            b"+ echo before sleep\n+ sleep 1\n",
            p_task_result.stderr,
            msg="stderr (want != got)",
        )
        self.assertEqual(
            PTaskCond.TIMEOUT, p_task_result.cond, "condition (want != got)"
        )
        killpg_mock.assert_called_once_with(p_task.pid, signal.SIGTERM)

    @patch("p_ssh.p_task.os.killpg")
    async def test_p_task_ignore_term_timeout(self, killpg_mock):
        killpg_mock.side_effect = os_killpg_mock

        p_task = PTask(
            cmd="bash",
            args=(
                "-c",
                "\n".join(
                    [
                        "trap '' SIGTERM",
                        "set -x",
                        "echo before sleep",
                        "sleep 3600",
                        "echo after sleep",
                    ]
                ),
            ),
            timeout=0.5,
            term_max_wait=0.5,
            out_disp=PTaskOutDisp.COLLECT,
            logger=self._stdout_logger,
        )
        done, _ = await asyncio.wait([p_task.task])

        self.assertEqual(1, len(done), msg="len(done) (want != got))")
        p_task_result = list(done)[0].result()
        self.assertEqual(
            -signal.SIGKILL, p_task_result.retcode, msg="retcode (want != got)"
        )
        self.assertEqual(
            b"before sleep\n", p_task_result.stdout, msg="stdout (want != got)"
        )
        self.assertEqual(
            b"+ echo before sleep\n+ sleep 3600\n",
            p_task_result.stderr,
            msg="stderr (want != got)",
        )
        self.assertEqual(
            PTaskCond.TIMEOUT, p_task_result.cond, "condition (want != got)"
        )
        self.assertEqual(
            [call(p_task.pid, signal.SIGTERM), call(p_task.pid, signal.SIGKILL)],
            killpg_mock.mock_calls,
            "os.killpgs calls (want != got)",
        )

    @patch("p_ssh.p_task.os.killpg")
    async def test_p_task_cancel(self, killpg_mock):
        killpg_mock.side_effect = os_killpg_mock

        p_task = PTask(
            cmd="bash",
            args=(
                "-c",
                "\n".join(
                    [
                        "set -x",
                        "echo before sleep",
                        "sleep 1",
                        "echo after sleep",
                    ]
                ),
            ),
            timeout=2,
            term_max_wait=0.5,
            out_disp=PTaskOutDisp.COLLECT,
            logger=self._stdout_logger,
        )
        done, pending = await asyncio.wait([p_task.task], timeout=0.5)
        self.assertEqual(0, len(done), msg="len(done) (want != got))")
        self.assertEqual(1, len(pending), msg="len(pending) (want != got))")
        for task in pending:
            task.cancel()
        done, _ = await asyncio.wait([p_task.task], timeout=0.5)

        self.assertEqual(1, len(done), msg="len(done) (want != got))")
        p_task_result = list(done)[0].result()
        self.assertEqual(
            -signal.SIGTERM, p_task_result.retcode, msg="retcode (want != got)"
        )
        self.assertEqual(
            b"before sleep\n", p_task_result.stdout, msg="stdout (want != got)"
        )
        self.assertEqual(
            b"+ echo before sleep\n+ sleep 1\n",
            p_task_result.stderr,
            msg="stderr (want != got)",
        )
        self.assertEqual(
            PTaskCond.CANCELLED, p_task_result.cond, "condition (want != got)"
        )
        killpg_mock.assert_called_once_with(p_task.pid, signal.SIGTERM)

    @patch("p_ssh.p_task.os.killpg")
    async def test_p_task_ignore_term_cancel(self, killpg_mock):
        killpg_mock.side_effect = os_killpg_mock

        p_task = PTask(
            cmd="bash",
            args=(
                "-c",
                "\n".join(
                    [
                        "trap '' SIGTERM",
                        "set -x",
                        "echo before sleep",
                        "sleep 1",
                        "echo after sleep",
                    ]
                ),
            ),
            timeout=2,
            term_max_wait=0.5,
            out_disp=PTaskOutDisp.COLLECT,
            logger=self._stdout_logger,
        )
        done, pending = await asyncio.wait([p_task.task], timeout=0.5)
        self.assertEqual(0, len(done), msg="len(done) (want != got))")
        self.assertEqual(1, len(pending), msg="len(pending) (want != got))")
        for task in pending:
            task.cancel()
        done, _ = await asyncio.wait([p_task.task], timeout=1)

        self.assertEqual(1, len(done), msg="len(done) (want != got))")
        p_task_result = list(done)[0].result()
        self.assertEqual(
            -signal.SIGKILL, p_task_result.retcode, msg="retcode (want != got)"
        )
        self.assertEqual(
            b"before sleep\n", p_task_result.stdout, msg="stdout (want != got)"
        )
        self.assertEqual(
            b"+ echo before sleep\n+ sleep 1\n",
            p_task_result.stderr,
            msg="stderr (want != got)",
        )
        self.assertEqual(
            PTaskCond.CANCELLED, p_task_result.cond, "condition (want != got)"
        )
        self.assertEqual(
            [call(p_task.pid, signal.SIGTERM), call(p_task.pid, signal.SIGKILL)],
            killpg_mock.mock_calls,
            "os.killpgs calls (want != got)",
        )

    @classmethod
    def tearDownClass(cls):
        tear_down_class_event_loop_hack()
        shutil.rmtree(cls._temp_dir, ignore_errors=True)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        unittest.main()
    else:
        unittest.main(verbosity=2, buffer=True)
