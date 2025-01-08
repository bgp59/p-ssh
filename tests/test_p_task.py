#! /usr/bin/env python3


""" Unit tests PTask
"""

import asyncio
import os
import signal
import sys
import unittest
from unittest.mock import call, patch

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)

from parallel_ssh.log import MultiLogger
from parallel_ssh.p_task import (
    HOST_PLACEHOLDER,
    HOST_SPEC_PLACEHOLDER,
    P_TASK_AUDIT_HOST_FIELD,
    P_TASK_AUDIT_USER_FIELD,
    PSshRsyncTask,
    PTask,
    PTaskCondition,
    PTaskOutDisposition,
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


class TestPSshRsyncTaskInit(unittest.TestCase):
    def test_host_placeholder_replacement(self):
        for host_spec, host, user in [
            (None, "", ""),
            ("", "", ""),
            ("host", "host", ""),
            ("user@host", "host", "user"),
        ]:
            p_task = PSshRsyncTask(
                cmd="cmd",
                args=(
                    "no_host_placeholder",
                    f"host_placeholder={HOST_PLACEHOLDER}",
                    f"host_spec_placeholder={HOST_SPEC_PLACEHOLDER}",
                ),
                host_spec=host_spec,
            )
            self.assertEqual(
                [(P_TASK_AUDIT_HOST_FIELD, host), (P_TASK_AUDIT_USER_FIELD, user)],
                p_task._log_args,
                msg="_log_args (want != got)",
            )
            self.assertEqual(
                (
                    "no_host_placeholder",
                    f"host_placeholder={host}",
                    f"host_spec_placeholder={host_spec or ''}",
                ),
                p_task._args,
                msg="_args (want != got)",
            )


class TestPTask(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls._logger = MultiLogger(stream=sys.stdout)
        return super().setUpClass()

    def setUp(self):
        print()  # add "\n" after test_ ...
        return super().setUp()

    @patch("parallel_ssh.p_task.os.killpg")
    async def test_cc_task_ok(self, killpg_mock):
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
            output_disposition=PTaskOutDisposition.COLLECT,
            logger=self._logger,
        )
        done, _ = await asyncio.wait([p_task.task])

        self.assertEqual(1, len(done), msg="len(done) (want != got))")
        retcode, stdout, stderr, condition = list(done)[0].result()
        self.assertEqual(0, retcode, msg="retcode (want != got)")
        self.assertEqual(
            b"before sleep\nafter sleep\n", stdout, msg="stdout (want != got)"
        )
        self.assertEqual(
            b"+ echo before sleep\n+ sleep 1\n+ echo after sleep\n",
            stderr,
            msg="stderr (want != got)",
        )
        self.assertEqual(PTaskCondition.OK, condition, "condition (want != got)")
        killpg_mock.assert_not_called()

    @patch("parallel_ssh.p_task.os.killpg")
    async def test_cc_task_timeout(self, killpg_mock):
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
            output_disposition=PTaskOutDisposition.COLLECT,
            logger=self._logger,
        )
        done, _ = await asyncio.wait([p_task.task])
        retcode, stdout, stderr, condition = list(done)[0].result()
        self.assertEqual(1, len(done), msg="len(done) (want != got))")

        self.assertEqual(-signal.SIGTERM, retcode, msg="retcode (want != got)")
        self.assertEqual(b"before sleep\n", stdout, msg="stdout (want != got)")
        self.assertEqual(
            b"+ echo before sleep\n+ sleep 1\n", stderr, msg="stderr (want != got)"
        )
        self.assertEqual(PTaskCondition.TIMEOUT, condition, "condition (want != got)")
        killpg_mock.assert_called_once_with(p_task.pid, signal.SIGTERM)

    @patch("parallel_ssh.p_task.os.killpg")
    async def test_cc_task_ignore_term_timeout(self, killpg_mock):
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
            termination_max_wait=0.5,
            output_disposition=PTaskOutDisposition.COLLECT,
            logger=self._logger,
        )
        done, _ = await asyncio.wait([p_task.task])

        self.assertEqual(1, len(done), msg="len(done) (want != got))")
        retcode, stdout, stderr, condition = list(done)[0].result()
        self.assertEqual(-signal.SIGKILL, retcode, msg="retcode (want != got)")
        self.assertEqual(b"before sleep\n", stdout, msg="stdout (want != got)")
        self.assertEqual(
            b"+ echo before sleep\n+ sleep 3600\n", stderr, msg="stderr (want != got)"
        )
        self.assertEqual(PTaskCondition.TIMEOUT, condition, "condition (want != got)")
        self.assertEqual(
            [call(p_task.pid, signal.SIGTERM), call(p_task.pid, signal.SIGKILL)],
            killpg_mock.mock_calls,
            "os.killpgs calls (want != got)",
        )

    @patch("parallel_ssh.p_task.os.killpg")
    async def test_cc_task_cancel(self, killpg_mock):
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
            termination_max_wait=0.5,
            output_disposition=PTaskOutDisposition.COLLECT,
            logger=self._logger,
        )
        done, pending = await asyncio.wait([p_task.task], timeout=0.5)
        self.assertEqual(0, len(done), msg="len(done) (want != got))")
        self.assertEqual(1, len(pending), msg="len(pending) (want != got))")
        for task in pending:
            task.cancel()
        done, _ = await asyncio.wait([p_task.task], timeout=0.5)

        self.assertEqual(1, len(done), msg="len(done) (want != got))")
        retcode, stdout, stderr, condition = list(done)[0].result()
        self.assertEqual(-signal.SIGTERM, retcode, msg="retcode (want != got)")
        self.assertEqual(b"before sleep\n", stdout, msg="stdout (want != got)")
        self.assertEqual(
            b"+ echo before sleep\n+ sleep 1\n", stderr, msg="stderr (want != got)"
        )
        self.assertEqual(PTaskCondition.CANCELLED, condition, "condition (want != got)")
        killpg_mock.assert_called_once_with(p_task.pid, signal.SIGTERM)

    @patch("parallel_ssh.p_task.os.killpg")
    async def test_cc_task_ignore_term_cancel(self, killpg_mock):
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
            termination_max_wait=0.5,
            output_disposition=PTaskOutDisposition.COLLECT,
            logger=self._logger,
        )
        done, pending = await asyncio.wait([p_task.task], timeout=0.5)
        self.assertEqual(0, len(done), msg="len(done) (want != got))")
        self.assertEqual(1, len(pending), msg="len(pending) (want != got))")
        for task in pending:
            task.cancel()
        done, _ = await asyncio.wait([p_task.task], timeout=1)

        self.assertEqual(1, len(done), msg="len(done) (want != got))")
        retcode, stdout, stderr, condition = list(done)[0].result()
        self.assertEqual(-signal.SIGKILL, retcode, msg="retcode (want != got)")
        self.assertEqual(b"before sleep\n", stdout, msg="stdout (want != got)")
        self.assertEqual(
            b"+ echo before sleep\n+ sleep 1\n", stderr, msg="stderr (want != got)"
        )
        self.assertEqual(PTaskCondition.CANCELLED, condition, "condition (want != got)")
        self.assertEqual(
            [call(p_task.pid, signal.SIGTERM), call(p_task.pid, signal.SIGKILL)],
            killpg_mock.mock_calls,
            "os.killpgs calls (want != got)",
        )

    @classmethod
    def tearDownClass(cls):
        tear_down_class_event_loop_hack()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        unittest.main()
    else:
        unittest.main(verbosity=2, buffer=True)
