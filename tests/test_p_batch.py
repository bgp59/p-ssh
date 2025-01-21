#! /usr/bin/env python3


""" Unit tests for p_batch
"""

import os
import signal
import sys
import unittest
from typing import List, Tuple

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)

from p_ssh.log import AuditLogger
from p_ssh.p_batch import run_p_batch
from p_ssh.p_task import PTask, PTaskCond, PTaskOutDisp, PTaskResult


class TestRunPBatch(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls._logger = AuditLogger(fh_or_fname=sys.stdout)
        return super().setUpClass()

    def _run_one(
        self,
        p_task_and_result_list: List[Tuple[PTask, PTaskResult]],
        n_parallel: int = 1,
        batch_timeout: float = 0,
        batch_cancel_max_wait: float = 0,
    ):
        p_tasks = [t_and_r[0] for t_and_r in p_task_and_result_list]
        got_results = run_p_batch(
            p_tasks,
            n_parallel=n_parallel,
            batch_timeout=batch_timeout,
            batch_cancel_max_wait=batch_cancel_max_wait,
        )
        for p_task, want_result in p_task_and_result_list:
            self.assertIn(p_task, got_results, msg="missing")
            got_result = got_results[p_task]
            for attr in PTaskResult.__dataclass_fields__:
                want_val = getattr(want_result, attr)
                got_val = getattr(got_result, attr)
                self.assertEqual(
                    want_val, got_val, msg=f"want != got {attr} for\n\t{p_task!r}"
                )
            del got_results[p_task]
        self.assertEqual(
            0,
            len(got_results),
            msg="unexpected tasks:\n\t" + "\n\t".join(map(repr, got_results)),
        )

    def test_batch_ok(self):
        print()
        for n_parallel in [1, 2, 0]:
            print(f"n_parallel={n_parallel}")
            p_task_and_result_list = [
                (
                    PTask(
                        "bash",
                        args=("-c", f"set -x; echo {k}"),
                        out_disp=PTaskOutDisp.COLLECT,
                        logger=self._logger,
                    ),
                    PTaskResult(
                        0,
                        bytes(f"{k}\n", "utf-8"),
                        bytes(f"+ echo {k}\n", "utf-8"),
                        PTaskCond.OK,
                    ),
                )
                for k in range(4)
            ]
            self._run_one(p_task_and_result_list, n_parallel=n_parallel)

    def test_batch_task_timeout(self):
        print()
        for n_parallel in [1, 2, 0]:
            print(f"n_parallel={n_parallel}")
            p_task_and_result_list = [
                (
                    PTask(
                        "bash",
                        args=("-c", f"set -x; echo {k} before; echo {k} after"),
                        out_disp=PTaskOutDisp.COLLECT,
                        logger=self._logger,
                    ),
                    PTaskResult(
                        0,
                        bytes(f"{k} before\n{k} after\n", "utf-8"),
                        bytes(f"+ echo {k} before\n+ echo {k} after\n", "utf-8"),
                        PTaskCond.OK,
                    ),
                )
                for k in range(2)
            ] + [
                (
                    PTask(
                        "bash",
                        args=(
                            "-c",
                            f"set -x; echo {k} before; sleep 3600; echo {k} after",
                        ),
                        timeout=0.2,
                        out_disp=PTaskOutDisp.COLLECT,
                        logger=self._logger,
                    ),
                    PTaskResult(
                        -signal.SIGTERM,
                        bytes(f"{k} before\n", "utf-8"),
                        bytes(f"+ echo {k} before\n+ sleep 3600\n", "utf-8"),
                        PTaskCond.TIMEOUT,
                    ),
                )
                for k in range(2, 4)
            ]
            self._run_one(p_task_and_result_list, n_parallel=n_parallel)

    def test_batch_timeout(self):
        print()
        for n_parallel in [1, 2, 0]:
            print(f"n_parallel={n_parallel}")
            p_task_and_result_list = [
                (
                    PTask(
                        "bash",
                        args=("-c", f"set -x; echo {k} before; echo {k} after"),
                        out_disp=PTaskOutDisp.COLLECT,
                        logger=self._logger,
                    ),
                    PTaskResult(
                        0,
                        bytes(f"{k} before\n{k} after\n", "utf-8"),
                        bytes(f"+ echo {k} before\n+ echo {k} after\n", "utf-8"),
                        PTaskCond.OK,
                    ),
                )
                for k in range(2)
            ] + [
                (
                    PTask(
                        "bash",
                        args=(
                            "-c",
                            "set -x; echo cancel before; sleep 3600; echo cancel after",
                        ),
                        out_disp=PTaskOutDisp.COLLECT,
                        logger=self._logger,
                    ),
                    PTaskResult(
                        -signal.SIGTERM,
                        bytes("cancel before\n", "utf-8"),
                        bytes(f"+ echo cancel before\n+ sleep 3600\n", "utf-8"),
                        PTaskCond.CANCELLED,
                    ),
                )
            ]
            if n_parallel == 0 or n_parallel >= 2:
                p_task_and_result_list.append(
                    (
                        PTask(
                            "bash",
                            args=(
                                "-c",
                                "trap '' TERM; set -x; echo term_ignore before; sleep 3600; echo term_ignore after",
                            ),
                            out_disp=PTaskOutDisp.COLLECT,
                            logger=self._logger,
                        ),
                        PTaskResult(
                            -signal.SIGKILL,
                            bytes("term_ignore before\n", "utf-8"),
                            bytes(
                                f"+ echo term_ignore before\n+ sleep 3600\n", "utf-8"
                            ),
                            PTaskCond.CANCELLED,
                        ),
                    )
                )
            self._run_one(
                p_task_and_result_list,
                n_parallel=n_parallel,
                batch_timeout=0.5,
                batch_cancel_max_wait=0.5,
            )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        unittest.main()
    else:
        unittest.main(verbosity=2, buffer=True)
