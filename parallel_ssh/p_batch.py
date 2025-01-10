#! /usr/bin/env python3

"""
asyncio execution of parallel ssh/rsync commands
"""

import asyncio
import time
from typing import Dict, List

from .p_task import PTask, PTaskCondition, PTaskEvent, PTaskResult


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
        p_task.log_event(
            event=PTaskEvent.CONDITION,
            P_TASK_AUDIT_CONDITION_FIELD=PTaskCondition.LINGER.name,
        )
        results[p_task] = PTaskResult(cond=PTaskCondition.LINGER)

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
