#! python3

"""Common definitions for p-*.py tools"""

import os
import sys
from collections import defaultdict
from typing import Iterable, Optional, Tuple

from .p_task import PRemoteTask, PTaskCond

# Built-in ssh options:
P_SSH_BUILT_IN_OPTIONS = [
    "-T",
]

# Max length of the shebang (#!/path/to/interpreter) line lookup:
SHEBANG_MAX_LINE_SIZE = 1024

# Env var with default ssh options:
P_SSH_DEFAULT_OPTIONS_ENV_VAR = "P_SSH_DEFAULT_OPTIONS"

# The base name of the file with host specs to be retried. It will be created
# under the same directory as the audit trail, if the latter is in use.
HOST_SPEC_RETRY_FILE = "host-spec-retry.list"

# Default task termination max wait:
DEFAULT_TERM_MAX_WAIT_SEC = 1

REPORT_FILE = "p-report.txt"


def process_batch_results(
    p_tasks: Iterable[PRemoteTask],
    audit_trail_fname: Optional[str] = None,
) -> Tuple[Optional[str], bool]:
    # Sift through the p_tasks and classify the failures:
    failed = defaultdict(list)
    for p_task in p_tasks:
        result = p_task.result
        if result is None:
            cond = PTaskCond.INCOMPLETE
        else:
            cond = result.cond
        if cond != PTaskCond.OK:
            failed[cond].append(p_task.host_spec)

    retry_fname = None
    if failed:
        if audit_trail_fname is not None:
            retry_fname = os.path.join(
                os.path.dirname(audit_trail_fname), HOST_SPEC_RETRY_FILE
            )
            retry_fh = open(retry_fname, "wt")
        else:
            retry_fh = sys.stdout

        for cond in sorted(failed):
            print(f"# {cond.name}:", file=retry_fh)
            for host_spec in failed[cond]:
                print(host_spec, file=retry_fh)
            print(file=retry_fh)

        if retry_fname is not None:
            retry_fh.close()

    return retry_fname, len(failed) == 0
