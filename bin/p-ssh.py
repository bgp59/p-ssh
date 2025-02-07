#! /usr/bin/env python3

"""
Parallel SSH Invoker w/ audit trail

"""

import argparse
from collections import defaultdict
import os
import shlex
import sys
import time
from typing import Optional

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import p_ssh

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


def get_shebang_line(fname: str) -> Optional[str]:
    """Check #! /path/to/interpreter"""
    with open(fname, "rb") as f:
        if f.read(2) != b"#!":
            return None
        line = str(f.read(SHEBANG_MAX_LINE_SIZE), "utf-8")
        i = line.find("\n")
        if i == -1:
            return None
        interpreter = line[:i].strip()
        return interpreter if len(interpreter) > 0 else None


def main():
    parser = argparse.ArgumentParser(
        description=f"""
            Parallel SSH Invoker w/ audit trail. 
            
            The typical invocation is:
                `p-ssh.py OPTION ... -- SSH_ARG ...'. 
            
            The OPTIONs are listed below.
            
            The SSH_ARGs may contain the following placeholders:
            `{p_ssh.HOST_SPEC_PLACEHOLDER}': substituted with the full
            [USER@]HOST specification, `{p_ssh.HOST_PLACEHOLDER}': substituted
            with the HOST part and `{p_ssh.USER_PLACEHOLDER}': substituted with
            the USER part.

            Additionally `{P_SSH_DEFAULT_OPTIONS_ENV_VAR}' env var may be
            defined with default ssh options to be prepended to the provided
            arguments.
        """,
    )

    default_working_dir_value = p_ssh.get_default_working_dir(comp="ssh")
    parser.add_argument(
        "-n",
        "--n-parallel",
        type=int,
        default=1,
        metavar="N",
        help="""
            The level of parallelism, 0 stands for unlimited (all command
            invoked at once)
        """,
    )
    parser.add_argument(
        "-l",
        "--host-list",
        action="append",
        required=True,
        help="""
            Host spec file, in [USER@]HOST format. Lines starting with `#' will
            be treated as comments and ignored and duplicate specs will be
            removed. Multiple `-l' may be specified and they will be
            consolidated
        """,
    )
    parser.add_argument(
        "-i",
        "--input-file",
        help="""
            Input file passed to the stdin of each ssh command. If there are no
            ssh args, read the first line looking for a shebang line and if
            found, use as implied command to exec remotely
        """,
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=float,
        help="""
            If specified, individual ssh command timeout, in seconds (float)
        """,
    )
    parser.add_argument(
        "-T",
        "--term-max-wait",
        type=float,
        default=DEFAULT_TERM_MAX_WAIT_SEC,
        help="""
            If specified, how long to wait, in seconds, for a command to exit
            upon being terminated via SIGTERM (float). Default: %(default).1f sec
        """,
    )
    parser.add_argument(
        "-B",
        "--batch-timeout",
        type=float,
        help="""
            If specified, the timeout for the entire batch, in seconds (float)
        """,
    )
    parser.add_argument(
        "-a",
        "--audit-trail",
        nargs="?",
        metavar="WORKING_DIR",
        const=default_working_dir_value,
        help=f"""
            Enable audit trail and output collection using the optional path
            passed as a parameter. 
            
            The path may contain the following placeholders:
            `{p_ssh.LOCAL_HOSTNAME_PLACEHOLDER}': substitute with `uname -n`
            (lowercase and stripped of domain), `{p_ssh.PID_PLACEHOLDER}':
            substitute with the PID of the process. Additionally the path may
            contain strftime formatting characters which will be interpolated
            using the invocation time.
        
            If the optional parameter is missing then a path rooted on
            `{p_ssh.P_SSH_WORKING_DIR_ROOT_ENV_VAR}' env var or on an internal
            fallback is used to form: `{default_working_dir_value.replace('%',
            '%%')}.
        """,
    )
    parser.add_argument(
        "-x",
        "--trace",
        "--x",
        action=argparse.BooleanOptionalAction,
        help="""
            Override the implied display of the result upon individual command
            completion. If no audit trail is specified then the implied action
            is to display the result, otherwise it is to do nothing (since the
            output is recorded anyway).
        """,
    )

    args, ssh_args = parser.parse_known_args()

    # Load mandatory host spec list:
    host_spec_list = p_ssh.load_host_spec_file(args.host_list)

    # If there is an input file then verify it's readable and look a shebang
    # line for a potential interpreter:
    input_file = args.input_file
    interpreter = get_shebang_line(input_file) if input_file is not None else None

    # Extract ssh args:
    if len(ssh_args) > 0 and ssh_args[0] == "--":
        ssh_args = ssh_args[1:]
    has_cmdline_ssh_args = len(ssh_args) > 0

    # Inspect the ssh_args for host spec placeholder; if none found, prepend it:
    if not has_cmdline_ssh_args or p_ssh.HOST_SPEC_PLACEHOLDER not in ssh_args:
        ssh_args = [p_ssh.HOST_SPEC_PLACEHOLDER] + (ssh_args or [])
    # Prepend default ssh options, if any:
    default_ssh_options = os.environ.get(P_SSH_DEFAULT_OPTIONS_ENV_VAR)
    if default_ssh_options is not None:
        ssh_args = shlex.split(default_ssh_options) + ssh_args
    # Prepend built-in options, if any:
    if P_SSH_BUILT_IN_OPTIONS:
        ssh_args = P_SSH_BUILT_IN_OPTIONS + ssh_args
    # If there were no ssh args on the command line and an interpreter was
    # gleaned from the input file then the former becomes the command to
    # execute:
    if not has_cmdline_ssh_args and interpreter is not None:
        ssh_args.append(f"exec {interpreter}")

    working_dir = (
        p_ssh.expand_working_dir(args.audit_trail)
        if args.audit_trail is not None
        else None
    )
    trace = args.trace if args.trace is not None else working_dir is None
    cb = p_ssh.DisplayTaskResultCB() if trace else None

    t_start = time.time()
    p_tasks, audit_trail_fname = p_ssh.run_p_remote_batch(
        "ssh",
        host_spec_list=host_spec_list,
        args=ssh_args,
        working_dir=working_dir,
        timeout=args.timeout,
        term_max_wait=args.term_max_wait,
        input_fname=args.input_file,
        n_parallel=args.n_parallel,
        batch_timeout=args.batch_timeout,
        cb=cb,
    )
    duration = time.time() - t_start

    # Sift through the p_tasks and classify the failures:
    failed = defaultdict(list)
    for p_task in p_tasks:
        result = p_task.result
        if result is None:
            cond = p_ssh.PTaskCond.INCOMPLETE
        else:
            cond = result.cond
        if cond != p_ssh.PTaskCond.OK:
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

    if audit_trail_fname is not None:
        print(f"Audit trail: {audit_trail_fname!r}")
    if retry_fname is not None:
        print(f"Retry list:  {retry_fname!r}")
    print(f"Completed in {duration:.03f} sec")
    return 1 if len(failed) > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
