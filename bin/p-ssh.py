#! /usr/bin/env python3

"""
Parallel SSH Invoker w/ audit trail

"""

import argparse
import os
import shlex
import sys

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import p_ssh

# Env var with default ssh options:
P_SSH_DEFAULT_OPTIONS_ENV_VAR = "P_SSH_DEFAULT_OPTIONS"

DEFAULT_TERM_MAX_WAIT_SEC = 1
DEFAULT_BATCH_CANCEL_MAX_WAIT_SEC = 2


def main():
    parser = argparse.ArgumentParser(
        description=f"""
            Parallel SSH Invoker w/ audit trail. 
            
            The typical invocation is:
                `p-ssh.py [OPTION] ... -- SSH_ARG ...'. 
            
            The OPTIONs are listed below.
            
            The SSH_ARGs may contain the following placeholders:
            `{p_ssh.HOST_SPEC_PLACEHOLDER}': substituted with the full
            [USER@]HOST specification, `{p_ssh.HOST_PLACEHOLDER}': substituted
            with the HOST part and `{p_ssh.USER_PLACEHOLDER}': substituted with
            the USER part.

            Additionally `{P_SSH_DEFAULT_OPTIONS_ENV_VAR}' env var may be
            defined with default ssh options to be prepended to the provided
            arguments.ß
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
        required=True,
        help="""
            Host spec file, in [USER@]HOST format. Lines starting with `#' will
            be treated as comments and ignored. Duplicate specs will be
            removed
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
        "-C",
        "--batch-cancel-max-wait",
        type=float,
        default=DEFAULT_BATCH_CANCEL_MAX_WAIT_SEC,
        help="""
            If specified, how long to wait, in seconds, for a batch to be
            cancelled (float). Default: %(default).1f sec
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

    # Extract ssh args:
    if len(ssh_args) > 0 and ssh_args[0] == "--":
        ssh_args = ssh_args[1:]
    # Inspect the ssh_args for host spec placeholder; if none found, prepend it:
    if not ssh_args or p_ssh.HOST_SPEC_PLACEHOLDER not in ssh_args:
        ssh_args = [p_ssh.HOST_SPEC_PLACEHOLDER] + (ssh_args or [])
    # Prepend default ssh options, if any:
    default_ssh_options = os.environ.get(P_SSH_DEFAULT_OPTIONS_ENV_VAR)
    if default_ssh_options is not None:
        ssh_args = shlex.split(default_ssh_options) + ssh_args

    working_dir = (
        p_ssh.expand_working_dir(args.audit_trail)
        if args.audit_trail is not None
        else None
    )
    trace = args.trace if args.trace is not None else working_dir is None
    cb = p_ssh.DisplayTaskResultCB() if trace else None

    audit_trail_fname = p_ssh.run_p_remote_batch(
        "ssh",
        host_spec_list=host_spec_list,
        args=ssh_args,
        working_dir=working_dir,
        timeout=args.timeout,
        term_max_wait=args.term_max_wait,
        n_parallel=args.n_parallel,
        batch_timeout=args.batch_timeout,
        batch_cancel_max_wait=args.batch_cancel_max_wait,
        cb=cb,
    )

    if audit_trail_fname is not None:
        if cb is not None:
            print()
        print(f"Audit trail in {audit_trail_fname!r}")


if __name__ == "__main__":
    main()
