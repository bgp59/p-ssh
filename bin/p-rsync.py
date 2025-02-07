#! /usr/bin/env python3

"""
Parallel Rsync Invoker w/ audit trail

"""

import argparse
import sys
import time

from common import (
    DEFAULT_TERM_MAX_WAIT_SEC,
    p_ssh,
    process_batch_results,
)


def main():
    parser = argparse.ArgumentParser(
        description=f"""
            Parallel Rsync Invoker w/ audit trail. 
            
            The typical invocation is:
                `%(prog)s OPTION ... -- RSYNC_ARG ...'. 

            The optional arguments OPTION ... are listed below.
            
            The RSYNC_ARGs are mandatory and they should be prefixed by `--'.
            They may contain the following placeholders:
            `{p_ssh.HOST_SPEC_PLACEHOLDER}': substituted with the full
            [USER@]HOST specification, `{p_ssh.HOST_PLACEHOLDER}': substituted
            with the HOST part and `{p_ssh.USER_PLACEHOLDER}': substituted with
            the USER part. 
        """,
    )

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
        "-t",
        "--timeout",
        type=float,
        help="""
            If specified, individual ssh command timeout, in seconds (float)
        """,
    )
    parser.add_argument(
        "-W",
        "--term-max-wait",
        type=float,
        default=DEFAULT_TERM_MAX_WAIT_SEC,
        help="""
            How long to wait, in seconds, for a command to exit upon being
            terminated via SIGTERM (float). Default: %(default).1f sec
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
    default_working_dir_value = p_ssh.get_default_working_dir(comp="rsync")
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
            '%%')}'.
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

    args, rsync_args = parser.parse_known_args()

    # Sanity check rsync_args:
    if len(rsync_args) < 3 or rsync_args[0] != "--":
        raise RuntimeError(f"Invalid rsync args, not in `-- OPT SRC DST' format")
    rsync_args = rsync_args[1:]
    has_host_spec = False
    for arg in rsync_args:
        for spec in [p_ssh.HOST_SPEC_PLACEHOLDER, p_ssh.HOST_PLACEHOLDER]:
            if f"{spec}:" in arg:
                has_host_spec = True
                break
        if has_host_spec:
            break
    if not has_host_spec:
        raise RuntimeError(
            f"Invalid rsync args, missing host spec {p_ssh.HOST_SPEC_PLACEHOLDER}: or {p_ssh.HOST_PLACEHOLDER}:"
        )

    # Load mandatory host spec list:
    host_spec_list = p_ssh.load_host_spec_file(args.host_list)

    working_dir = (
        p_ssh.expand_working_dir(args.audit_trail)
        if args.audit_trail is not None
        else None
    )
    trace = args.trace if args.trace is not None else working_dir is None
    cb = p_ssh.DisplayTaskResultCB() if trace else None

    t_start = time.time()
    p_tasks, audit_trail_fname = p_ssh.run_p_remote_batch(
        "rsync",
        host_spec_list=host_spec_list,
        args=rsync_args,
        setpgid=False,
        working_dir=working_dir,
        timeout=args.timeout,
        term_max_wait=args.term_max_wait,
        n_parallel=args.n_parallel,
        batch_timeout=args.batch_timeout,
        cb=cb,
    )
    duration = time.time() - t_start
    retry_fname, all_ok = process_batch_results(p_tasks, audit_trail_fname)

    if audit_trail_fname is not None:
        print(f"Audit trail: {audit_trail_fname!r}")
    if retry_fname is not None:
        print(f"Retry list:  {retry_fname!r}")
    print(f"Completed with{'out' if all_ok else ''} errors in {duration:.03f} sec")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
