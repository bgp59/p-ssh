#! /usr/bin/env python3

"""
Helper for p-rsync.py, create local dirs if they include placeholders

e.g.

run:

    mkdir-local-dirs.py -l HOST_SPEC_LIST_FILE 'path/to/{h}/dst'

before:

    p-rsync.py -l HOST_SPEC_LIST_FILE -- -plrtHS 'path/to/src/' 'path/to/{h}/dst'

"""


import argparse
import os

from common import (
    p_ssh,
)


def main():
    parser = argparse.ArgumentParser(
        description=f"""
            p-rsync.py helper for creating local destination dirs whose path may
            include placeholders (see p-rsync.py -h)
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
    parser.add_argument("ph_path_list", metavar="PLACEHOLDER_PATH", nargs="+")

    args = parser.parse_args()
    host_spec_list = p_ssh.load_host_spec_file(args.host_list)
    for host_spec in host_spec_list:
        for ph_path in args.ph_path_list:
            os.makedirs(
                p_ssh.replace_placeholders(ph_path, host_spec),
                exist_ok=True,
            )


if __name__ == "__main__":
    main()
