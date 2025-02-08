#! /usr/bin/env python3

"""
Helper for p-rsync.py, needed for rsync pre 3.2.3, when --mkpath option was added.

Create destination path as needed, either remote or locally.

e.g.

run:

    p-rysnc-mkpath.py -l HOST_SPEC_LIST_FILE 'path/to/{h}/dst'

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
            Create destination path as needed, either remote or locally; the
            path may include placeholders (see p-rsync.py -h). This is needed if
            the underlying rsync is pre 3.2.3, when --mkpath option was added.
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
    parser.add_argument("dst_path_list", metavar="DST", nargs="+")

    args = parser.parse_args()
    host_spec_list = p_ssh.load_host_spec_file(args.host_list)
    for host_spec in host_spec_list:
        for dst_path in args.dst_path_list:
            os.makedirs(
                p_ssh.replace_placeholders(dst_path, host_spec),
                exist_ok=True,
            )


if __name__ == "__main__":
    main()
