#! /usr/bin/env python3

import sys

min_python_ver = (3, 8)
curr_python_ver = (
    sys.version_info.major,
    sys.version_info.minor,
    sys.version_info.micro,
)


def semver_fmt(semver):
    return ".".join(map(str, semver))


assert (
    curr_python_ver >= min_python_ver
), f"Python {semver_fmt(curr_python_ver)}, want at least {semver_fmt(min_python_ver)}"
print(f"Python {semver_fmt(curr_python_ver)} OK")
