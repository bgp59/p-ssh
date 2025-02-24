#! /usr/bin/env python3

"""Update README.md w/ latest info:
- version for release install
"""

import os
import re
import sys

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(root_dir, "src"))

import p_ssh

readme_file = os.path.join(root_dir, "README.md")

updated = False
in_memory = []
with open(readme_file, "rt") as r:
    for line in r:
        m = re.match(
            r"""(.*https://github.com/.+/releases/download/v)[^/]*(/[^-]+-)[^-]+(-(?s:.)+)""",
            line,
        )
        if m is not None:
            new_line = p_ssh.__version__.join(m.groups())
            if new_line != line:
                updated = True
                in_memory.append(new_line)
                updated = True
            continue
        in_memory.append(line)

if updated:
    with open(readme_file, "wt") as w:
        for line in in_memory:
            w.write(line)
