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

work_dir = os.path.join(root_dir, ".work")
os.makedirs(work_dir, exist_ok=True)

readme_file = os.path.join(root_dir, "README.md")
tmp_readme_file = os.path.join(work_dir, "README.md")

with open(readme_file, "rt") as r, open(tmp_readme_file, "wt") as w:
    download_url_updated = False
    for line in r:
        if not download_url_updated:
            m = re.match(
                r"""(.*https://github.com/.+/releases/download/v)[^/]+(/[^-]+-)[^-]+(-(?s:.)+)""",
                line,
            )
            if m is not None:
                line = p_ssh.__version__.join(m.groups())
                download_url_updated = True
        w.write(line)

os.rename(tmp_readme_file, readme_file)
