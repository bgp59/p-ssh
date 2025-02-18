#! /bin/bash --noprofile

# Execute python build

case "$0" in
    /*|*/*) script_dir=$(dirname $(realpath $0));;
    *) script_dir=$(dirname $(realpath $(which $0)));;
esac
export PATH="$script_dir${PATH:+:}${PATH}"

project_root_dir=$(realpath $script_dir/..)

set -ex
update_readme.py # Required *before* build since README is part of the pkg
cd $project_root_dir
python3 -m build --wheel
