#! /bin/bash --noprofile

# Execute python build

case "$0" in
    /*|*/*) script_dir=$(dirname $(realpath $0));;
    *) script_dir=$(dirname $(realpath $(which $0)));;
esac

project_root_dir=$(realpath $script_dir/..)

set -ex
cd $project_root_dir
python3 -m build --wheel
