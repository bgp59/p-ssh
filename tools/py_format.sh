#! /bin/bash

# Apply Python code formatting tools:

this_script=${0##*/}

case "$0" in
    /*|*/*) script_dir=$(dirname $(realpath $0));;
    *) script_dir=$(dirname $(realpath $(which $0)));;
esac

project_root_dir=$(realpath $script_dir/../..)

case "$1" in
    -h|--h*)
        echo >&2 "Usage: $this_script DIR ..."
        exit 1
    ;;
esac

for d in ${@:-.}; do
    real_d=$(realpath $d) || continue
    if [[ "$real_d" != "$project_root_dir" && "$real_d" != "$project_root_dir/"* ]]; then
        echo >&2 "$this_script: '$d' ignored, its real path '$real_d' is outside '$project_root_dir', the project root"
        continue
    fi
    (
        set -x
        autoflake -v --config $script_dir/setup.cfg $d # until the .toml supporting version shows in pip!
        isort --settings-path $script_dir $d
        black --config=$script_dir/pyproject.toml $d
    )
done
