#! /bin/bash --noprofile

git_required_branch="main"

this_script=${0##*/}

usage="
Usage: $this_script [TAG]

Check that the tree is on '$git_required_branch' branch, in a clean state and that 
the optional TAG is applied.
"
case "$0" in
    /*|*/*) script_dir=$(dirname $(realpath $0));;
    *) script_dir=$(dirname $(realpath $(which $0)));;
esac

project_root_dir=$(realpath $script_dir/..)

case "$1" in
    -h|--h*)
        echo >&2 "$usage"
        exit 1
        ;;
esac
check_tag="$1"; shift

case "$__p_ssh_skip_git_state_check" in
    1|[Yy]*|[Tt]*)
        echo >&2 "$this_script: __p_ssh_skip_git_state_check=$__p_ssh_skip_git_state_check, check skipped"
        exit 0
        ;;
esac

set -e
git_curr_branch=$(git branch --show-current)
if [[ "$git_curr_branch" != "$git_required_branch" ]]; then
    echo >&2 "$this_script: current git branch: '$git_curr_branch', want: '$git_required_branch'"
    exit 1
fi

git_status=$(cd $project_root_dir && git status --porcelain)
if [[ -n "$git_status" ]]; then
    echo >&2 "$this_script: git status unclean:"
    echo >&2 "$git_status"
    exit 1
fi

git_status_branch=$(cd $project_root_dir && git status --branch)
if [[ "$git_status_branch" != *"branch is up to date"* ]]; then
    echo >&2 "$this_script: git status unclean:"
    echo >&2 "$git_status_branch"
    exit 1
fi

# If an argument was provided, check if HEAD is tagged with it:
if [[ -n "$check_tag" ]]; then
    tag_commit=$(git log -n 1 --format=oneline $check_tag | awk '(NR == 1){print $1}')
    if [[ -z "$tag_commit" ]]; then
        echo >&2 "$this_script: cannot find commit for '$check_tag' tag"
        exit 1
    fi
    current_commit=$(git log -n 1 --format=oneline | awk '(NR == 1){print $1}')
    if [[ "$tag_commit" != "$current_commit" ]]; then
        echo >&2 "$this_script: current commit: '$current_commit', want: '$tag_commit', matching '$check_tag' tag"
        exit 1
    fi
fi
exit 0
