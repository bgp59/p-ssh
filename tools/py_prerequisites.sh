#! /bin/bash --noprofile

# Python pre-requisites

case "$0" in
    /*|*/*) this_dir=$(cd $(dirname $0) && pwd);;
    *) this_dir=$(cd $(dirname $(which $0)) && pwd);;
esac

set -e
cd $this_dir
./check_python_ver.py
pip3 install --upgrade pip
pip3 install --upgrade -r requirements.txt

