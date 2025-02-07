# Parallel ssh/rsync Framework

## Description

During the operation/administration of clusters of Linux (Unix) hosts the need may arise to run a (set of) command(s) on a list of hosts and/or to transfer files between them and a central location.

Typically this is accomplished via ad-hoc shell constructs as follows:

```bash
for host in $(cat host.list); do
    ssh $host CMD
done
```

```bash
for host in $(cat host.list); do
    rsync -plrtHS SRC $host:DST
done
```

While this may be adequate for a handful of hosts where everything works fine, for mid-size environments and above (>= 100 hosts) a more robust approach is needed, with the following features:

* support parallel invocation on N hosts at a time
* collect output (stdout/stderr) in a way that keeps track of the originating host
* support timeout limits, per hosts as well as overall for the entire set, since commands may hang
* maintain an audit trail in a machine readable format
* generate a list with failed hosts to allow for a fix-and-retry approach

This repo provides both command line utilities and Python modules for parallel ssh and rsync with the features above.

## Usage

### p-ssh.py

```text
usage: p-ssh.py [-h] [-n N] -l HOST_LIST [-i INPUT_FILE] [-t TIMEOUT]
                [-W TERM_MAX_WAIT] [-B BATCH_TIMEOUT] [-a [WORKING_DIR]]
                [-x | --trace | --no-trace | --x | --no-x]

Parallel SSH Invoker w/ audit trail. The typical invocation is: `p-ssh.py
OPTION ... -- SSH_ARG ...'. The optional arguments OPTION ... are listed
below. The SSH_ARGs may contain the following placeholders: `{s}': substituted
with the full [USER@]HOST specification, `{h}': substituted with the HOST part
and `{u}': substituted with the USER part. Additionally
`P_SSH_DEFAULT_OPTIONS' env var may be defined with default ssh options to be
prepended to the provided arguments.

options:
  -h, --help            show this help message and exit
  -n N, --n-parallel N  The level of parallelism, 0 stands for unlimited (all
                        command invoked at once)
  -l HOST_LIST, --host-list HOST_LIST
                        Host spec file, in [USER@]HOST format. Lines starting
                        with `#' will be treated as comments and ignored and
                        duplicate specs will be removed. Multiple `-l' may be
                        specified and they will be consolidated
  -i INPUT_FILE, --input-file INPUT_FILE
                        Input file passed to the stdin of each ssh command. If
                        there are no ssh args, read the first line looking for
                        a shebang line and if found, use as implied command to
                        exec remotely
  -t TIMEOUT, --timeout TIMEOUT
                        If specified, individual ssh command timeout, in
                        seconds (float)
  -W TERM_MAX_WAIT, --term-max-wait TERM_MAX_WAIT
                        How long to wait, in seconds, for a command to exit
                        upon being terminated via SIGTERM (float). Default:
                        1.0 sec
  -B BATCH_TIMEOUT, --batch-timeout BATCH_TIMEOUT
                        If specified, the timeout for the entire batch, in
                        seconds (float)
  -a [WORKING_DIR], --audit-trail [WORKING_DIR]
                        Enable audit trail and output collection using the
                        optional path passed as a parameter. The path may
                        contain the following placeholders: `{n}': substitute
                        with `uname -n` (lowercase and stripped of domain),
                        `{p}': substitute with the PID of the process.
                        Additionally the path may contain strftime formatting
                        characters which will be interpolated using the
                        invocation time. If the optional parameter is missing
                        then a path rooted on `P_SSH_WORKING_DIR_ROOT' env var
                        or on an internal fallback is used to form: 
                        `...github/p-ssh/.work/{n}/ssh/%Y-%m-%dT%H:%M:%S%z-{p}'.
  -x, --trace, --no-trace, --x, --no-x
                        Override the implied display of the result upon
                        individual command completion. If no audit trail is
                        specified then the implied action is to display the
                        result, otherwise it is to do nothing (since the
                        output is recorded anyway).
```

### p-rsync.py

```text
usage: p-rsync.py [-h] [-n N] -l HOST_LIST [-t TIMEOUT] [-W TERM_MAX_WAIT]
                  [-B BATCH_TIMEOUT] [-a [WORKING_DIR]]
                  [-x | --trace | --no-trace | --x | --no-x]

Parallel Rsync Invoker w/ audit trail. The typical invocation is: `p-rsync.py
OPTION ... -- RSYNC_ARG ...'. The optional arguments OPTION ... are listed
below. The RSYNC_ARGs are mandatory and they should be prefixed by `--'. They
may contain the following placeholders: `{s}': substituted with the full
[USER@]HOST specification, `{h}': substituted with the HOST part and `{u}':
substituted with the USER part.

options:
  -h, --help            show this help message and exit
  -n N, --n-parallel N  The level of parallelism, 0 stands for unlimited (all
                        command invoked at once)
  -l HOST_LIST, --host-list HOST_LIST
                        Host spec file, in [USER@]HOST format. Lines starting
                        with `#' will be treated as comments and ignored and
                        duplicate specs will be removed. Multiple `-l' may be
                        specified and they will be consolidated
  -t TIMEOUT, --timeout TIMEOUT
                        If specified, individual ssh command timeout, in
                        seconds (float)
  -W TERM_MAX_WAIT, --term-max-wait TERM_MAX_WAIT
                        How long to wait, in seconds, for a command to exit
                        upon being terminated via SIGTERM (float). Default:
                        1.0 sec
  -B BATCH_TIMEOUT, --batch-timeout BATCH_TIMEOUT
                        If specified, the timeout for the entire batch, in
                        seconds (float)
  -a [WORKING_DIR], --audit-trail [WORKING_DIR]
                        Enable audit trail and output collection using the
                        optional path passed as a parameter. The path may
                        contain the following placeholders: `{n}': substitute
                        with `uname -n` (lowercase and stripped of domain),
                        `{p}': substitute with the PID of the process.
                        Additionally the path may contain strftime formatting
                        characters which will be interpolated using the
                        invocation time. If the optional parameter is missing
                        then a path rooted on `P_SSH_WORKING_DIR_ROOT' env var
                        or on an internal fallback is used to form: 
                        `.../github/p-ssh/.work/{n}/rsync/%Y-%m-%dT%H:%M:%S%z-{p}'.
  -x, --trace, --no-trace, --x, --no-x
                        Override the implied display of the result upon
                        individual command completion. If no audit trail is
                        specified then the implied action is to display the
                        result, otherwise it is to do nothing (since the
                        output is recorded anyway).
```
