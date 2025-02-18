# Parallel ssh/rsync Framework

<!-- TOC tocDepth:2..4 chapterDepth:2..6 -->

- [Description](#description)
- [Installation](#installation)
- [Usage](#usage)
  - [p-ssh](#p-ssh)
  - [p-rsync](#p-rsync)
  - [p-rsync-mkpath](#p-rsync-mkpath)
  - [p-report](#p-report)
- [Best Practices](#best-practices)
- [Development](#development)

<!-- /TOC -->

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

- support parallel invocation on N hosts at a time
- collect output (stdout/stderr) in a way that keeps track of the originating host
- support timeout limits, per hosts as well as overall for the entire set, since commands may hang
- maintain an audit trail in a machine readable format
- generate a list with failed hosts to allow for a fix-and-retry approach

This repo provides both command line utilities and Python modules for parallel ssh and rsync with the features above.

## Installation

**NOTE** Requires python >= 3.9

<!--  
  The URL below is updated automatically during build to point to the current release  
-->
```bash
pip install --upgrade \
  https://github.com/bgp59/p-ssh/releases/download/v1.0.2/p_ssh-1.0.2-py3-none-any.whl
```

## Usage

### p-ssh

```text
usage: p-ssh OPTION ... -- SSH_OPTION ...

Parallel SSH Invoker w/ audit trail and output recording.

The effect is that of invoking `ssh SSH_OPTION ...' for a batch of N targets at
a time, from a list of host specification.

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  -n N, --n-parallel N  The level of parallelism, 0 stands for unlimited (all
                        command invoked at once)
  -l HOST_LIST, --host-list HOST_LIST
                        Host spec file, in [USER@]HOST format. Lines starting
                        with `#' will be treated as comments and ignored and
                        duplicate specs will be removed. Multiple `-l' may be
                        specified and they will be consolidated
  -i INPUT_FILE, --input-file INPUT_FILE
                        Input file passed to the stdin of each ssh command. If
                        there are no ssh args, read the first line looking for a
                        shebang line and if found, use as implied command to
                        exec remotely
  -t TIMEOUT, --timeout TIMEOUT
                        If specified, individual ssh command timeout, in seconds
                        (float)
  -W TERM_MAX_WAIT, --term-max-wait TERM_MAX_WAIT
                        How long to wait, in seconds, for a command to exit upon
                        being terminated via SIGTERM (float). Default: 1.0 sec
  -B BATCH_TIMEOUT, --batch-timeout BATCH_TIMEOUT
                        If specified, the timeout for the entire batch, in
                        seconds (float)
  -a [WORKING_DIR], --audit-trail [WORKING_DIR]
                        Enable audit trail and output collection using the
                        optional path passed as a parameter. The path may
                        contain the following placeholders: `{n}': substitute
                        with `uname -n` (lowercase and stripped of domain),
                        `{p}': substitute with the PID of the process, `{U}:
                        substitute with the local user name. Additionally the
                        path may contain strftime formatting characters which
                        will be interpolated using the invocation time. If the
                        optional parameter is missing then a path rooted on
                        `P_SSH_WORKING_DIR_ROOT' env var or on an internal
                        fallback is used to form:
                        `/tmp/{U}/p_ssh/work/p-ssh/%Y-%m-%dT%H:%M:%S%z-{p}'.
  -x, --trace, --no-trace, --x, --no-x
                        Override the implied display of the result upon
                        individual command completion. If no audit trail is
                        specified then the implied action is to display the
                        result, otherwise it is to do nothing (since the output
                        is recorded anyway).

The SSH_OPTIONs may contain the following placeholders:

    `{s}': substituted with the full [USER@]HOST specification 
    `{h}': substituted with the HOST part  
    `{u}': substituted with the USER part.

Additionally `P_SSH_DEFAULT_OPTIONS' env var may be
defined with default ssh options to be prepended to the provided
arguments.
```

Examples

1. Setting working dir root on a NFS mounted file system. It is highly advisable
   that the path incorporates the local hostname in case the same setting is
   shared with other hosts with the same mount.

    ```bash

    export P_SSH_WORKING_DIR_ROOT=/share/{U}/{n}
    ```

2. Run a bash script remotely, for instance to collect server inventory data:

    - the script, `inventory.sh`

      ```bash
      #! /bin/bash --noprofile

      uname -a
      uptime
      ifconfig -a
      ```

    - the invocation:

      ```bash
      # -n 20: at most  20 parallel ssh sessions
      #    -a: create audit trail
      #    -x: also trace progress to stdout as ssh commands
      #        complete
 
      p-ssh -l HOST_FILE -n 20 -i inventory.sh -a -x
      ```

### p-rsync

```text
usage: p-rsync OPTION ... -- RSYNC_OPTION ...

Parallel Rsync Invoker w/ audit trail and output recording.

The effect is that of invoking `rsync RSYNC_OPTION ...' for a batch of N targets
at a time, from a list of host specification.
        

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  -n N, --n-parallel N  The level of parallelism, 0 stands for unlimited (all
                        command invoked at once)
  -l HOST_LIST, --host-list HOST_LIST
                        Host spec file, in [USER@]HOST format. Lines starting
                        with `#' will be treated as comments and ignored and
                        duplicate specs will be removed. Multiple `-l' may be
                        specified and they will be consolidated
  -t TIMEOUT, --timeout TIMEOUT
                        If specified, individual ssh command timeout, in seconds
                        (float)
  -W TERM_MAX_WAIT, --term-max-wait TERM_MAX_WAIT
                        How long to wait, in seconds, for a command to exit upon
                        being terminated via SIGTERM (float). Default: 1.0 sec
  -B BATCH_TIMEOUT, --batch-timeout BATCH_TIMEOUT
                        If specified, the timeout for the entire batch, in
                        seconds (float)
  -a [WORKING_DIR], --audit-trail [WORKING_DIR]
                        Enable audit trail and output collection using the
                        optional path passed as a parameter. The path may
                        contain the following placeholders: `{n}': substitute
                        with `uname -n` (lowercase and stripped of domain),
                        `{p}': substitute with the PID of the process, `{U}:
                        substitute with the local user name. Additionally the
                        path may contain strftime formatting characters which
                        will be interpolated using the invocation time. If the
                        optional parameter is missing then a path rooted on
                        `P_SSH_WORKING_DIR_ROOT' env var or on an internal
                        fallback is used to form:
                        `/tmp/{U}/p_ssh/work/p-rsync/%Y-%m-%dT%H:%M:%S%z-{p}'.
  -x, --trace, --no-trace, --x, --no-x
                        Override the implied display of the result upon
                        individual command completion. If no audit trail is
                        specified then the implied action is to display the
                        result, otherwise it is to do nothing (since the output
                        is recorded anyway).

The RSYNC_OPTION may contain the following placeholders:

    `{s}': substituted with the full [USER@]HOST specification 
    `{h}': substituted with the HOST part  
    `{u}': substituted with the USER part.

The `--' separator between p-rsync options and rsync ones is mandatory.

At least one of the RSYNC_OPTION should contain {s}:PATH
either for source or for destination.
```

Examples

1. From local to remote:

    ```bash
    p-rsync -l HOST_FILE -n 10 -a -- \
      -plrtHS -z --mkpath \
      /path/to/local/src/dir/ {s}:/path/to/remote/dir
    ```

    If `--mkpath` is not supported by the underlying rsync (pre 3.2.3) then the destination path has to be created beforehand:

    ```bash
    p-ssh -l HOST_FILE -n 50 -a -- mkdir -p /path/to/remote/dir
    ```

2. From remote to local:

    ```bash
    p-rsync -l HOST_FILE -n 10 -a -- \
      -plrtHS -z --mkpath \
      {s}:/path/to/remote/src/dir/ /local/root/{h}/path/to/dst/dir
    ```

    If `--mkpath` is not supported by the underlying rsync (pre 3.2.3) then the destination path has to be created beforehand:

    ```bash
    p-rsync-mkpath -l HOST_FILE /local/root/{h}/path/to/dst/dir
    ```

### p-rsync-mkpath

```text

usage: p-rsync-mkpath [-h] -l HOST_LIST DST [DST ...]

Create destination path as needed, either remotely or locally; the path may
include placeholders (see p-rsync -h). This is needed if the underlying
rsync is pre 3.2.3, when --mkpath option was added.

positional arguments:
  DST

options:
  -h, --help            show this help message and exit
  -l HOST_LIST, --host-list HOST_LIST
                        Host spec file, in [USER@]HOST format. Lines starting
                        with `#' will be treated as comments and ignored and
                        duplicate specs will be removed. Multiple `-l' may be
                        specified and they will be consolidated

```

### p-report

```text

usage: p-report [-h] [-r RETRY_FILE] [--stderr | --no-stderr]
                   [--stdout | --no-stdout] [-o REPORT_FILE] [-p]
                   AUDIT_FILE

Generate report based on p-... command audit trail.

positional arguments:
  AUDIT_FILE

options:
  -h, --help            show this help message and exit
  -r RETRY_FILE, --retry-file RETRY_FILE
                        Host spec retry file; the report will be generated
                        only for failed targets inside. Default: 'host-spec-
                        retry.list' under the same directory as the audit
                        trail file.
  --stderr, --no-stderr
                        Include/exclude stderr from the report. Default True.
  --stdout, --no-stdout
                        Include/exclude stdout from the report. Default False.
                        Note that stdout may be binary (non-text, that is), so
                        its inclusion should be considered carefully.
  -o REPORT_FILE, --out REPORT_FILE
                        Output for the report, use `-' for stdout. Default
                        'p-report.txt' under the same directory as the audit
                        trail file.
  -p, --pprint-events   Format events with pprint, instead of JSON.

```

## Best Practices

- always specify a timeout, at both task (command) and batch level
- use an audit trail, unless the number of target hosts is small and the
  commands are not too verbose (i.e. it is feasible to scroll up the terminal
  window and inspect the outcome)

## Development

**NOTE!** All the commands below should be invoked from the root dir of the project.

- if using VSCode, prime your (private) `.vscode/` from [.vscode-ref/](.vscode-ref/) (recommended)

- pre-requisites:

    ```bash
    ./tools/py_prerequisites.sh
    ```

- test:

    ```bash
    pytest -v -s
    ```

- format `.py` files:

    ```bash
    ./tools/py_format.sh
    ```

- use `bin/` to test run the commands directly from [src/](src/), e.g.:

    ```bash
    ./bin/p-ssh --help
    ```

- maintain version via `__version__` in [src/p_ssh/\_\_init\_\_.py](src/p_ssh/__init__.py)

- build the package (wheel under `dist`):

    ```bash
    ./tools/build.sh
    ```

- apply semver tag (it requires clean state in `main` branch, pushed to github):

    ```bash
    ./tools/git_tag_with_semver.sh
    ```
