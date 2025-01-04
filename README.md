# Parallel ssh/rsync Framework

## Motivation

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
* create 2 lists, for successful and failed hosts, in order to allow for a fix-and-retry approach

## Solution

This repo provides both command line utilities and Python modules for parallel ssh and rsync with the features above.

