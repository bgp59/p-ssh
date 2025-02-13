#! /usr/bin/env python3

"""Generate report based on audit trail"""

import argparse
import io
import json
import os
import pprint
import sys

from .. import (
    HOST_SPEC_RETRY_FILE,
    P_TASK_AUDIT_EVENT_FIELD,
    P_TASK_AUDIT_GEN_NUM_FIELD,
    P_TASK_AUDIT_HOST_FIELD,
    P_TASK_AUDIT_PID_FIELD,
    P_TASK_AUDIT_STDERR_FILE_FIELD,
    P_TASK_AUDIT_STDOUT_FILE_FIELD,
    P_TASK_AUDIT_USER_FIELD,
    REPORT_FILE,
    PTaskEvent,
    get_user_host,
    load_host_spec_file,
)


def main():
    parser = argparse.ArgumentParser(
        description=f"""
            Generate report based on p-... command audit trail. 
        """,
    )

    parser.add_argument(
        "-r",
        "--retry-file",
        help=f"""
            Host spec retry file; the report will be generated only for failed
            targets inside. Default: {HOST_SPEC_RETRY_FILE!r} under the same
            directory as the audit trail file.
        """,
    )
    parser.add_argument(
        "--stderr",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="""
            Include/exclude stderr from the report. Default %(default)r.
        """,
    )
    parser.add_argument(
        "--stdout",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="""
            Include/exclude stdout from the report. Default %(default)r. Note
            that stdout may be binary (non-text, that is), so its inclusion
            should be considered carefully.
        """,
    )
    parser.add_argument(
        "-o",
        "--out",
        metavar="REPORT_FILE",
        help=f"""
            Output for the report, use `-' for stdout. Default {REPORT_FILE!r}
            under the same directory as the audit trail file.
        """,
    )
    parser.add_argument(
        "-p",
        "--pprint-events",
        action="store_true",
        help=f"""
            Format events with pprint, instead of JSON.
        """,
    )

    parser.add_argument("audit_trail_file", metavar="AUDIT_FILE")

    args = parser.parse_args()
    audit_trail_file = args.audit_trail_file
    audit_trail_dir = os.path.dirname(audit_trail_file)

    host_spec_retry_file = (
        args.retry_file
        if args.retry_file is not None
        else os.path.join(audit_trail_dir, HOST_SPEC_RETRY_FILE)
    )
    try:
        host_spec_retry_list = load_host_spec_file(host_spec_retry_file)
    except FileNotFoundError as e:
        print(f"{e}, no report will be generated", file=sys.stderr)
        return
    if not host_spec_retry_list:
        print(
            f"Host spec retry list empty, no report will be generated", file=sys.stderr
        )
        return
    host_spec_sep = "*" * (max(map(len, host_spec_retry_list)) + 8)
    wanted_user_host_pairs = {
        get_user_host(host_spec): host_spec for host_spec in host_spec_retry_list
    }

    report_file = (
        args.out if args.out is not None else os.path.join(audit_trail_dir, REPORT_FILE)
    )
    if report_file == "-":
        report_fh = sys.stdout
    else:
        report_fh = open(report_file, "wt")

    # The records in the audit file are keyed by (pid, gen#) pairs and only the
    # START event ones have the user and host host. They records for a given ID
    # may show up interspersed with others with different ID's due to running in
    # parallel. Collect said records on a per ID basis, resolved at the time
    # when START even is encountered. The records are being dumped to the report
    # upon the COMPLETE event.
    audit_records_by_id = {}
    host_spec_by_id = {}

    with open(audit_trail_file, "rt") as audit_fh:
        for line in audit_fh:
            record = json.loads(line)
            record_id = record.get(P_TASK_AUDIT_PID_FIELD), record.get(
                P_TASK_AUDIT_GEN_NUM_FIELD
            )
            if record.get(P_TASK_AUDIT_EVENT_FIELD) == PTaskEvent.START.name:
                user_host = (
                    record.get(P_TASK_AUDIT_USER_FIELD),
                    record.get(P_TASK_AUDIT_HOST_FIELD),
                )
                if user_host in wanted_user_host_pairs:
                    audit_records_by_id[record_id] = []
                    host_spec_by_id[record_id] = wanted_user_host_pairs[user_host]
            if record_id not in audit_records_by_id:
                continue
            audit_records_by_id[record_id].append(record)
            if record.get(P_TASK_AUDIT_EVENT_FIELD) != PTaskEvent.COMPLETE.name:
                continue

            print(host_spec_sep, file=report_fh)
            print(f"* {host_spec_by_id[record_id]}:", file=report_fh)
            print(host_spec_sep, file=report_fh)
            print("\nEvents:", file=report_fh)
            if args.pprint_events:
                pprint.pprint(
                    audit_records_by_id[record_id],
                    stream=report_fh,
                    indent=2,
                    sort_dicts=False,
                )
            else:
                for record in audit_records_by_id[record_id]:
                    print(record, file=report_fh)
            del audit_records_by_id[record_id]
            report_fh.write("\n")

            for what, field, enabled in [
                ("Stdout", P_TASK_AUDIT_STDOUT_FILE_FIELD, args.stdout),
                ("Stderr", P_TASK_AUDIT_STDERR_FILE_FIELD, args.stderr),
            ]:
                if not enabled:
                    continue
                fpath = record.get(field)
                if fpath is None:
                    continue
                with open(fpath, "rt") as f:
                    has_data = False
                    while True:
                        buf = f.read(io.DEFAULT_BUFFER_SIZE)
                        if len(buf) == 0:
                            break
                        if not has_data:
                            print(f"{what}:", file=report_fh)
                            has_data = True
                        report_fh.write(buf)
                        ends_with_nl = buf.endswith("\n")
                if has_data:
                    if not ends_with_nl:
                        report_fh.write("\n")
                    report_fh.write("\n")

    if report_file != "-":
        report_fh.close()
        print(f"Report: {report_file!r}")
    return 0
