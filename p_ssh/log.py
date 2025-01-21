#! /usr/bin/env python3

"""Audit logger
"""

import json
import sys
import time
from collections import OrderedDict
from enum import Enum
from threading import RLock
from typing import Optional, TextIO, Union


class Level(Enum):
    INFO = 1
    WARN = 2
    ERROR = 3
    FATAL = 4


P_SSH_AUDIT_TIMESTAMP_FIELD = "ts"
P_SSH_AUDIT_COMPONENT_FIELD = "comp"
P_SSH_AUDIT_LEVEL_FIELD = "lvl"
P_SSH_AUDIT_TEXT_FIELD = "txt"


def format_log_ts(ts: Optional[float] = None) -> str:
    if ts is None:
        ts = time.time()
    usec = int((ts - int(ts)) * 1_000_000)
    return time.strftime(f"%Y-%m-%dT%H:%M:%S.{usec:06d}%z", time.localtime(ts))


class AuditLogger:
    _fh = None
    _close_on_exit = False

    def __init__(self, fh_or_fname: Union[TextIO, str] = sys.stderr):
        self._lck = RLock()
        if isinstance(fh_or_fname, str):
            self._fh = open(fh_or_fname, "wt")
        else:
            self._fh = fh_or_fname

    def log(
        self,
        txt: Optional[str] = None,
        ts: Optional[float] = None,
        lvl: Level = Level.INFO,
        comp: str = __package__,
        **kwargs,
    ):
        if self._fh is None:
            return
        log_entry = OrderedDict(
            [
                (P_SSH_AUDIT_TIMESTAMP_FIELD, format_log_ts(ts)),
                (P_SSH_AUDIT_COMPONENT_FIELD, comp),
                (P_SSH_AUDIT_LEVEL_FIELD, lvl.name),
            ]
        )
        if txt is not None:
            log_entry[P_SSH_AUDIT_TEXT_FIELD] = txt
        log_entry.update(kwargs)
        with self._lck:
            json.dump(log_entry, self._fh)
            self._fh.write("\n")
        self._fh.flush()
        if lvl == Level.FATAL:
            raise RuntimeError(txt)

    def close(self):
        with self._lck:
            if self._fh is not None:
                if self._close_on_exit:
                    self._fh.close()
            self._fh = None

    def __del__(self):
        self.close()


default_logger = AuditLogger()
