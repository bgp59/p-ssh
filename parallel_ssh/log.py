#! /usr/bin/env python3

"""Logger stderr (text) + file (JSONLine)

Easier to write a custom one than to adapt library or of-the-shelf existent ones.
"""

import json
import sys
import time
from collections import OrderedDict
from enum import Enum
from threading import RLock
from typing import Optional, TextIO


class Level(Enum):
    INFO = 1
    WARN = 2
    ERROR = 3
    FATAL = 4


P_SSH_AUDIT_TIMESTAMP_FIELD = "ts"
P_SSH_AUDIT_COMPONENT_FIELD = "comp"
P_SSH_AUDIT_LEVEL_FIELD = "lvl"
P_SSH_AUDIT_TEXT_FIELD = "txt"


class MultiLogger:
    _stream = None
    _fh = None

    def __init__(
        self, stream: Optional[TextIO] = sys.stderr, fname: Optional[str] = None
    ):
        self._lck = RLock()
        self.change(stream=stream, fname=fname)

    def change(
        self, stream: Optional[TextIO] = sys.stderr, fname: Optional[str] = None
    ):
        with self._lck:
            self._stream = stream
            if self._fh is not None:
                self._fh.close()
                self._fh = None
            if fname is not None:
                self._fh = open(fname, "wt")

    def log(
        self,
        txt: Optional[str] = None,
        ts: Optional[float] = None,
        lvl: Level = Level.INFO,
        comp: str = __package__,
        **kwargs,
    ):
        if ts is None:
            ts = time.time()
        usec = int((ts - int(ts)) * 1_000_000)
        log_ts = time.strftime(f"%Y-%m-%dT%H:%M:%S.{usec:06d}%z", time.localtime(ts))
        log_lvl = lvl.name
        with self._lck:
            if self._stream is not None:
                print(f"{log_ts} {comp} {log_lvl}", file=self._stream, end="")
                if txt is not None:
                    print("", txt, file=self._stream, end="")
                for k in kwargs:
                    print(f" {k}={kwargs[k]!r}", file=self._stream, end="")
                print(file=self._stream)
                self._stream.flush()
            if self._fh is not None:
                log_entry = OrderedDict(
                    {
                        P_SSH_AUDIT_TIMESTAMP_FIELD: log_ts,
                        P_SSH_AUDIT_COMPONENT_FIELD: comp,
                        P_SSH_AUDIT_LEVEL_FIELD: log_lvl,
                    }
                )
                if txt is not None:
                    log_entry[P_SSH_AUDIT_TEXT_FIELD] = txt
                log_entry.update(kwargs)
                json.dump(log_entry, self._fh)
                self._fh.write("\n")
                self._fh.flush()
        if lvl == Level.FATAL:
            raise RuntimeError(txt)

    def info(self, txt, **kwargs):
        self.log(lvl=Level.INFO, txt=txt, **kwargs)

    def warn(self, txt, **kwargs):
        self.log(lvl=Level.WARN, txt=txt, **kwargs)

    def error(self, txt, **kwargs):
        self.log(lvl=Level.ERROR, txt=txt, **kwargs)

    def fatal(self, txt, **kwargs):
        self.log(lvl=Level.FATAL, txt=txt, **kwargs)

    @property
    def lck(self):
        return self._lck

    def __del__(self):
        with self._lck:
            if self._fh is not None:
                self._fh.close()
                self._fh = None


default_logger = MultiLogger()
