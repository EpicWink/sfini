# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN task."""

import logging as lg

from . import _state

_logger = lg.getLogger(__name__)


class Task(_state.State):
    def __init__(
            self,
            name,
            fn,
            comment=None,
            end=False,
            timeout=None,
            heartbeat=60):
        super().__init__(name, comment=comment, end=end)
        self.fn = fn
        self.timeout = timeout
        self.heartbeat = heartbeat
