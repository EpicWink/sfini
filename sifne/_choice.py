# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN choice."""

import logging as lg

from . import _state

_logger = lg.getLogger(__name__)


class Choice(_state.State):
    def __init__(
            self,
            name,
            choices,
            comment=None,
            default=None):
        super().__init__(name, comment=comment, end=False)
        self.choices = choices
        self.default = default

    def goes_to(self, state):
        raise RuntimeError("Cannot define next state for choice state")

    def catch(self, exc, next_state):
        raise RuntimeError("Choice state cannot have catch clause")
