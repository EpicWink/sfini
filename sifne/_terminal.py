# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN termination states."""

import logging as lg

from . import _state

_logger = lg.getLogger(__name__)


class _Terminal(_state.State):
    def __init__(self, name, comment=None):
        super().__init__(name, comment=comment, end=True)

    def catch(self, exc, next_state):
        raise RuntimeError("Terminal state cannot have catch clause")

    def goes_to(self, state):
        raise RuntimeError("Cannot define next state for terminal state")


class Succeed(_Terminal):
    """End execution successfully.

    Args:
        name (str): name of state
        comment (str): state description
    """

    def __init__(self, name, comment=None):
        super().__init__(name, comment=comment)


class Fail(_Terminal):
    """End execution unsuccessfully.

    Args:
        name (str): name of state
        comment (str): state description
        cause (str): cause of failure
        error (str): name of failure error
    """

    def __init__(self, name, comment=None, cause=None, error=None):
        super().__init__(name, comment=comment)
        self.cause = cause
        self.error = error
