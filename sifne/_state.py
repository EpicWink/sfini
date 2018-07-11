# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN state."""

import logging as lg

_logger = lg.getLogger(__name__)


class State:
    def __init__(self, name, comment=None, end=False):
        self.name = name
        self.comment = comment
        self.end = end

        self._next = None
        self._retries = {}
        self._catches = {}

    def goes_to(self, state):
        """Set next state after this state finishes.

        Args:
            state (State): state to execute next
        """

        if self.end:
            raise RuntimeError("Cannot set next state of a terminal state")

        self._next = state

    def to_dict(self):
        """Convert this state to a definition dictionary.

        Returns:
            dict: definition
        """

        raise NotImplementedError

    @staticmethod
    def _assert_str_exc(exc):
        errs = ("*", "ALL", "Timeout", "TaskFailed", "Permissions")
        if exc not in errs:
            _s = "Error name was '%s', must be one of: %s"
            raise ValueError(_s % (exc, errs))

    def retry(
            self,
            exc,
            interval=None,
            max_attempts=None,
            backoff_rate=None):
        """Add a retry condition.

        Args:
            exc (Exception or str): error for retry to be executed. If a
                string, must be one of '*', 'ALL', 'Timeout', 'TaskFailed',
                or 'Permissions' (see AWS Step Functions documentation)
            interval (int): (initial) retry interval (seconds)
            max_attempts (int): maximum number of attempts before
                re-raising error
            backoff_rate (float): retry interval increase factor between
                attempts
        """

        if isinstance(exc, str):
            self._assert_str_exc(exc)
            exc = "ALL" if exc == "*" else exc

        self._retries[exc] = {
            "interval": interval,
            "max_attempts": max_attempts,
            "backoff_rate": backoff_rate}

    def catch(self, exc, next_state):
        """Add a catch clause.

        Args:
            exc (Exception or str): error for catch clause to be executed.
                If a string, must be one of '*', 'ALL', 'Timeout',
                'TaskFailed', or 'Permissions' (see AWS Step Functions
                documentation)
            next_state (State): state to execute for catch clause
        """

        if isinstance(exc, str):
            self._assert_str_exc(exc)
            exc = "ALL" if exc == "*" else exc

        self._catches[exc] = next_state
