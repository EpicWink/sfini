# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN state."""

import math
import logging as lg

_logger = lg.getLogger(__name__)


class State:  # TODO: unit-test
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
        elif not isinstance(exc, Exception):
            raise TypeError("Error must be exception or accepted string")

        if exc in self._retries:
            raise ValueError("Error '%s' already registered" % exc)

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

        if exc in self._catches:
            raise ValueError("Error '%s' already registered" % exc)

        self._catches[exc] = next_state

    @staticmethod
    def _compare(defn_key, defn, retry_val, compare_key=None):
        compare_key = compare_key or (lambda a, b: a == b)
        if defn_key in defn:
            return compare_key(defn[defn_key], retry_val)
        else:
            return retry_val is None

    def _retries_equal(self, defn, retry):
        ic = math.isclose
        return all((
            self._compare("IntervalSeconds", defn, retry["interval"]),
            self._compare("MaxAttempts", defn, retry["max_attempts"]),
            self._compare("BackoffRate", defn, retry["backoff_rate"], ic)))

    def _retries_defn(self):
        defns = []
        all_defn = None
        for exc, retry in self._retries.items():
            # Convert error to string
            if isinstance(exc, str):
                exc = "States." + exc
            elif isinstance(exc, Exception):
                exc = str(exc)

            # Search for already-defined retries
            for defn_ in defns:
                if self._retries_equal(defn_, retry):
                    defn_["ErrorEquals"].append(exc)
                    break
            else:
                defn = {
                    "ErrorEquals": [exc],
                    "IntervalSeconds": retry["interval"],
                    "MaxAttempts": retry["max_attempts"],
                    "BackoffRate": retry["backoff_rate"]}

                # Defer adding wildcard error until end (AWS SFN spec)
                if exc == "States.ALL":
                    all_defn = defn
                    continue

                defns.append(defn)

        if all_defn is not None:  # append ALL retry at the end
            defns.append(all_defn)

        return defns

    def _catches_defn(self):
        defns = []
        all_defn = None
        for exc, state in self._catches.items():
            # Convert error to string
            if isinstance(exc, str):
                exc = "States." + exc
            elif isinstance(exc, Exception):
                exc = str(exc)

            # Search for already-defined retries
            for defn_ in defns:
                if defn_["Next"] == state.name:
                    defn_["ErrorEquals"].append(exc)
                    break
            else:
                defn = {
                    "ErrorEquals": [exc],
                    "ResultPath": "$.error-info",
                    "Next": state.name}

                # Defer adding wildcard error until end (AWS SFN spec)
                if exc == "States.ALL":
                    all_defn = defn
                    continue

                defns.append(defn)

        if all_defn is not None:  # append ALL retry at the end
            defns.append(all_defn)

        return defns
