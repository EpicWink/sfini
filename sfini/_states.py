# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN states."""

import math
import datetime
import logging as lg
import functools as ft

from . import _util

_logger = lg.getLogger(__name__)


def _assert_str_exc(exc):
    errs = ("*", "ALL", "Timeout", "TaskFailed", "Permissions")
    if exc not in errs:
        _s = "Error name was '%s', must be one of: %s"
        raise ValueError(_s % (exc, errs))


def _compare_defn(defn_key, defn, retry_val, compare_key=None):  # TODO: unit-test
    compare_key = compare_key or (lambda a, b: a == b)
    if defn_key in defn:
        return compare_key(defn[defn_key], retry_val)
    else:
        return retry_val is None


class State:  # TODO: unit-test
    """Abstract state.

    Args:
        name (str): name of state
        comment (str): state description
    """

    def __init__(self, name, comment=None):
        self.name = name
        self.comment = comment

    def to_dict(self):
        """Convert this state to a definition dictionary.

        Returns:
            dict: definition
        """

        defn = {"Type": type(self).__name__}
        if self.comment is not None:
            defn["Comment"] = self.comment
        return defn

    def add_to(self, states):
        """Add this state to a states collection.

        Args:
            states (dict[str, State]): states to add to
        """

        if self.name in states:
            if states[self.name] is not self:
                _s = "Multiple states defined with name '%s'"
                raise RuntimeError(_s % self.name)
        else:
            states[self.name] = self


class _HasNext(State):  # TODO: unit-test
    def __init__(self, name, comment=None):
        super().__init__(name, comment=comment)
        self._next = None

    def goes_to(self, state):
        """Set next state after this state finishes.

        Args:
            state (State): state to execute next
        """

        self._next = state

    def clear_next_state(self):
        """Remove next state, making this state terminal."""
        self._next = None

    def to_dict(self):
        defn = super().to_dict()
        if self._next is None:
            defn["End"] = True
        else:
            defn["Next"] = self._next.name
        return defn

    def add_to(self, states):
        super().add_to(states)
        if self._next is not None:
            self._next.add_to(states)


class _CanRetry(State):  # TODO: unit-test
    def __init__(self, name, comment=None):
        super().__init__(name, comment=comment)
        self._retries = {}

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
            _assert_str_exc(exc)
            exc = "ALL" if exc == "*" else exc
        elif not isinstance(exc, Exception):
            raise TypeError("Error must be exception or accepted string")

        if exc in self._retries:
            raise ValueError("Error '%s' already registered" % exc)

        self._retries[exc] = {
            "interval": interval,
            "max_attempts": max_attempts,
            "backoff_rate": backoff_rate}

    @staticmethod
    def _retries_equal(defn, retry):
        ic = math.isclose
        return all((
            _compare_defn("IntervalSeconds", defn, retry["interval"]),
            _compare_defn("MaxAttempts", defn, retry["max_attempts"]),
            _compare_defn("BackoffRate", defn, retry["backoff_rate"], ic)))

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

    def to_dict(self):
        defn = super().to_dict()
        defn["Retry"] = self._retries_defn()
        return defn


class _CanCatch(State):  # TODO: unit-test
    def __init__(self, name, comment=None):
        super().__init__(name, comment=comment)
        self._catches = {}

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
            _assert_str_exc(exc)
            exc = "ALL" if exc == "*" else exc

        if exc in self._catches:
            raise ValueError("Error '%s' already registered" % exc)

        self._catches[exc] = next_state

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
                    "ResultPath": "$.%s.error-info" % self.name,
                    "Next": state.name}

                # Defer adding wildcard error until end (AWS SFN spec)
                if exc == "States.ALL":
                    all_defn = defn
                    continue

                defns.append(defn)

        if all_defn is not None:  # append ALL retry at the end
            defns.append(all_defn)

        return defns

    def to_dict(self):
        defn = super().to_dict()
        defn["Catch"] = self._catches_defn()
        return defn


class Succeed(State):  # TODO: unit-test
    """End execution successfully.

    Args:
        name (str): name of state
        comment (str): state description
    """

    def __init__(self, name, comment=None):
        super().__init__(name, comment=comment)


class Fail(State):  # TODO: unit-test
    """End execution unsuccessfully.

    Args:
        name (str): name of state
        comment (str): state description
        cause (str): failure description
        error (str): name of failure error
    """

    def __init__(self, name, comment=None, cause=None, error=None):
        super().__init__(name, comment=comment)
        self.cause = cause
        self.error = error

    def to_dict(self):
        defn = super().to_dict()
        defn["Cause"] = self.cause
        defn["Error"] = self.error
        return defn


class Pass(_HasNext, State):  # TODO: unit-test
    """No-op state, possibly introducing data.

    The name specifies the location of any introduced data.

    Args:
        name (str): name of state
        comment (str): state description
        result: return value of state, stored in the variable ``name``
    """

    def __init__(self, name, comment=None, result=None):
        super().__init__(name, comment=comment)
        self.result = result

    def to_dict(self):
        defn = super().to_dict()
        if self.result is not None:
            defn["ResultPath"] = "$.%s" % self.name
            defn["Result"] = self.result
        return defn


class Wait(_HasNext, State):  # TODO: unit-test
    """Wait for a time before continuing.

    Args:
        name (str): name of state
        until (int or datetime.datetime or str): time to wait. If ``int``,
            then seconds to wait; if ``datetime.datetime``, then time to
            wait until; if ``str``, then name of variable containing
            seconds to wait for
        comment (str): state description
    """

    def __init__(self, name, until, comment=None):
        super().__init__(name, comment=comment)
        self.until = until

    def to_dict(self):
        defn = super().to_dict()
        t = self.until
        if isinstance(t, int):
            defn["Seconds"] = t
        elif isinstance(t, datetime.datetime):
            if t.tzinfo is None or t.tzinfo.utcoffset(t) is None:
                raise ValueError("Wait time must be aware")
            defn["Timestamp"] = t.isoformat("T")
        elif isinstance(t, str):
            defn["SecondsPath"] = "$.%s" % t
        else:
            _s = "Invalid type for wait time: %s"
            raise TypeError(_s % type(t).__name__)
        return defn


class Parallel(_HasNext, _CanRetry, _CanCatch, State):  # TODO: unit-test
    """Run states-machines in parallel.

    Args:
        name (str): name of state
        state_machines (list[StateMachine]): state-machines to run in
            parallel. These state-machines do not need to be registered
            with AWS Step Functions.
        comment (str): state description
    """

    def __init__(self, name, state_machines, comment=None):
        super().__init__(name, comment=comment)
        self.state_machines = state_machines

    def to_dict(self):
        defn = super().to_dict()
        defn["Branches"] = [sm.to_dict() for sm in self.state_machines]
        defn["ResultPath"] = "$.%s" % self.name
        return defn


class Choice(State):  # TODO: unit-test
    """Branch execution based on comparisons.

    Args:
        name (str): name of state
        choices (list[_ChoiceRule]): choice rules determining branch
            conditions
        comment (str): state description
        default (State): fall-back state if all comparisons fail
    """

    def __init__(
            self,
            name,
            choices,
            comment=None,
            default=None):
        super().__init__(name, comment=comment)
        self.choices = choices
        self.default = default

    def to_dict(self):
        defn = super().to_dict()
        defn["Choices"] = [cr.to_dict() for cr in self.choices]
        if self.default is not None:
            defn["Default"] = self.default.name
        return defn

    def add_to(self, states):
        super().add_to(states)
        for rule in self.choices:
            rule.next_state.add_to(states)


class Task(_HasNext, _CanRetry, _CanCatch, State):  # TODO: unit-test
    """Activity execution.

    Args:
        name (str): name of state
        fn (callable): function to run activity
        comment (str): state description
        timeout (int): seconds before task time-out
        heartbeat (int): second between task heartbeats

    Attributes:
        session (_util.Session): AWS session to use for communication,
            must be set before using task
    """

    def __init__(
            self,
            name,
            fn,
            comment=None,
            timeout=None,
            heartbeat=60):
        super().__init__(name, comment=comment)
        self.fn = fn
        self.timeout = timeout
        self.heartbeat = heartbeat
        self.session = None

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    @_util.cached_property
    def arn(self) -> str:
        """Task resource identifier."""
        if self.session is None:
            raise RuntimeError("Attach a session before using task")
        region = self.session.region
        account_id = self.session.account_id
        _s = "arn:aws:states:%s:%s:activity:%s"
        return _s % (region, account_id, self.name)

    def to_dict(self):
        defn = super().to_dict()
        defn["Resource"] = self.arn
        defn["ResultPath"] = "$.%s" % self.name
        if self.timeout is not None:
            defn["TimeoutSeconds"] = self.timeout
        defn["HeartbeatSeconds"] = self.heartbeat
        return defn


def task(name, comment=None, timeout=None, heartbeat=60):  # TODO: unit-test
    """Task function decorator.

    Args:
        name (str): name of task
        comment (str): task description
        timeout (int): seconds before task time-out
        heartbeat (int): second between task heartbeats

    Example:
        >>> @task("myTask")
        >>> def fn():
        ...     print("hi")
    """

    def wrapper(fn):
        task_ = Task(
            name,
            fn,
            comment=comment,
            timeout=timeout,
            heartbeat=heartbeat)
        ft.update_wrapper(task_, fn)
        return task_
    return wrapper
