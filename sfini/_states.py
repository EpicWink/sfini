# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN states."""

import math
import datetime
import logging as lg

from . import _util

_logger = lg.getLogger(__name__)


def _assert_str_exc(exc):  # TODO: unit-test
    """Ensure exception type-name is valid.

    Args:
        exc (str): exception type-name

    Raises:
        ValueError: bad type-name
    """

    errs = ("*", "ALL", "Timeout", "TaskFailed", "Permissions")
    if exc not in errs:
        _s = "Error name was '%s', must be one of: %s"
        raise ValueError(_s % (exc, errs))


def _compare_defn(defn_key, defn, compare_val, compare_key=None):  # TODO: unit-test
    """Compare definition value, checking for existance.

    Args:
        defn_key (str): key of definition to compare
        defn (dict): definition of object
        compare_val: comparison value
        compare_key (callable): returns if comparison is true, must have
            signature ``compares = compare_key(a, b)``, default: ``==``

    Returns:
        bool: whether value compares
    """

    compare_key = compare_key or (lambda a, b: a == b)
    if defn_key in defn:
        return compare_key(defn[defn_key], compare_val)
    else:
        return compare_val is None


class State:  # TODO: unit-test
    """Abstract state.

    Args:
        name (str): name of state
        comment (str): state description
        state_machine (StateMachine): state-machine this state is a part of
    """

    def __init__(self, name, comment=None, *, state_machine):
        self.name = name
        self.comment = comment
        self.state_machine = state_machine

    def __str__(self):
        return "%s '%s' [%s]" % (
            type(self).__name__,
            self.name,
            self.state_machine.name)

    def __repr__(self):
        return "%s(%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            "" if self.comment is None else (", " + repr(self.comment)),
            ", state_machine=" + repr(self.state_machine))

    def to_dict(self):
        """Convert this state to a definition dictionary.

        Returns:
            dict: definition
        """

        defn = {"Type": type(self).__name__}
        if self.comment is not None:
            defn["Comment"] = self.comment
        return defn


class _HasNext(State):  # TODO: unit-test
    def __init__(self, name, comment=None, *, state_machine):
        super().__init__(name, comment=comment, state_machine=state_machine)
        self.next = None

    def goes_to(self, state):
        """Set next state after this state finishes.

        Args:
            state (State): state to execute next
        """

        if state not in self.state_machine.states.values():
            _s = "State '%s' is not part of this state-machine"
            raise ValueError(_s % state)

        self.next = state

    def remove_next(self):
        """Remove next state, making this state terminal."""
        self.next = None

    def to_dict(self):
        defn = super().to_dict()
        if self.next is None:
            defn["End"] = True
        else:
            defn["Next"] = self.next.name
        return defn


class _CanRetry(State):  # TODO: unit-test
    def __init__(self, name, comment=None, *, state_machine):
        super().__init__(name, comment=comment, state_machine=state_machine)
        self.retries = {}

    def retry_for(
            self,
            exc,
            interval=None,
            max_attempts=None,
            backoff_rate=None):
        """Add a retry condition.

        Args:
            exc (type or str): error for retry to be executed. If a string,
                must be one of '*', 'ALL', 'Timeout', 'TaskFailed', or
                'Permissions' (see AWS Step Functions documentation)
            interval (int): (initial) retry interval (seconds)
            max_attempts (int): maximum number of attempts before re-raising
                error
            backoff_rate (float): retry interval increase factor between
                attempts
        """

        if isinstance(exc, str):
            _assert_str_exc(exc)
            exc = "ALL" if exc == "*" else exc
        elif not issubclass(exc, Exception):
            raise TypeError("Error must be exception or accepted string")

        if exc in self.retries:
            raise ValueError("Error '%s' already registered" % exc)

        self.retries[exc] = {
            "interval": interval,
            "max_attempts": max_attempts,
            "backoff_rate": backoff_rate}

    @staticmethod
    def _retries_equal(defn, retry):
        """Check if retry definitions are equal.

        Args:
            defn (dict): registered retry definition
            retry (dict): new retry definition

        Returns:
            bool: definitions are equal
        """

        ic = math.isclose
        return all((
            _compare_defn("IntervalSeconds", defn, retry["interval"]),
            _compare_defn("MaxAttempts", defn, retry["max_attempts"]),
            _compare_defn("BackoffRate", defn, retry["backoff_rate"], ic)))

    def _retries_defn(self):
        """Get definition of retry rules.

        Returns:
            list[dict]: retries' definitions
        """

        defns = []
        all_defn = None
        for exc, retry in self.retries.items():
            # Convert error to string
            if isinstance(exc, str):
                exc = "States." + exc
            elif issubclass(exc, Exception):
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
    def __init__(self, name, comment=None, *, state_machine):
        super().__init__(name, comment=comment, state_machine=state_machine)
        self.catches = {}

    def catch(self, exc, next_state):
        """Add a catch clause.

        Args:
            exc (type or str): error for catch clause to be executed. If a
                string, must be one of '*', 'ALL', 'Timeout', 'TaskFailed', or
                'Permissions' (see AWS Step Functions documentation)
            next_state (State): state to execute for catch clause
        """

        if next_state not in self.state_machine.states.values():
            _s = "State '%s' is not part of this state-machine"
            raise ValueError(_s % next_state)

        if isinstance(exc, str):
            _assert_str_exc(exc)
            exc = "ALL" if exc == "*" else exc
        elif not issubclass(exc, Exception):
            raise TypeError("Error must be exception or accepted string")

        if exc in self.catches:
            raise ValueError("Error '%s' already registered" % exc)

        self.catches[exc] = next_state

    def _catches_defn(self):
        """Get definition of catch rules.

        Returns:
            list[dict]: catches' definitions
        """

        defns = []
        all_defn = None
        for exc, state in self.catches.items():
            # Convert error to string
            if isinstance(exc, str):
                exc = "States." + exc
            elif issubclass(exc, Exception):
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

    def __init__(self, name, comment=None, *, state_machine):
        super().__init__(name, comment=comment, state_machine=state_machine)


class Fail(State):  # TODO: unit-test
    """End execution unsuccessfully.

    Args:
        name (str): name of state
        comment (str): state description
        cause (str): failure description
        error (str): name of failure error
    """

    def __init__(
            self,
            name,
            comment=None,
            cause=None,
            error=None,
            *,
            state_machine):
        super().__init__(name, comment=comment, state_machine=state_machine)
        self.cause = cause
        self.error = error

    def to_dict(self):
        defn = super().to_dict()
        defn["Cause"] = self.cause
        defn["Error"] = self.error
        return defn

    def __repr__(self):
        return "%s(%s%s%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            "" if self.comment is None else (", " + repr(self.comment)),
            "" if self.cause is None else (", " + repr(self.cause)),
            "" if self.error is None else (", " + repr(self.error)),
            ", " + repr(self.state_machine))


class Pass(_HasNext, State):  # TODO: unit-test
    """No-op state, possibly introducing data.

    The name specifies the location of any introduced data.

    Args:
        name (str): name of state
        comment (str): state description
        result: return value of state, stored in the variable ``name``

    Attributes:
        next (State): next state to execute
    """

    def __init__(self, name, comment=None, result=None, *, state_machine):
        super().__init__(name, comment=comment, state_machine=state_machine)
        self.result = result

    def __repr__(self):
        return "%s(%s%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            "" if self.comment is None else (", " + repr(self.comment)),
            "" if self.result is None else (", " + repr(self.result)),
            ", " + repr(self.state_machine))

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
        until (int or datetime.datetime or str): time to wait. If ``int``, then
            seconds to wait; if ``datetime.datetime``, then time to wait until;
            if ``str``, then name of variable containing seconds to wait for
        comment (str): state description

    Attributes:
        next (State): next state to execute
    """

    def __init__(self, name, until, comment=None, *, state_machine):
        super().__init__(name, comment=comment, state_machine=state_machine)
        self.until = until

    def __repr__(self):
        return "%s(%s%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            repr(self.until),
            "" if self.comment is None else (", " + repr(self.comment)),
            ", " + repr(self.state_machine))

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
        comment (str): state description

    Attributes:
        state_machines (list[StateMachine]): state-machines to run in parallel.
            These state-machines do not need to be registered with AWS Step
            Functions.
        next (State): next state to execute
        retries (dict[Exception or str]): retry conditions
        catches (dict[Exception or str]): handled state errors
    """

    def __init__(self, name, comment=None, *, state_machine):
        super().__init__(name, comment=comment, state_machine=state_machine)
        self.state_machines = []

    def to_dict(self):
        defn = super().to_dict()
        defn["Branches"] = [sm.to_dict() for sm in self.state_machines]
        defn["ResultPath"] = "$.%s" % self.name
        return defn

    def add(self, state_machine):
        """Add a state-machine to be executed.

        The input to the state-machine execution is the input into this
        parallel state. The output of the parallel state is a list of each
        state-machine's output (in order of adding).

        Args:
            state_machine (sfini.StateMachine): state-machine to add. It will
                be run when this task is executed. State-machines do not need
                to be registered with AWS Step Functions
        """

        self.state_machines.append(state_machine)


class Choice(State):  # TODO: unit-test
    """Branch execution based on comparisons.

    Args:
        name (str): name of state
        comment (str): state description

    Attributes:
        choices (list[_ChoiceRule]): choice rules determining branch conditions
        default (State): fall-back state if all comparisons fail
    """

    def __init__(self, name, comment=None, *, state_machine):
        super().__init__(name, comment=comment, state_machine=state_machine)
        self.choices = []
        self.default = None

    def to_dict(self):
        if not self.choices and self.default is None:
            raise RuntimeError("Choice '%s' has no next path")
        defn = super().to_dict()
        defn["Choices"] = [cr.to_dict() for cr in self.choices]
        if self.default is not None:
            defn["Default"] = self.default.name
        return defn

    def add(self, rule):
        """Add a branch.

        Args:
            rule (_ChoiceOp): branch execution condition and specification to
                add

        Raises:
            RuntimeError: rule will go to a state not part of this
                state-machine
        """

        if rule.next_state.state_machine is not self.state_machine:
            _s = "Rule '%s' has next-state which is not part of this "
            _s += "state-machine"
            raise RuntimeError(_s % rule)
        self.choices.append(rule)

    def remove(self, rule):
        """Remove a branch.

        Args:
            rule (_ChoiceOp): branch execution condition and specification to
                remove

        Raises:
            ValueError: if rule is not a registered branch
        """

        if rule not in self.choices:
            raise ValueError("Rule '%s' is not registered with this state")
        self.choices.remove(rule)

    def set_default(self, state):
        """Set the default state to execute when no conditions were met.

        Args:
            state (State): default state to execute
        """

        if self.default is not None:
            _s = "Overwriting current default state '%s'"
            _logger.warning(_s % self.default)
        self.default = state


class Task(_HasNext, _CanRetry, _CanCatch, State):  # TODO: unit-test
    """Activity execution.

    Args:
        name (str): name of state
        activity (Activity): activity to execute
        comment (str): state description
        timeout (int): seconds before task time-out
        heartbeat (int): second between task heartbeats

    Attributes:
        next (State): next state to execute
        retries (dict[Exception or str]): retry conditions
        catches (dict[Exception or str]): handled state errors
    """

    def __init__(
            self,
            name,
            activity,
            comment=None,
            timeout=None,
            heartbeat=60,
            *,
            state_machine):
        super().__init__(name, comment=comment, state_machine=state_machine)
        self.activity = activity
        self.timeout = timeout
        self.heartbeat = heartbeat

    def __repr__(self):
        return "%s(%s%s%s%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            repr(self.activity),
            "" if self.comment is None else (", " + repr(self.comment)),
            "" if self.timeout is None else (", " + repr(self.timeout)),
            ", " + repr(self.heartbeat),
            ", " + repr(self.state_machine))

    @_util.cached_property
    def arn(self) -> str:
        """Task resource identifier."""
        region = self.state_machine.session.region
        account_id = self.state_machine.session.account_id
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
