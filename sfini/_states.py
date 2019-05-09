# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""SFN states."""

import math
import datetime
import typing as T
import logging as lg

from . import _util
from . import _state_error

_logger = lg.getLogger(__name__)
_default = _util.DefaultParameter()
Exc = _state_error.ExceptionCondition.Exc
Rule = _state_error.ExceptionCondition.Rule


class State:  # TODO: unit-test
    """Abstract state.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        state_machine (sfini.StateMachine): state-machine this state is a
            part of
    """

    def __init__(
            self,
            name: str,
            comment: str = _default,
            input_path: T.Union[str, None] = _default,
            output_path: T.Union[str, None] = _default,
            *,
            state_machine):
        self.name = name
        self.comment = comment
        self.input_path = input_path
        self.output_path = output_path
        self.state_machine = state_machine

    def __str__(self):
        _sm_n = self.state_machine.name
        return "%s '%s' [%s]" % (type(self).__name__, self.name, _sm_n)

    def __repr__(self):
        return "%s(%s%s%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            "" if self.comment == _default else ", %r" % self.comment,
            "" if self.input_path == _default else ", %r" % self.input_path,
            "" if self.output_path == _default else ", %r" % self.output_path,
            ", state_machine=%r" % self.state_machine)

    def _validate_state(self, state: "State"):
        """Ensure state is of the same state-machine.

        Args:
            state: state to validate

        Raises:
            ValueError: if state is not of the same state-machine
        """

        if state.state_machine is not self.state_machine:
            _s = "State '%s' is not part of this state-machine"
            raise ValueError(_s % state)

    def to_dict(self) -> T.Dict[str, _util.JSONable]:
        """Convert this state to a definition dictionary.

        Returns:
            definition
        """

        defn = {"Type": type(self).__name__}
        if self.comment != _default:
            defn["Comment"] = self.comment
        if self.input_path != _default:
            defn["InputPath"] = self.input_path
        if self.output_path != _default:
            defn["OutputPath"] = self.output_path
        return defn


class _HasNext(State):  # TODO: unit-test
    """Activity execution.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        state_machine: state-machine this state is a part of

    Attributes:
        next: next state to execute, or ``None`` if state is terminal
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default,
            *,
            state_machine):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            state_machine=state_machine)
        self.next: T.Union[State, None] = None

    def goes_to(self, state: State):
        """Set next state after this state finishes.

        Args:
            state: state to execute next
        """

        self._validate_state(state)
        if self.next is not None:
            _logger.warning("Overriding current next state: %s" % self.next)
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


class _HasResultPath(State):  # TODO: unit-test
    """Activity execution.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        result_path: task output location JSONPath, ``None`` for discarded
            output
        state_machine: state-machine this state is a part of
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default,
            result_path: T.Union[str, None] = _default,
            *,
            state_machine):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            state_machine=state_machine)
        self.result_path = result_path

    def to_dict(self):
        defn = super().to_dict()
        if self.result_path != _default:
            defn["ResultPath"] = self.result_path
        return defn


class _CanRetry(_state_error.ExceptionCondition, State):  # TODO: unit-test
    """Activity execution.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        state_machine: state-machine this state is a part of

    Attributes:
        retries: retry conditions
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default,
            *,
            state_machine):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            state_machine=state_machine)
        self.retries: T.Dict[Exc, Rule] = {}

    def retry_for(
            self,
            exc: Exc,
            interval: int = _default,
            max_attempts: int = _default,
            backoff_rate: float = _default):
        """Add a retry condition.

        Args:
            exc: error for retry to be executed. If a string, must be one of
                the pre-defined errors (see AWS Step Functions documentation)
            interval: (initial) retry interval (seconds)
            max_attempts: maximum number of attempts before re-raising error
            backoff_rate: retry interval increase factor between attempts
        """

        exc = self._process_exc(exc)

        if exc in self.retries:
            raise ValueError("Error '%s' already registered" % exc)

        retry = {}
        if interval != _default:
            retry["interval"] = interval
        if max_attempts != _default:
            retry["max_attempts"] = max_attempts
        if backoff_rate != _default:
            retry["backoff_rate"] = backoff_rate
        self.retries[exc] = retry

    @staticmethod
    def _rules_similar(rule_a, rule_b):
        if rule_a.keys() != rule_b.keys():
            return False
        for k in ("interval", "max_attempts"):
            if k in rule_a and rule_a[k] != rule_b[k]:
                return False
        k = "backoff_rate"
        if k in rule_a and not math.isclose(rule_a[k], rule_b[k]):
            return False
        return True

    @staticmethod
    def _rule_defn(rule):
        defn = {}
        if "interval" in rule:
            defn["IntervalSeconds"] = rule["interval"]
        if "max_attempts" in rule:
            defn["MaxAttempts"] = rule["max_attempts"]
        if "backoff_rate" in rule:
            defn["BackoffRate"] = rule["backoff_rate"]
        return defn

    def to_dict(self):
        defn = super().to_dict()
        retry = self._rule_defns(self.retries)
        if retry:
            defn["Retry"] = retry
        return defn


class _CanCatch(_state_error.ExceptionCondition, State):  # TODO: unit-test
    """Activity execution.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        state_machine: state-machine this state is a part of

    Attributes:
        catches: handled state errors
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default,
            *,
            state_machine):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            state_machine=state_machine)
        self.catches: T.Dict[Exc, Rule] = {}

    def catch(
            self,
            exc: Exc,
            next_state: State,
            result_path: T.Union[str, None] = _default):
        """Add a catch clause.

        Args:
            exc: error for catch clause to be executed. If a string, must be
                one of the pre-defined errors (see AWS Step Functions
                documentation)
            next_state: state to execute for catch clause
            result_path: error details location JSONPath
        """

        self._validate_state(next_state)

        exc = self._process_exc(exc)
        if exc in self.catches:
            raise ValueError("Error '%s' already registered" % exc)
        self.catches[exc] = (next_state, result_path)

    @staticmethod
    def _rules_similar(rule_a, rule_b):
        same_state = rule_a[0].name == rule_b[0].name
        same_result_path = rule_a[1] == rule_b[1]
        return same_state and same_result_path

    @staticmethod
    def _rule_defn(rule):
        defn = {"Next": rule[0].name}
        if rule[1] != _default:
            defn["ResultPath"] = rule[1]
        return defn

    def to_dict(self):
        defn = super().to_dict()
        catch = self._rule_defns(self.catches)
        if catch:
            defn["Catch"] = catch
        return defn


class Succeed(State):  # TODO: unit-test
    """End execution successfully.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        state_machine: state-machine this state is a part of
    """

    pass


class Fail(State):  # TODO: unit-test
    """End execution unsuccessfully.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        cause: failure description
        error: name of failure error
        state_machine: state-machine this state is a part of
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default,
            cause: str = _default,
            error: str = _default,
            *,
            state_machine):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            state_machine=state_machine)
        self.cause = cause
        self.error = error

    def to_dict(self):
        defn = super().to_dict()
        if self.cause != _default:
            defn["Cause"] = self.cause
        if self.error != _default:
            defn["Error"] = self.error
        return defn

    def __repr__(self):
        return "%s(%s%s%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            "" if self.comment == _default else ", %r" % self.comment,
            "" if self.cause == _default else ", %r" % self.cause,
            "" if self.error == _default else ", %r" % self.error,
            ", %r" % self.state_machine)


class Pass(_HasResultPath, _HasNext, State):  # TODO: unit-test
    """No-op state, possibly introducing data.

    The name specifies the location of any introduced data.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        result_path: task output location JSONPath, ``None`` for discarded
            output
        result: return value of state, stored in the variable ``name``
        state_machine: state-machine this state is a part of

    Attributes:
        next: next state to execute
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default,
            result_path=_default,
            result: _util.JSONable = _default,
            *,
            state_machine):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            result_path=result_path,
            state_machine=state_machine)
        self.result = result

    def __repr__(self):
        return "%s(%s%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            "" if self.comment == _default else ", %r" % self.comment,
            "" if self.result == _default else ", %r" % self.result,
            ", %r" % self.state_machine)

    def to_dict(self):
        defn = super().to_dict()
        if self.result != _default:
            defn["Result"] = self.result
        return defn


class Wait(_HasNext, State):  # TODO: unit-test
    """Wait for a time before continuing.

    Args:
        name: name of state
        until: time to wait. If ``int``, then seconds to wait; if
            ``datetime.datetime``, then time to wait until; if ``str``,
            then name of state-variable containing time to wait until
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        state_machine: state-machine this state is a part of

    Attributes:
        next: next state to execute
    """

    def __init__(
            self,
            name,
            until: T.Union[int, datetime.datetime, str],
            comment=_default,
            input_path=_default,
            output_path=_default,
            *,
            state_machine):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            state_machine=state_machine)
        self.until = until

    def __repr__(self):
        return "%s(%s%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            repr(self.until),
            "" if self.comment == _default else ", %r" % self.comment,
            ", %r" % self.state_machine)

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
            defn["TimestampPath"] = t
        else:
            _s = "Invalid type for wait time: %s"
            raise TypeError(_s % type(t).__name__)
        return defn


# TODO: unit-test
class Parallel(_HasResultPath, _HasNext, _CanRetry, _CanCatch, State):
    """Run states-machines in parallel.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        result_path: task output location JSONPath, ``None`` for discarded
            output
        state_machine: state-machine this state is a part of

    Attributes:
        state_machines (list[sfini.StateMachine]): state-machines to run in
            parallel. These state-machines do not need to be registered
            with AWS Step Functions.
        next: next state to execute
        retries: retry conditions
        catches: handled state errors
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default,
            result_path=_default,
            *,
            state_machine):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            result_path=result_path,
            state_machine=state_machine)
        self.state_machines = []

    def to_dict(self):
        defn = super().to_dict()
        defn["Branches"] = [sm.to_dict() for sm in self.state_machines]
        return defn

    def add(self, state_machine):
        """Add a state-machine to be executed.

        The input to the state-machine execution is the input into this
        parallel state. The output of the parallel state is a list of each
        state-machine's output (in order of adding).

        Args:
            state_machine (sfini.StateMachine): state-machine to add. It will
                be run when this task is executed. Added state-machines do not
                need to be registered with AWS Step Functions
        """

        self.state_machines.append(state_machine)


class Choice(State):  # TODO: unit-test
    """Branch execution based on comparisons.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        state_machine: state-machine this state is a part of

    Attributes:
        choices (list[sfini._choice_ops._ChoiceOp]): choice rules
            determining branch conditions
        default (State): fall-back state if all comparisons fail, or
            ``None`` for no fall-back (Step Functions will raise a
            'States.NoChoiceMatched' error)
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default,
            *,
            state_machine):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            state_machine=state_machine)
        self.choices = []
        self.default: T.Union[State, None] = None

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
            rule (sfini._choice_ops._ChoiceOp): branch execution condition
                and specification to add

        Raises:
            RuntimeError: rule will go to a state not part of this
                state-machine
        """

        self._validate_state(rule.next_state)
        self.choices.append(rule)

    def remove(self, rule):
        """Remove a branch.

        Args:
            rule (sfini._choice_ops._ChoiceOp): branch execution condition
                and specification to remove

        Raises:
            ValueError: if rule is not a registered branch
        """

        if rule not in self.choices:
            raise ValueError("Rule '%s' is not registered with this state")
        self.choices.remove(rule)

    def set_default(self, state: State):
        """Set the default state to execute when no conditions were met.

        Args:
            state: default state to execute
        """

        self._validate_state(state)
        if self.default is not None:
            _s = "Overwriting current default state '%s'"
            _logger.warning(_s % self.default)
        self.default = state


# TODO: unit-test
class Task(_HasResultPath, _HasNext, _CanRetry, _CanCatch, State):
    """Activity execution.

    Args:
        name: name of state
        resource (sfini._task_resource.TaskResource): task executor, eg
            activity or Lambda function
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        result_path: task output location JSONPath, ``None`` for discarded
            output
        timeout: seconds before task time-out
        state_machine: state-machine this state is a part of

    Attributes:
        next: next state to execute
        retries: retry conditions
        catches: handled state errors
    """

    _heartbeat_extra = 5

    def __init__(
            self,
            name,
            resource,
            comment=_default,
            input_path=_default,
            output_path=_default,
            result_path=_default,
            timeout: int = _default,
            *,
            state_machine):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            result_path=result_path,
            state_machine=state_machine)
        self.resource = resource
        self.timeout = timeout

    def __repr__(self):
        to_cm_str = ", timeout=%r" if self.comment is None else ", %r"
        _to = "" if self.timeout == _default else to_cm_str % self.timeout
        return "%s(%s%s%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            repr(self.resource),
            "" if self.comment is None else ", %r" % self.comment,
            _to,
            ", state_machine=%r" % self.state_machine)

    def to_dict(self):
        defn = super().to_dict()
        defn["Resource"] = self.resource.arn
        if self.timeout != _default:
            defn["TimeoutSeconds"] = self.timeout
        if hasattr(self.resource, "heartbeat"):
            _h = self._heartbeat_extra
            defn["HeartbeatSeconds"] = self.resource.heartbeat + _h
        return defn
