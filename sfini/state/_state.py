# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2019/05/13

"""State definitions."""

import datetime
import typing as T
import logging as lg

from . import _base
from .. import _util

_logger = lg.getLogger(__name__)
_default = _util.DefaultParameter()


class Succeed(_base.State):  # TODO: unit-test
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


class Fail(_base.State):  # TODO: unit-test
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


# TODO: unit-test
class Pass(_base.HasResultPath, _base.HasNext, _base.State):
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


class Wait(_base.HasNext, _base.State):  # TODO: unit-test
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
class Parallel(
        _base.HasResultPath,
        _base.HasNext,
        _base.CanRetry,
        _base.CanCatch,
        _base.State):
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


class Choice(_base.State):  # TODO: unit-test
    """Branch execution based on comparisons.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        state_machine: state-machine this state is a part of

    Attributes:
        choices (list[sfini.choice.ChoiceRule]): choice rules
            determining branch conditions
        default: fall-back state if all comparisons fail, or ``None`` for
            no fall-back (Step Functions will raise a 'States.NoChoiceMatched'
            error)
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
        self.default: T.Union[_base.State, None] = None

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
            rule (sfini.choice.ChoiceRule): branch execution condition
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
            rule (sfini.choice.ChoiceRule): branch execution condition
                and specification to remove

        Raises:
            ValueError: if rule is not a registered branch
        """

        if rule not in self.choices:
            raise ValueError("Rule '%s' is not registered with this state")
        self.choices.remove(rule)

    def set_default(self, state: _base.State):
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
class Task(
        _base.HasResultPath,
        _base.HasNext,
        _base.CanRetry,
        _base.CanCatch,
        _base.State):
    """Activity execution.

    Args:
        name: name of state
        resource (sfini.task_resource.TaskResource): task executor, eg
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
