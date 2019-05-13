# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2019/05/13

"""State definition bases and mix-ins."""

import math
import typing as T
import logging as lg

from .. import _util
from . import error as sfini_state_error

_logger = lg.getLogger(__name__)
_default = _util.DefaultParameter()
Exc = sfini_state_error.ExceptionCondition.Exc
Rule = sfini_state_error.ExceptionCondition.Rule


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


class HasNext(State):  # TODO: unit-test
    """State able to advance mix-in.

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


class HasResultPath(State):  # TODO: unit-test
    """State with result mix-in.

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


# TODO: unit-test
class CanRetry(sfini_state_error.ExceptionCondition, State):
    """Retryable state mix-in.

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


# TODO: unit-test
class CanCatch(sfini_state_error.ExceptionCondition, State):
    """Exception catching state mix-in.

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
