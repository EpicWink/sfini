# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2019/05/13

"""State definition bases and mix-ins."""

import typing as T
import logging as lg

from .. import _util

_logger = lg.getLogger(__name__)
_default = _util.DefaultParameter()
STATES_ERRORS = (
    "ALL",
    "Timeout",
    "TaskFailed",
    "Permissions",
    "ResultPathMatchFailure",
    "ParameterPathFailure",
    "BranchFailed",
    "NoChoiceMatched")


class State:
    """Abstract state.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
    """

    def __init__(
            self,
            name: str,
            comment: str = _default,
            input_path: T.Union[str, None] = _default,
            output_path: T.Union[str, None] = _default):
        self.name = name
        self.comment = comment
        self.input_path = input_path
        self.output_path = output_path

    def __str__(self):
        name = type(self).__name__
        return "%s [%s]" % (self.name, name)

    __repr__ = _util.easy_repr

    def add_to(self, states):
        """Add this state to a state-machine definition.

        Any child states will also be added to the definition.

        Args:
            states (dict[str, State]): state-machine states
        """

        _logger.debug("Adding state to state-machine definition: '%s'" % self)
        if states.get(self.name, self) != self:
            raise ValueError("State name '%s' already registered" % self.name)
        states[self.name] = self

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


class HasNext(State):
    """State able to advance mix-in.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output

    Attributes:
        next: next state to execute, or ``None`` if state is terminal
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path)
        self.next: T.Union[State, None] = None

    def add_to(self, states):
        super().add_to(states)
        if self.next is not None and self.next.name not in states:
            self.next.add_to(states)

    def goes_to(self, state: State):
        """Set next state after this state finishes.

        Args:
            state: state to execute next
        """

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


class HasResultPath(State):
    """State with result mix-in.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output
        result_path: task output location JSONPath, ``None`` for discarded
            output
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default,
            result_path: T.Union[str, None] = _default):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path)
        self.result_path = result_path

    def to_dict(self):
        defn = super().to_dict()
        if self.result_path != _default:
            defn["ResultPath"] = self.result_path
        return defn


class CanRetry(State):
    """Retryable state mix-in.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output

    Attributes:
        retriers: error handler policies
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path)
        self.retriers: T.List[T.Tuple[T.Sequence[str], T.Dict[str, ...]]] = []

    def retry_for(
            self,
            errors: T.Sequence[str],
            interval: int = _default,
            max_attempts: int = _default,
            backoff_rate: float = _default):
        """Add a retry handler.

        Args:
            errors: codes of errors for retry to be executed. See AWS Step
                Functions documentation
            interval: (initial) retry interval (seconds)
            max_attempts: maximum number of attempts before re-raising error
            backoff_rate: retry interval increase factor between attempts
        """

        policy = {
            "interval": interval,
            "max_attempts": max_attempts,
            "backoff_rate": backoff_rate}
        self.retriers.append((errors, policy))

    @staticmethod
    def _retrier_defn(
            errors: T.Sequence[str],
            policy: T.Dict[str, T.Any]
    ) -> T.Dict[str, _util.JSONable]:
        """Build retry handler definition.

        Args:
            errors: codes of errors for retry handler to be invoked
            policy: retry handler policy

        Returns:
            definitions
        """

        _validate_errors(errors)
        defn = {"ErrorEquals": errors}
        if policy["interval"] != _default:
            defn["IntervalSeconds"] = policy["interval"]
        if policy["max_attempts"] != _default:
            defn["MaxAttempts"] = policy["max_attempts"]
        if policy["backoff_rate"] != _default:
            defn["BackoffRate"] = policy["backoff_rate"]
        return defn

    def _get_retrier_defns(self) -> T.List[T.Dict[str, _util.JSONable]]:
        """Build retry handler definitions.

        Returns:
            definitions
        """

        return [self._retrier_defn(e, p) for e, p in self.retriers]

    def to_dict(self):
        defn = super().to_dict()
        retry = self._get_retrier_defns()
        if retry:
            defn["Retry"] = retry
        return defn


class CanCatch(State):
    """Exception catching state mix-in.

    Args:
        name: name of state
        comment: state description
        input_path: state input filter JSONPath, ``None`` for empty input
        output_path: state output filter JSONPath, ``None`` for discarded
            output

    Attributes:
        catchers: error handler policies
    """

    def __init__(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default):
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path)
        self.catchers: T.List[T.Tuple[T.Sequence[str], T.Dict[str, ...]]] = []

    def add_to(self, states):
        super().add_to(states)
        for _, policy in self.catchers:
            if policy["next_state"].name not in states:
                policy["next_state"].add_to(states)

    def catch(
            self,
            errors: T.Sequence[str],
            next_state: State,
            result_path: T.Union[str, None] = _default):
        """Add an error handler.

        Args:
            errors: code of errors for catch clause to be executed. See AWS
                Step Functions documentation
            next_state: state to execute for catch clause
            result_path: error details location JSONPath
        """

        if any(any(e in excs_ for e in errors) for excs_, _ in self.catchers):
            fmt = "Handler has already accounted-for errors: %s"
            _logger.warning(fmt % errors)
        policy = {"next_state": next_state, "result_path": result_path}
        self.catchers.append((errors, policy))

    @staticmethod
    def _catcher_defn(
            errors: T.Sequence[str],
            policy: T.Dict[str, T.Any]
    ) -> T.Dict[str, _util.JSONable]:
        """Build error handler definition.

        Args:
            errors: codes of errors for retry handler to be invoked
            policy: retry handler policy

        Returns:
            definitions
        """

        _validate_errors(errors)
        defn = {"ErrorEquals": errors, "Next": policy["next_state"].name}
        if policy["result_path"] != _default:
            defn["ResultPath"] = policy["result_path"]
        return defn

    def _get_catcher_defns(self) -> T.List[T.Dict[str, _util.JSONable]]:
        """Build error handler definitions.

        Returns:
            definitions
        """

        return [self._catcher_defn(e, p) for e, p in self.catchers]

    def to_dict(self):
        defn = super().to_dict()
        catch = self._get_catcher_defns()
        if catch:
            defn["Catch"] = catch
        return defn


def _validate_errors(errors: T.Sequence[str]):
    """Validate error conditions.

    Args:
        errors: condition error codes

    Raises:
        ValueError: invalid condition
    """

    if not errors:
        raise ValueError("Cannot have no-error condition")
    if "States.ALL" in errors and len(errors) > 1:
        msg = "Cannot combine 'States.ALL' condition with other errors"
        raise ValueError(msg)

    for err in errors:
        if err.startswith("States."):
            if err[7:] not in STATES_ERRORS:
                fmt = "States error name was '%s', must be one of: %s"
                raise ValueError(fmt % (err[7:], STATES_ERRORS))
