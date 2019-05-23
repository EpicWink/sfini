# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2019/05/13

"""State definition bases and mix-ins."""

import typing as T
import logging as lg

from .. import _util

_logger = lg.getLogger(__name__)
_default = _util.DefaultParameter()


class State:  # TODO: unit-test
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

        Arguments:
            states (dict[str, State]): state-machine states
        """

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


class HasNext(State):  # TODO: unit-test
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
        if self.next is not None:
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


class ErrorHandling:  # TODO: unit-test
    """Error handling mix-in."""
    states_errors = (
        "ALL",
        "Timeout",
        "TaskFailed",
        "Permissions",
        "ResultPathMatchFailure",
        "ParameterPathFailure",
        "BranchFailed",
        "NoChoiceMatched")

    def _validate_errors(self, errors: T.Sequence[str]):
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
                if err[7:] not in self.states_errors:
                    fmt = "States error name was '%s', must be one of: %s"
                    raise ValueError(fmt % (err[7:], self.states_errors))

    @staticmethod
    def _policy_defn(policy: T.Any) -> T.Dict[str, _util.JSONable]:
        """Get handler definition details from policy.

        Args:
            policy: exception policy

        Returns:
            dict: definition details
        """

        raise NotImplementedError

    def _handler_defns(
            self,
            handlers: T.List[T.Tuple[T.Sequence[str], T.Any]]
    ) -> T.List[T.Dict[str, _util.JSONable]]:
        """Build error handler policy definitions.

        Args:
            handlers: error handlers

        Returns:
            definitions
        """

        defns = []
        for errors, policy in handlers:
            self._validate_errors(errors)
            defn = self._policy_defn(policy)
            defn["ErrorEquals"] = errors
            defns.append(defn)
        return defns


# TODO: unit-test
class CanRetry(ErrorHandling, State):
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
        self.retriers: T.List[T.Tuple[T.Sequence[str], T.Dict]] = []

    def retry_for(
            self,
            errors: T.Sequence[str],
            interval: int = _default,
            max_attempts: int = _default,
            backoff_rate: float = _default):
        """Add a retry condition.

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
    def _policy_defn(policy):
        defn = {}
        if policy["interval"] != _default:
            defn["IntervalSeconds"] = policy["interval"]
        if policy["max_attempts"] != _default:
            defn["MaxAttempts"] = policy["max_attempts"]
        if policy["backoff_rate"] != _default:
            defn["BackoffRate"] = policy["backoff_rate"]
        return defn

    def to_dict(self):
        defn = super().to_dict()
        retry = self._handler_defns(self.retriers)
        if retry:
            defn["Retry"] = retry
        return defn


# TODO: unit-test
class CanCatch(ErrorHandling, State):
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
        self.catchers: T.List[T.Tuple[T.Sequence[str], T.Any]] = []

    def add_to(self, states):
        super().add_to(states)
        for _, policy in self.catchers:
            policy["next_state"].add_to(states)

    def catch(
            self,
            excs: T.Sequence[str],
            next_state: State,
            result_path: T.Union[str, None] = _default):
        """Add a catch clause.

        Args:
            excs: errors for catch clause to be executed. If a string, must be
                one of the pre-defined errors (see AWS Step Functions
                documentation)
            next_state: state to execute for catch clause
            result_path: error details location JSONPath
        """

        if any(any(e in excs_ for e in excs) for excs_, _ in self.catchers):
            fmt = "Handler has already accounted-for exceptions: %s"
            _logger.warning(fmt % excs)
        policy = {"next_state": next_state, "result_path": result_path}
        self.catchers.append((excs, policy))

    def _policy_defn(self, policy):
        defn = {"Next": policy["next_state"].name}
        if policy["result_path"] != _default:
            defn["ResultPath"] = policy["result_path"]
        return defn

    def to_dict(self):
        defn = super().to_dict()
        catch = self._handler_defns(self.catchers)
        if catch:
            defn["Catch"] = catch
        return defn
