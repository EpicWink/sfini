# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN states."""

import logging as lg
import functools as ft

from . import _state

_logger = lg.getLogger(__name__)


class _Terminal(_state.State):  # TODO: unit-test
    def __init__(self, name, comment=None):
        super().__init__(name, comment=comment, end=True)

    def catch(self, exc, next_state):
        raise RuntimeError("Terminal state cannot have catch clause")

    def goes_to(self, state):
        raise RuntimeError("Cannot define next state for terminal state")


class Succeed(_Terminal):  # TODO: unit-test
    """End execution successfully.

    Args:
        name (str): name of state
        comment (str): state description
    """

    def __init__(self, name, comment=None):
        super().__init__(name, comment=comment)


class Fail(_Terminal):  # TODO: unit-test
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


class Pass(_state.State):  # TODO: unit-test
    """No-op state.

    Args:
        name (str): name of state
        comment (str): state description
        end (bool): state stops execution
        result: return value of state
    """

    def __init__(self, name, comment=None, end=False, result=None):
        super().__init__(name, comment=comment, end=end)
        self.result = result

    def catch(self, exc, next_state):
        raise RuntimeError("Pass state cannot have catch clause")


class Wait(_state.State):  # TODO: unit-test
    """Wait for a time before continuing.

    Args:
        name (str): name of state
        comment (str): state description
        end (bool): state stops execution
        until (int or datetime.datetime or str): time to wait. If `int`,
            then seconds to wait; if `datetime.datetime`, then time to wait
            until; if `str`, then name of variable containing seconds to
            wait for
    """

    def __init__(self, name, comment=None, end=False, until=None):
        super().__init__(name, comment=comment, end=end)
        self.until = until

    def catch(self, exc, next_state):
        raise RuntimeError("Wait state cannot have catch clause")


class Parallel(_state.State):  # TODO: unit-test
    """Run states-machines in parallel.

    Args:
        name (str): name of state
        state_machines (list[StateMachine] or tuple[StateMachine]:
            state-machines to run in parallel. These state-machines do not
            need to be registered with AWS Step Functions.
        comment (str): state description
        end (bool): state stops execution
    """

    def __init__(self, name, state_machines, comment=None, end=False):
        super().__init__(name, comment=comment, end=end)
        self.state_machines = state_machines

    def to_dict(self):
        branches = [sm.to_dict() for sm in self.state_machines]
        raise NotImplementedError


class Choice(_state.State):  # TODO: unit-test
    """Branch execution based on comparisons.

    Args:
        name (str): name of state
        choices (list[sfini._choice_ops._ChoiceRule]): choice rules
            determining branch conditions
        comment (str): state description
        default (_state.State): fall-back state if all comparisons fail
    """

    def __init__(
            self,
            name,
            choices,
            comment=None,
            default=None):
        super().__init__(name, comment=comment, end=False)
        self.choices = choices
        self.default = default

    def goes_to(self, state):
        raise RuntimeError("Cannot define next state for choice state")

    def catch(self, exc, next_state):
        raise RuntimeError("Choice state cannot have catch clause")


class Task(_state.State):  # TODO: unit-test
    """Activity execution.

    Args:
        name (str): name of state
        fn (callable): function to run activity
        comment (str): state description
        end (bool): state stops execution
        timeout (int): seconds before task time-out
        heartbeat (int): second between task heartbeats

    """

    def __init__(
            self,
            name,
            fn,
            comment=None,
            end=False,
            timeout=None,
            heartbeat=60):
        super().__init__(name, comment=comment, end=end)
        self.fn = fn
        self.timeout = timeout
        self.heartbeat = heartbeat

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)


def task(name, comment=None, end=False, timeout=None, heartbeat=60):  # TODO: unit-test
    """Task function decorator.

    Args:
        name (str): name of task
        comment (str): task description
        end (bool): task stops execution
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
            end=end,
            timeout=timeout,
            heartbeat=heartbeat)
        ft.update_wrapper(task_, fn)
        return task_
    return wrapper

