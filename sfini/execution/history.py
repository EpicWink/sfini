# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/08/09

"""State-machine execution history events.

Use ``sfini.execution.Execution.format_history`` for nice history
printing.
"""

import json
import typing as T
import logging as lg

from .. import _util

_logger = lg.getLogger(__name__)
_default = _util.DefaultParameter()
_type_keys = {
    "ActivityFailed": "activityFailedEventDetails",
    "ActivityScheduleFailed": "activityScheduleFailedEventDetails",
    "ActivityScheduled": "activityScheduledEventDetails",
    "ActivityStarted": "activityStartedEventDetails",
    "ActivitySucceeded": "activitySucceededEventDetails",
    "ActivityTimedOut": "activityTimedOutEventDetails",
    "ChoiceStateEntered": "stateEnteredEventDetails",
    "ChoiceStateExited": "stateExitedEventDetails",
    "ExecutionFailed": "executionFailedEventDetails",
    "ExecutionStarted": "executionStartedEventDetails",
    "ExecutionSucceeded": "executionSucceededEventDetails",
    "ExecutionAborted": "executionAbortedEventDetails",
    "ExecutionTimedOut": "executionTimedOutEventDetails",
    "FailStateEntered": "stateEnteredEventDetails",
    "LambdaFunctionFailed": "lambdaFunctionFailedEventDetails",
    "LambdaFunctionScheduleFailed": "lambdaFunctionScheduleFailedEventDetails",
    "LambdaFunctionScheduled": "lambdaFunctionScheduledEventDetails",
    "LambdaFunctionStartFailed": "lambdaFunctionStartFailedEventDetails",
    # "LambdaFunctionStarted": "lambdaFunctionStartedEventDetails",
    "LambdaFunctionSucceeded": "lambdaFunctionSucceededEventDetails",
    "LambdaFunctionTimedOut": "lambdaFunctionTimedOutEventDetails",
    "SucceedStateEntered": "stateEnteredEventDetails",
    "SucceedStateExited": "stateExitedEventDetails",
    # "TaskStateAborted": "stateAbortedEventDetails",
    "TaskStateEntered": "stateEnteredEventDetails",
    "TaskStateExited": "stateExitedEventDetails",
    "PassStateEntered": "stateEnteredEventDetails",
    "PassStateExited": "stateExitedEventDetails",
    # "ParallelStateAborted": "stateAbortedEventDetails",
    "ParallelStateEntered": "stateEnteredEventDetails",
    "ParallelStateExited": "stateExitedEventDetails",
    "ParallelStateFailed": "stateFailedEventDetails",
    # "ParallelStateStarted": "stateStartedEventDetails",
    # "ParallelStateSucceeded": "stateSucceededEventDetails",
    # "WaitStateAborted": "stateAbortedEventDetails",
    "WaitStateEntered": "stateEnteredEventDetails",
    "WaitStateExited": "stateExitedEventDetails"}


class Event:  # TODO: unit-test
    """An execution history event.

    Args:
        timestamp (datetime.datetime): event time-stamp
        event_type: type of event
        event_id: identifying index of event
        previous_event_id: identifying index of causal event
    """

    def __init__(
            self,
            timestamp,
            event_type: str,
            event_id: int,
            previous_event_id: int = None):
        self.timestamp = timestamp
        self.event_type = event_type
        self.event_id = event_id
        self.previous_event_id = previous_event_id

    def __str__(self):
        fmt = "%s [%s] @ %s"
        return fmt % (self.event_type, self.event_id, self.timestamp)

    __repr__ = _util.easy_repr

    @staticmethod
    def _get_args(
            history_event: T.Dict[str, _util.JSONable]
    ) -> T.Tuple[tuple, T.Dict[str, T.Any], T.Dict[str, _util.JSONable]]:
        """Get initialisation arguments by parsing history event.

        Args:
            history_event: execution history event, provided by AWS API

        Returns:
            initialisation positional and keyword arguments, and event details
        """

        # _logger.debug("history_event: %s" % history_event)
        timestamp = history_event["timestamp"]
        event_type = history_event["type"]
        event_id = history_event["id"]
        args = (timestamp, event_type, event_id)
        kwargs = {}
        if "previousEventId" in history_event:
            kwargs["previous_event_id"] = history_event["previousEventId"]
        details = history_event.get(_type_keys.get(event_type), {})
        return args, kwargs, details

    @classmethod
    def from_history_event(
            cls,
            history_event: T.Dict[str, _util.JSONable]
    ) -> "Event":
        """Parse an history event.

        Args:
            history_event: execution history event date, provided by AWS API

        Returns:
            Event: constructed execution history event
        """

        args, kwargs, _ = cls._get_args(history_event)
        return cls(*args, **kwargs)

    @_util.cached_property
    def details_str(self) -> str:
        """Format the event details.

        Returns:
            str: event details, formatted as string
        """

        return ""


class Failed(Event):  # TODO: unit-test
    """An execution history failure event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        previous_event_id: identifying index of causal event
        error: error type
        cause: failure details
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id=None,
            error: str = None,
            cause: str = None):
        super().__init__(
            timestamp,
            event_type,
            event_id,
            previous_event_id=previous_event_id)
        self.error = error
        self.cause = cause

    @classmethod
    def _get_args(cls, history_event):
        args, kwargs, details = super()._get_args(history_event)
        if "error" in details:
            kwargs["error"] = details["error"]
        if "cause" in details:
            kwargs["cause"] = details["cause"]
        return args, kwargs, details

    @_util.cached_property
    def details_str(self):
        return "error: %s" % self.error


class LambdaFunctionScheduled(Event):  # TODO: unit-test
    """An execution history AWS Lambda task-schedule event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        resource: AWS Lambda function ARN
        previous_event_id: identifying index of causal event
        task_input: task input
        timeout: time-out (seconds) of task execution
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            resource: str,
            previous_event_id=None,
            task_input: _util.JSONable = _default,
            timeout: int = None):
        super().__init__(
            timestamp,
            event_type,
            event_id,
            previous_event_id=previous_event_id)
        self.resource = resource
        self.task_input = task_input
        self.timeout = timeout

    @classmethod
    def _get_args(cls, history_event):
        args, kwargs, details = super()._get_args(history_event)
        args += (details["resource"],)
        if "input" in details:
            kwargs["task_input"] = json.loads(details["input"])
        if "timeoutInSeconds" in details:
            kwargs["timeout"] = details["timeoutInSeconds"]
        return args, kwargs, details

    @_util.cached_property
    def details_str(self):
        return "resource: %s" % self.resource


class ActivityScheduled(LambdaFunctionScheduled):  # TODO: unit-test
    """An execution history activity task-schedule event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        resource: AWS Lambda function ARN
        previous_event_id: identifying index of causal event
        task_input: task input
        timeout: time-out (seconds) of task execution
        heartbeat: heartbeat time-out (seconds)
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            resource,
            previous_event_id=None,
            task_input=_default,
            timeout=None,
            heartbeat: int = None):
        super().__init__(
            timestamp,
            event_type,
            event_id,
            resource,
            previous_event_id=previous_event_id,
            task_input=task_input,
            timeout=timeout)
        self.heartbeat = heartbeat

    @classmethod
    def _get_args(cls, history_event):
        args, kwargs, details = super()._get_args(history_event)
        if "heartbeatInSeconds" in details:
            kwargs["heartbeat"] = details["heartbeatInSeconds"]
        return args, kwargs, details


class ActivityStarted(Event):  # TODO: unit-test
    """An execution history activity task-start event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        worker_name: name of activity worker executing activity task
        previous_event_id: identifying index of causal event
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            worker_name: str,
            previous_event_id=None):
        super().__init__(
            timestamp,
            event_type,
            event_id,
            previous_event_id=previous_event_id)
        self.worker_name = worker_name

    @classmethod
    def _get_args(cls, history_event):
        args, kwargs, details = super()._get_args(history_event)
        args += (details["workerName"],)
        return args, kwargs, details

    @_util.cached_property
    def details_str(self):
        return "worker: %s" % self.worker_name


class ObjectSucceeded(Event):  # TODO: unit-test
    """An execution history succeed event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        previous_event_id: identifying index of causal event
        output: output of state/execution
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id=None,
            output: _util.JSONable = _default):
        super().__init__(
            timestamp,
            event_type,
            event_id,
            previous_event_id=previous_event_id)
        self.output = output

    @classmethod
    def _get_args(cls, history_event):
        args, kwargs, details = super()._get_args(history_event)
        if "output" in details:
            kwargs["output"] = json.loads(details["output"])
        return args, kwargs, details


class ExecutionStarted(Event):  # TODO: unit-test
    """An execution history execution-start event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        previous_event_id: identifying index of causal event
        execution_input: execution input
        role_arn: execution AWS IAM role ARN
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id=None,
            execution_input: _util.JSONable = _default,
            role_arn: str = None):
        super().__init__(
            timestamp,
            event_type,
            event_id,
            previous_event_id=previous_event_id)
        self.execution_input = execution_input
        self.role_arn = role_arn

    @classmethod
    def _get_args(cls, history_event):
        args, kwargs, details = super()._get_args(history_event)
        if "input" in details:
            kwargs["execution_input"] = json.loads(details["input"])
        if "roleArn" in details:
            kwargs["role_arn"] = details["roleArn"]
        return args, kwargs, details


class StateEntered(Event):  # TODO: unit-test
    """An execution history state-enter event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        state_name: state name
        previous_event_id: identifying index of causal event
        state_input: state input
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            state_name: str,
            previous_event_id=None,
            state_input: _util.JSONable = _default):
        super().__init__(
            timestamp,
            event_type,
            event_id,
            previous_event_id=previous_event_id)
        self.state_name = state_name
        self.state_input = state_input

    @classmethod
    def _get_args(cls, history_event):
        args, kwargs, details = super()._get_args(history_event)
        args += (details["name"],)
        if "input" in details:
            kwargs["state_input"] = json.loads(details["input"])
        return args, kwargs, details

    @_util.cached_property
    def details_str(self):
        return "name: %s" % self.state_name


class StateExited(Event):  # TODO: unit-test
    """An execution history state-exit event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        state_name: state name
        previous_event_id: identifying index of causal event
        output: state output
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            state_name: str,
            previous_event_id=None,
            output: _util.JSONable = _default):
        super().__init__(
            timestamp,
            event_type,
            event_id,
            previous_event_id=previous_event_id)
        self.state_name = state_name
        self.output = output

    @classmethod
    def _get_args(cls, history_event):
        args, kwargs, details = super()._get_args(history_event)
        args += (details["name"],)
        if "output" in details:
            kwargs["output"] = json.loads(details["output"])
        return args, kwargs, details

    @_util.cached_property
    def details_str(self):
        return "name: %s" % self.state_name


_type_classes = {
    "ActivityFailed": Failed,
    "ActivityScheduleFailed": Failed,
    "ActivityScheduled": ActivityScheduled,
    "ActivityStarted": ActivityStarted,
    "ActivitySucceeded": ObjectSucceeded,
    "ActivityTimedOut": Failed,
    "ChoiceStateEntered": StateEntered,
    "ChoiceStateExited": StateExited,
    "ExecutionFailed": Failed,
    "ExecutionStarted": ExecutionStarted,
    "ExecutionSucceeded": ObjectSucceeded,
    "ExecutionAborted": Failed,
    "ExecutionTimedOut": Failed,
    "FailStateEntered": StateEntered,
    "LambdaFunctionFailed": Failed,
    "LambdaFunctionScheduleFailed": Failed,
    "LambdaFunctionScheduled": LambdaFunctionScheduled,
    "LambdaFunctionStartFailed": Failed,
    "LambdaFunctionStarted": Event,
    "LambdaFunctionSucceeded": ObjectSucceeded,
    "LambdaFunctionTimedOut": Failed,
    "SucceedStateEntered": StateEntered,
    "SucceedStateExited": StateExited,
    "TaskStateAborted": Event,
    "TaskStateEntered": StateEntered,
    "TaskStateExited": StateExited,
    "PassStateEntered": StateEntered,
    "PassStateExited": StateExited,
    "ParallelStateAborted": Event,
    "ParallelStateEntered": StateEntered,
    "ParallelStateExited": StateExited,
    "ParallelStateFailed": Failed,
    "ParallelStateStarted": Event,
    "ParallelStateSucceeded": Event,
    "WaitStateAborted": Event,
    "WaitStateEntered": StateEntered,
    "WaitStateExited": StateExited}


def parse_history(  # TODO: unit-test
        history_events: T.List[T.Dict[str, _util.JSONable]]
) -> T.List[Event]:
    """List the execution history.

    Args:
        history_events: history events as provided by AWS API

    Returns:
        history of execution events
    """

    events = []
    for history_event in history_events:
        eclass = _type_classes[history_event["type"]]
        event = eclass.from_history_event(history_event)
        events.append(event)
    return events
