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
_type_key_map = {
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
            previous_event_id: int):
        self.timestamp = timestamp
        self.event_type = event_type
        self.event_id = event_id
        self.previous_event_id = previous_event_id

    def __str__(self):
        _s = "%s [%s] @ %s"
        return _s % (self.event_type, self.event_id, self.timestamp)

    def __repr__(self):
        return "%s(%s, %s, %s, %s)" % (
            type(self).__name__,
            repr(self.timestamp),
            repr(self.event_type),
            repr(self.event_id),
            repr(self.previous_event_id))

    @staticmethod
    def _get_args(
            history_event: T.Dict[str, _util.JSONable]
    ) -> T.Tuple[tuple, T.Dict[str, _util.JSONable]]:
        """Get initialisation arguments by parsing history event.

        Args:
            history_event: execution history event date, provided by AWS API

        Returns:
            initialisation arguments, and event details
        """

        # _logger.debug("history_event: %s" % history_event)
        timestamp = history_event["timestamp"]
        event_type = history_event["type"]
        event_id = history_event["id"]
        previous_event_id = history_event.get("previousEventId", _default)
        details = history_event[_type_key_map[event_type]]
        return (timestamp, event_type, event_id, previous_event_id), details

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

        args, _ = cls._get_args(history_event)
        return cls(*args)

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
        error error name
        cause failure details
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            error: str,
            cause: str):
        super().__init__(timestamp, event_type, event_id, previous_event_id)
        self.error = error
        self.cause = cause

    def __repr__(self):
        return "%s(%s, %s, %s, %s, %s, %s)" % (
            type(self).__name__,
            repr(self.timestamp),
            repr(self.event_type),
            repr(self.event_id),
            repr(self.previous_event_id),
            repr(self.error),
            repr(self.cause))

    @classmethod
    def _get_args(cls, history_event):
        args, details = super()._get_args(history_event)
        error = details["error"]
        cause = details["cause"]
        return args + (error, cause), details

    @_util.cached_property
    def details_str(self):
        return "error: %s" % self.error


class LambdaFunctionScheduled(Event):  # TODO: unit-test
    """An execution history AWS Lambda task-schedule event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        previous_event_id: identifying index of causal event
        resource: AWS Lambda function ARN
        task_input: task input
        timeout: time-out (seconds) of task execution
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            resource: str,
            task_input: _util.JSONable,
            timeout: int):
        super().__init__(timestamp, event_type, event_id, previous_event_id)
        self.resource = resource
        self.task_input = task_input
        self.timeout = timeout

    def __repr__(self):
        return "%s(%s, %s, %s, %s, %s, %s, %s)" % (
            type(self).__name__,
            repr(self.timestamp),
            repr(self.event_type),
            repr(self.event_id),
            repr(self.previous_event_id),
            repr(self.resource),
            repr(self.task_input),
            repr(self.timeout))

    @classmethod
    def _get_args(cls, history_event):
        args, details = super()._get_args(history_event)
        resource = details["resource"]
        task_input = json.loads(details["input"])
        timeout = details.get("timeoutInSeconds", _default)
        return args + (resource, task_input, timeout), details

    @_util.cached_property
    def details_str(self):
        return "resource: %s" % self.resource


class ActivityScheduled(LambdaFunctionScheduled):  # TODO: unit-test
    """An execution history activity task-schedule event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        previous_event_id: identifying index of causal event
        resource: AWS Lambda function ARN
        task_input: task input
        timeout: time-out (seconds) of task execution
        heartbeat: heartbeat time-out (seconds)
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            resource,
            task_input,
            timeout,
            heartbeat: int):
        super().__init__(
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            resource,
            task_input,
            timeout)
        self.heartbeat = heartbeat

    def __repr__(self):
        return "%s(%s, %s, %s, %s, %s, %s, %s, %s)" % (
            type(self).__name__,
            repr(self.timestamp),
            repr(self.event_type),
            repr(self.event_id),
            repr(self.previous_event_id),
            repr(self.resource),
            repr(self.task_input),
            repr(self.timeout),
            repr(self.heartbeat))

    @classmethod
    def _get_args(cls, history_event):
        args, details = super()._get_args(history_event)
        heartbeat = details["heartbeatInSeconds"]
        return args + (heartbeat,), details


class ActivityStarted(Event):  # TODO: unit-test
    """An execution history activity task-start event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        previous_event_id: identifying index of causal event
        worker_name: name of activity worker executing activity task
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            worker_name: str):
        super().__init__(timestamp, event_type, event_id, previous_event_id)
        self.worker_name = worker_name

    def __repr__(self):
        return "%s(%s, %s, %s, %s, %s)" % (
            type(self).__name__,
            repr(self.timestamp),
            repr(self.event_type),
            repr(self.event_id),
            repr(self.previous_event_id),
            repr(self.worker_name))

    @classmethod
    def _get_args(cls, history_event):
        args, details = super()._get_args(history_event)
        worker_name = details["workerName"]
        return args + (worker_name,), details

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
            previous_event_id,
            output: _util.JSONable):
        super().__init__(timestamp, event_type, event_id, previous_event_id)
        self.output = output

    def __repr__(self):
        return "%s(%s, %s, %s, %s, %s)" % (
            type(self).__name__,
            repr(self.timestamp),
            repr(self.event_type),
            repr(self.event_id),
            repr(self.previous_event_id),
            repr(self.output))

    @classmethod
    def _get_args(cls, history_event):
        args, details = super()._get_args(history_event)
        output = json.loads(details["output"])
        return args + (output,), details


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
            previous_event_id,
            execution_input: _util.JSONable,
            role_arn: str):
        super().__init__(timestamp, event_type, event_id, previous_event_id)
        self.execution_input = execution_input
        self.role_arn = role_arn

    def __repr__(self):
        return "%s(%s, %s, %s, %s, %s, %s)" % (
            type(self).__name__,
            repr(self.timestamp),
            repr(self.event_type),
            repr(self.event_id),
            repr(self.previous_event_id),
            repr(self.execution_input),
            repr(self.role_arn))

    @classmethod
    def _get_args(cls, history_event):
        args, details = super()._get_args(history_event)
        execution_input = json.loads(details["input"])
        role_arn = details["roleArn"]
        return args + (execution_input, role_arn), details


class StateEntered(Event):  # TODO: unit-test
    """An execution history state-enter event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        previous_event_id: identifying index of causal event
        state_name: state name
        state_input: state input
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            state_name: str,
            state_input: _util.JSONable):
        super().__init__(timestamp, event_type, event_id, previous_event_id)
        self.state_name = state_name
        self.state_input = state_input

    def __repr__(self):
        return "%s(%s, %s, %s, %s, %s, %s)" % (
            type(self).__name__,
            repr(self.timestamp),
            repr(self.event_type),
            repr(self.event_id),
            repr(self.previous_event_id),
            repr(self.state_name),
            repr(self.state_input))

    @classmethod
    def _get_args(cls, history_event):
        args, details = super()._get_args(history_event)
        state_name = details["name"]
        state_input = json.loads(details["input"])
        return args + (state_name, state_input), details

    @_util.cached_property
    def details_str(self):
        return "name: %s" % self.state_name


class StateExited(Event):  # TODO: unit-test
    """An execution history state-exit event.

    Args:
        timestamp: event time-stamp
        event_type: type of event
        event_id: identifying index of event
        previous_event_id: identifying index of causal event
        state_name: state name
        state_output: state output
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            state_name: str,
            state_output: _util.JSONable):
        super().__init__(timestamp, event_type, event_id, previous_event_id)
        self.state_name = state_name
        self.state_output = state_output

    def __repr__(self):
        return "%s(%s, %s, %s, %s, %s, %s)" % (
            type(self).__name__,
            repr(self.timestamp),
            repr(self.event_type),
            repr(self.event_id),
            repr(self.previous_event_id),
            repr(self.state_name),
            repr(self.state_output))

    @classmethod
    def _get_args(cls, history_event):
        args, details = super()._get_args(history_event)
        state_name = details["name"]
        state_output = json.loads(details["output"])
        return args + (state_name, state_output), details

    @_util.cached_property
    def details_str(self):
        return "name: %s" % self.state_name


_type_class_map = {
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
        eclass = _type_class_map[history_event["type"]]
        event = eclass.from_history_event(history_event)
        events.append(event)
    return events
