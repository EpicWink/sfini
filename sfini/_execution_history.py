# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/08/09

"""SFN state-machine execution history events."""

import json
import logging as lg

from . import _util

_logger = lg.getLogger(__name__)
_type_key_map = {
    "ActivityFailed": "activityFailedEventDetails",
    "ActivityScheduleFailed": "activityScheduleFailedEventDetails",
    "ActivityScheduled": "activityScheduledEventDetails",
    "ActivityStarted": "activityStartedEventDetails",
    "ActivitySucceeded": "activitySucceededEventDetails",
    "ActivityTimedOut": "activityTimedOutEventDetails",
    "ChoiceStateEntered": "choiceStateEnteredEventDetails",
    "ChoiceStateExited": "choiceStateExitedEventDetails",
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


class _Event:  # TODO: unit-test
    """An execution history event.

    Arguments:
        timestamp (datetime): event time-stamp
        event_type (str): type of event
        event_id (int): identifying index of event
        previous_event_id (int): identifying index of causal event
    """

    def __init__(self, timestamp, event_type, event_id, previous_event_id):
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
    def _get_args(history_event):
        """Get initialisation arguments by parsing history event.

        Arguments:
            history_event (dict[str]): execution history event date, provided
                by AWS API

        Returns:
            tuple[tuple, dict]: initialisation arguments, and event details
        """

        timestamp = history_event["timestamp"]
        event_type = history_event["type"]
        event_id = history_event["id"]
        previous_event_id = history_event.get("previousEventId")
        details = history_event[_type_key_map[event_type]]
        return (timestamp, event_type, event_id, previous_event_id), details

    @classmethod
    def from_history_event(cls, history_event):
        """Parse an history event.

        Arguments:
            history_event (dict[str]): execution history event date, provided
                by AWS API

        Returns:
            _Event: constructed execution history event
        """

        args, _ = cls._get_args(history_event)
        return cls(*args)

    @_util.cached_property
    def details_str(self):
        """Format the event details.

        Returns:
            str: event details, formatted as string
        """

        return ""


class _Failed(_Event):  # TODO: unit-test
    """An execution history failure event.

    Arguments:
        timestamp (datetime): event time-stamp
        event_type (str): type of event
        event_id (int): identifying index of event
        previous_event_id (int): identifying index of causal event
        error (str): error name
        cause (str): failure details
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            error,
            cause):
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

    @staticmethod
    def _get_args(history_event):
        args, details = super()._get_args(history_event)
        error = details["error"]
        cause = details["cause"]
        return args + (error, cause), details

    @_util.cached_property
    def details_str(self):
        return "error: %s" % self.error


class _LambdaFunctionScheduled(_Event):  # TODO: unit-test
    """An execution history AWS Lambda task-schedule event.

    Arguments:
        timestamp (datetime): event time-stamp
        event_type (str): type of event
        event_id (int): identifying index of event
        previous_event_id (int): identifying index of causal event
        resource (str): AWS Lambda function ARN
        task_input: task input
        timeout (int): time-out (seconds) of task execution
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            resource,
            task_input,
            timeout):
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

    @staticmethod
    def _get_args(history_event):
        args, details = super()._get_args(history_event)
        resource = details["resource"]
        task_input = json.loads(details["input"])
        timeout = details["timeoutInSeconds"]
        return args + (resource, task_input, timeout), details

    @_util.cached_property
    def details_str(self):
        return "resource: %s" % self.resource


class _ActivityScheduled(_LambdaFunctionScheduled):  # TODO: unit-test
    """An execution history activity task-schedule event.

    Arguments:
        timestamp (datetime): event time-stamp
        event_type (str): type of event
        event_id (int): identifying index of event
        previous_event_id (int): identifying index of causal event
        resource (str): AWS Lambda function ARN
        task_input: task input
        timeout (int): time-out (seconds) of task execution
        heartbeat (int): heartbeat time-out (seconds)
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
            heartbeat):
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

    @staticmethod
    def _get_args(history_event):
        args, details = super()._get_args(history_event)
        heartbeat = details["heartbeatInSeconds"]
        return args + (heartbeat,), details


class _ActivityStarted(_Event):  # TODO: unit-test
    """An execution history activity task-start event.

    Arguments:
        timestamp (datetime): event time-stamp
        event_type (str): type of event
        event_id (int): identifying index of event
        previous_event_id (int): identifying index of causal event
        worker_name (str): name of activity worker executing activity task
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            worker_name):
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

    @staticmethod
    def _get_args(history_event):
        args, details = super()._get_args(history_event)
        worker_name = details["workerName"]
        return args + (worker_name,), details

    @_util.cached_property
    def details_str(self):
        return "worker: %s" % self.worker_name


class _ObjectSucceeded(_Event):  # TODO: unit-test
    """An execution history succeed event.

    Arguments:
        timestamp (datetime): event time-stamp
        event_type (str): type of event
        event_id (int): identifying index of event
        previous_event_id (int): identifying index of causal event
        output: output of state/execution
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            output):
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

    @staticmethod
    def _get_args(history_event):
        args, details = super()._get_args(history_event)
        output = json.loads(details["output"])
        return args + (output,), details


class _ExecutionStarted(_Event):  # TODO: unit-test
    """An execution history execution-start event.

    Arguments:
        timestamp (datetime): event time-stamp
        event_type (str): type of event
        event_id (int): identifying index of event
        previous_event_id (int): identifying index of causal event
        execution_input: execution input
        role_arn (str): execution AWS IAM role ARN
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            execution_input,
            role_arn):
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

    @staticmethod
    def _get_args(history_event):
        args, details = super()._get_args(history_event)
        execution_input = json.loads(details["input"])
        role_arn = details["roleArn"]
        return args + (execution_input, role_arn), details


class _StateEntered(_Event):  # TODO: unit-test
    """An execution history state-enter event.

    Arguments:
        timestamp (datetime): event time-stamp
        event_type (str): type of event
        event_id (int): identifying index of event
        previous_event_id (int): identifying index of causal event
        state_name (str): state name
        state_input: state input
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            state_name,
            state_input):
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

    @staticmethod
    def _get_args(history_event):
        args, details = super()._get_args(history_event)
        state_name = details["name"]
        state_input = json.loads(details["input"])
        return args + (state_name, state_input), details

    @_util.cached_property
    def details_str(self):
        return "name: %s" % self.state_name


class _StateExited(_Event):  # TODO: unit-test
    """An execution history state-exit event.

    Arguments:
        timestamp (datetime): event time-stamp
        event_type (str): type of event
        event_id (int): identifying index of event
        previous_event_id (int): identifying index of causal event
        state_name (str): state name
        state_output: state output
    """

    def __init__(
            self,
            timestamp,
            event_type,
            event_id,
            previous_event_id,
            state_name,
            state_output):
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

    @staticmethod
    def _get_args(history_event):
        args, details = super()._get_args(history_event)
        state_name = details["name"]
        state_output = json.loads(details["output"])
        return args + (state_name, state_output), details

    @_util.cached_property
    def details_str(self):
        return "name: %s" % self.state_name


_type_class_map = {
    "ActivityFailed": _Failed,
    "ActivityScheduleFailed": _Failed,
    "ActivityScheduled": _ActivityScheduled,
    "ActivityStarted": _ActivityStarted,
    "ActivitySucceeded": _ObjectSucceeded,
    "ActivityTimedOut": _Failed,
    "ChoiceStateEntered": _StateEntered,
    "ChoiceStateExited": _StateExited,
    "ExecutionFailed": _Failed,
    "ExecutionStarted": _ExecutionStarted,
    "ExecutionSucceeded": _ObjectSucceeded,
    "ExecutionAborted": _Failed,
    "ExecutionTimedOut": _Failed,
    "FailStateEntered": _StateEntered,
    "LambdaFunctionFailed": _Failed,
    "LambdaFunctionScheduleFailed": _Failed,
    "LambdaFunctionScheduled": _LambdaFunctionScheduled,
    "LambdaFunctionStartFailed": _Failed,
    "LambdaFunctionStarted": _Event,
    "LambdaFunctionSucceeded": _ObjectSucceeded,
    "LambdaFunctionTimedOut": _Failed,
    "SucceedStateEntered": _StateEntered,
    "SucceedStateExited": _StateExited,
    "TaskStateAborted": _Event,
    "TaskStateEntered": _StateEntered,
    "TaskStateExited": _StateExited,
    "PassStateEntered": _StateEntered,
    "PassStateExited": _StateExited,
    "ParallelStateAborted": _Event,
    "ParallelStateEntered": _StateEntered,
    "ParallelStateExited": _StateExited,
    "ParallelStateFailed": _Failed,
    "ParallelStateStarted": _Event,
    "ParallelStateSucceeded": _Event,
    "WaitStateAborted": _Event,
    "WaitStateEntered": _StateEntered,
    "WaitStateExited": _StateExited}


def parse_history(history_events):  # TODO: unit-test
    """List the execution history."""
    events = []
    for history_event in history_events:
        eclass = _type_class_map[history_event["type"]]
        event = eclass.from_history_event(history_event)
        events.append(event)
    return events
