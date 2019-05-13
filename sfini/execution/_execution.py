# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""State-machine execution."""

import time
import json
import typing as T
import logging as lg

from .. import _util
from . import history as sfini_execution_history

_logger = lg.getLogger(__name__)
_default = _util.DefaultParameter()


class Execution:  # TODO: unit-test
    """A state-machine execution.

    Args:
        name: name of execution
        state_machine (sfini.StateMachine): state-machine to execute
        execution_input: execution input (must be JSON-serialisable)
        session: session to use for AWS communication
    """

    _wait_sleep_time = 3.0

    def __init__(
            self,
            name: str,
            state_machine,
            execution_input: _util.JSONable,
            *,
            session: _util.AWSSession = None):
        self.name = name
        self.state_machine = state_machine
        self.execution_input = execution_input
        self.session = session or _util.AWSSession()

        self._start_time = None
        self._arn = None
        self._output = _default

    def __str__(self):
        _s = "%s '%s' on '%s'"
        return _s % (type(self).__name__, self.name, self.state_machine)

    def __repr__(self):
        _eii = isinstance(self.execution_input, (dict, list, tuple))
        _ei = len(self.execution_input) if _eii else repr(self.execution_input)
        _eips = "len(execution_input)=" if _eii else ""
        return "%s(%s, %s, %s%s, session=%s)" % (
            type(self).__name__,
            repr(self.name),
            repr(self.state_machine),
            _eips,
            _ei,
            repr(self.session))

    @classmethod
    def from_arn(
            cls,
            arn: str,
            *,
            session: _util.AWSSession = None
    ) -> "Execution":
        """Construct an ``Execution`` from an existing execution.

        Args:
            arn: existing execution ARN
            session: session to use for AWS communication

        Returns:
            described execution. Note that the ``state_machine`` attribute
                will be the ARN of the state-machine, not a ``StateMachine``
                instance
        """

        session = session or _util.AWSSession()
        resp = session.sfn.describe_execution(executionArn=arn)
        sm_arn = resp["stateMachineArn"]
        self = cls(resp["name"], sm_arn, resp["input"], session=session)
        self._start_time = resp["startDate"]
        self._arn = arn
        self._output = resp.get("output", _default)
        return self

    @classmethod
    def from_execution_list_item(
            cls,
            item: T.Dict[str, _util.JSONable],
            *,
            session: _util.AWSSession = None
    ) -> "Execution":
        """Construct an ``Execution`` from a list response item.

        Args:
            item: execution list item
            session: AWS session to use for AWS
                communication

        Returns:
            described execution. Note that the ``state_machine`` attribute
                will be the ARN of the state-machine, not a ``StateMachine``
                instance
        """

        self = cls(
            item["name"],
            item["stateMachineArn"],
            None,
            session=session)
        self._start_time = item["startDate"]
        self._arn = item["executionArn"]
        return self

    def start(self):
        """Start this state-machine execution."""
        if self._start_time is not None:
            _s = "Execution already started at %s"
            raise RuntimeError(_s % self._start_time)
        _util.assert_valid_name(self.name)
        resp = self.session.sfn.start_execution(
            stateMachineArn=self.state_machine.arn,
            name=self.name,
            input=json.dumps(self.execution_input))
        self._arn = resp["executionArn"]
        self._start_time = resp["startDate"]

    def _get_execution_status(self) -> str:
        """Request status of this execution.

        Returns:
            execution status
        """

        if self._output != _default:
            return "SUCCEEDED"
        if self._start_time is None:
            raise RuntimeError("Execution not yet started")
        resp = self.session.sfn.describe_execution(executionArn=self._arn)
        if resp["status"] == "SUCCEEDED" and "output" in resp:
            self._output = resp["output"]
        return resp["status"]

    @property
    def output(self) -> _util.JSONable:
        """Output of execution."""
        if self._output == _default:
            if self._get_execution_status() != "SUCCEEDED":
                raise RuntimeError("Execution not yet finished")
        return self._output

    def wait(self, raise_on_error: bool = True, timeout: float = None):
        """Wait for execution to finish.

        Args:
            raise_on_error: raise error when execution fails
            timeout: time to wait for execution to finish (seconds),
                default: no time-out

        Raises:
            RuntimeError: if execution finishes unsuccessfully, or if time-out
                is specified and reached before execution finishes
        """

        t = time.time()
        while True:
            status = self._get_execution_status()
            if status != "RUNNING":
                if status == "SUCCEEDED" or not raise_on_error:
                    break
                raise RuntimeError("Execution '%s' %s" % (self, status))
            if timeout is not None and time.time() - t > timeout:
                raise RuntimeError("Time-out waiting on execution '%s'" % self)
            time.sleep(self._wait_sleep_time)

    def stop(self, error_code: str = _default, details: str = _default):
        """Stop an existing execution.

        Args:
            error_code: stop reason identification
            details: stop reason
        """

        status = self._get_execution_status()
        if status != "RUNNING":
            raise RuntimeError("Cannot stop execution; execution %s" % status)
        _kw = {}
        if error_code != _default:
            _kw["error"] = error_code
        if details != _default:
            _kw["cause"] = details
        resp = self.session.sfn.stop_execution(executionArn=self._arn, **_kw)
        _logger.info("Execution stopped on %s" % resp["stopDate"])

    def get_history(self) -> T.List[sfini_execution_history.Event]:
        """List the execution history.

        Returns:
            history of execution events
        """

        if self._start_time is None:
            raise RuntimeError("Execution not yet started")
        resp = _util.collect_paginated(
            self.session.sfn.get_execution_history,
            executionArn=self._arn)
        return sfini_execution_history.parse_history(resp["events"])

    def format_history(self) -> str:
        """Format the execution history for printing.

        Returns:
            history formatted
        """

        events = self.get_history()
        lines = []
        for event in events:
            _d = event.details_str
            lines.append("%s:\n  %s" % (event, _d) if _d else str(event))
        if self._output != _default:
            lines.append("Output: %s" % json.dumps(self._output))
        return "\n".join(lines)
