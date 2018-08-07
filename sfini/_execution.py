# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN state-machine execution."""

import time
import json
import logging as lg

from . import _util

_logger = lg.getLogger(__name__)


class Execution:  # TODO: unit-test
    """A state-machine execution.

    Args:
        name (str): name of execution
        state_machine (StateMachine): state-machine to execute
        execution_input: execution input (must be JSON-serialisable)
        session (AWSSession): AWS session to use for AWS communication
    """

    def __init__(self, name, state_machine, execution_input, *, session=None):
        self.name = name
        self.state_machine = state_machine
        self.execution_input = execution_input
        self.session = session or _util.AWSSession()

        self._start_time = None
        self._arn = None
        self._output = None

    def __str__(self):
        _s = "%s '%s' on '%s'"
        return _s % (type(self).__name__, self.name, self.state_machine)

    def __repr__(self):
        return "%s(%s, %s, len(execution_input)=%s, session=%s)" % (
            type(self).__name__,
            repr(self.name),
            repr(self.state_machine),
            len(self.execution_input),
            repr(self.session))

    @classmethod
    def from_arn(cls, arn, *, session=None):
        """Construct an ``Execution`` from an existing execution.

        Args:
            arn (str): existing execution ARN
            session (AWSSession): AWS session to use for AWS communication

        Returns:
            Execution: described execution. Note that the ``state_machine``
                attribute will be the ARN of the state-machine, not a
                ``StateMachine`` instance
        """

        session = session or _util.AWSSession()
        resp = session.sfn.describe_execution(executionArn=arn)
        sm_arn = resp["stateMachineArn"]
        self = cls(resp["name"], sm_arn, resp["input"], session=session)
        self._start_time = resp["startDate"]
        self._arn = arn
        self._output = resp.get("output", None)
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
            input=self.execution_input)
            # input=json.dumps(self.execution_input))
        self._arn = resp["executionArn"]
        self._start_time = resp["startDate"]

    def _get_execution_status(self):
        """Request status of this execution.

        Returns:
            str: execution status
        """

        if self._output is not None:
            return "SUCCEEDED"
        if self._start_time is None:
            raise RuntimeError("Execution not yet started")
        resp = self.session.sfn.describe_execution(executionArn=self._arn)
        if resp["status"] == "SUCCEEDED" and self._output is None:
            self._output = resp["output"]
        return resp["status"]

    @property
    def output(self):
        """Output of execution."""
        if self._output is None:
            if self._get_execution_status() != "SUCCEEDED":
                raise RuntimeError("Execution not yet finished")
        return self._output

    def wait(self, raise_on_error=True):
        """Wait for execution to finish.

        Args:
            raise_on_error (bool): raise error when execution fails
        """

        while True:
            status = self._get_execution_status()
            if status != "RUNNING":
                if status == "SUCCEEDED" or not raise_on_error:
                    break
                raise RuntimeError("Execution %s" % status)
            time.sleep(3.0)

    def stop(self, error_code=None, details=None):
        """Stop an existing execution.

        Args:
            error_code (str): stop reason identification
            details (str): stop reason
        """

        status = self._get_execution_status()
        if status != "RUNNING":
            raise RuntimeError("Cannot stop execution; execution %s" % status)
        _kw = {}
        if error_code:
            _kw["error"] = error_code
        if details:
            _kw["cause"] = details
        resp = self.session.sfn.stop_execution(executionArn=self._arn, **_kw)
        _logger.info("Execution stopped on %s" % resp["stopDate"])

    def print_history(self):
        """Print the execution history."""
        if self._start_time is None:
            raise RuntimeError("Execution not yet started")
        resp = _util.collect_paginated(
            self.session.sfn.get_execution_history,
            kwargs={"executionArn": self._arn})
        for event in resp["events"]:
            id_ = event["id"]
            ts = event["timestamp"]
            t = event["type"]
            prev_id = event["previousEventId"]
            detes = event[event["type"]]
            print("  [%s] %s %s (from %s):\n%s" % (id_, ts, t, prev_id, detes))
