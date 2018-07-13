# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN state-machine execution."""

import time
import logging as lg

from . import _util

_logger = lg.getLogger(__name__)


class Execution:  # TODO: unit-test
    def __init__(
            self,
            name,
            state_machine,
            execution_input,
            *,
            session=None):
        self.name = name
        self.state_machine = state_machine
        self.execution_input = execution_input
        self.session = session or _util.AWSSession()

        self._start_time = None
        self._arn = None
        self._output = None

    def start(self):
        """Start this state-machine execution."""
        if self._start_time is not None:
            _s = "Execution already started at %s" % self._start_time
            raise RuntimeError(_s)
        _util.assert_valid_name(self.name)
        resp = self.session.sfn.start_execution(
            stateMachineArn=self.state_machine.arn,
            name=self.name,
            input=self.execution_input)
        self._arn = resp["executionArn"]
        self._start_time = resp["startDate"]

    def _get_execution_status(self):
        if self._start_time is None:
            raise RuntimeError("Execution not yet started")
        resp = self.session.sfn.describe_execution(executionArn=self._arn)
        if resp["status"] == "SUCCEEDED" and self._output is not None:
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
