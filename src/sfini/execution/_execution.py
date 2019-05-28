"""State-machine execution."""

import time
import json
import datetime
import typing as T
import logging as lg

from .. import _util
from . import history

_logger = lg.getLogger(__name__)
_default = _util.DefaultParameter()


class Execution:
    """A state-machine execution.

    Args:
        name: name of execution
        state_machine_arn: executed state-machine ARN
        execution_input: execution input (must be JSON-serialisable)
        session: session to use for AWS communication
    """

    _wait_sleep_time = 3.0
    _not_provided = object()

    def __init__(
            self,
            name: str,
            state_machine_arn: str,
            execution_input: _util.JSONable = _default,
            *,
            session: _util.AWSSession = None):
        self.name = name
        self.state_machine_arn = state_machine_arn
        self.execution_input = execution_input
        self.session = session or _util.AWSSession()

        self._status = None
        self._start_date = None
        self._stop_date = None
        self._output = _default

    def __str__(self):
        status_str = (" [%s]" % self._status) if self._status else ""
        return "%s%s" % (self.name, status_str)

    __repr__ = _util.easy_repr

    @classmethod
    def from_arn(
            cls,
            arn: str,
            *,
            session: _util.AWSSession = None):
        """Construct an ``Execution`` from an existing execution.

        Args:
            arn: existing execution ARN
            session: session to use for AWS communication

        Returns:
            described execution
        """

        arn_split = arn.split(":", 7)
        name = arn_split[7]
        state_machine_arn = ":".join(arn_split[:7])
        self = cls(
            name,
            state_machine_arn,
            execution_input=cls._not_provided,
            session=session)
        assert self.arn == arn
        return self

    @classmethod
    def from_list_item(
            cls,
            item: T.Dict[str, _util.JSONable],
            *,
            session: _util.AWSSession = None):
        """Construct an ``Execution`` from a response list-item.

        Args:
            item: execution list item
            session: session to use for AWS communication

        Returns:
            described execution
        """

        self = cls.from_arn(item["executionArn"], session=session)
        assert self.name == item["name"]
        self._status = item["status"]
        self._start_date = item["startDate"]
        self._stop_date = item.get("stopDate")
        return self

    @property
    def arn(self) -> str:
        """Execution generated ARN."""
        return self.state_machine_arn + ":" + self.name

    @property
    def status(self) -> str:
        """Execution status."""
        if self._status in (None, "RUNNING"):
            self.update()
        return self._status

    @property
    def start_date(self) -> datetime.datetime:
        """Execution start time."""
        if self._start_date is None:
            self.update()
        return self._start_date

    @property
    def stop_date(self) -> datetime.datetime:
        """Execution stop time.

        Raises:
            RuntimeError: if execution is not yet finished
        """

        if self._stop_date is None:
            self.update()
            self._raise_unfinished()
        return self._stop_date

    @property
    def output(self) -> _util.JSONable:
        """Output of execution.

        Raises:
            RuntimeError: if execution is not yet finished, or execution
                failed
        """

        if self._output == _default:
            self.update()
            self._raise_unfinished()
            self._raise_on_failure()
        return self._output

    def update(self):
        """Update execution information from AWS."""
        status_known = self._status not in (None, "RUNNING")
        input_known = self.execution_input != self._not_provided
        if status_known and input_known:
            _logger.debug("Execution finished: update is unnecessary")
            return
        resp = self.session.sfn.describe_execution(executionArn=self.arn)
        assert resp["executionArn"] == self.arn
        assert resp["name"] == self.name
        assert resp["stateMachineArn"] == self.state_machine_arn
        self._status = resp["status"]
        self._start_date = resp["startDate"]
        self._stop_date = resp.get("stopDate")
        if "input" in resp:
            input_ = json.loads(resp["input"])
            if self.execution_input == self._not_provided:
                self.execution_input = input_
            else:
                assert self.execution_input == input_
        if "output" in resp:
            self._output = json.loads(resp["output"])

    def _raise_on_failure(self):
        """Raise ``RuntimeError`` on execution failure."""
        failed = self._status not in ("RUNNING", "SUCCEEDED")
        if failed:
            raise RuntimeError("Execution '%s' %s" % (self, self._status))

    def _raise_unfinished(self):
        """Raise ``RuntimeError`` when requiring execution to be finished."""
        if self._status == "RUNNING":
            raise RuntimeError("Execution '%s' not yet finished" % self)

    def start(self):
        """Start this state-machine execution.

        Sets the ``arn`` attribute.
        """

        _util.assert_valid_name(self.name)
        if self.execution_input == _default:
            self.execution_input = {}
        input_str = json.dumps(self.execution_input)
        resp = self.session.sfn.start_execution(
            stateMachineArn=self.state_machine_arn,
            name=self.name,
            input=input_str)
        assert self.arn == resp["executionArn"]
        self._status = "RUNNING"
        self._start_date = resp["startDate"]

    def wait(self, raise_on_failure: bool = True, timeout: float = None):
        """Wait for execution to finish.

        Args:
            raise_on_failure: raise error when execution fails
            timeout: time to wait for execution to finish (seconds),
                default: no time-out

        Raises:
            RuntimeError: if execution fails, or if time-out is reached
                before execution finishes
        """

        t = time.time()
        while True:
            self.update()
            if self._status != "RUNNING":
                break
            if timeout is not None and time.time() - t > timeout:
                raise RuntimeError("Time-out waiting on execution '%s'" % self)
            time.sleep(self._wait_sleep_time)
        if raise_on_failure:
            self._raise_on_failure()

    def stop(self, error_code: str = _default, details: str = _default):
        """Stop an existing execution.

        Args:
            error_code: stop reason identification
            details: stop reason

        Raises:
            RuntimeError: if execution is already finished
        """

        kw = {}
        if error_code != _default:
            kw["error"] = error_code
        if details != _default:
            kw["cause"] = details
        resp = self.session.sfn.stop_execution(executionArn=self.arn, **kw)
        self._stop_date = resp["stopDate"]
        _logger.info("Execution stopped on %s" % resp["stopDate"])

    def get_history(self) -> T.List[history.Event]:
        """List the execution history.

        Returns:
            history of execution events
        """

        resp = _util.collect_paginated(
            self.session.sfn.get_execution_history,
            executionArn=self.arn)
        return history.parse_history(resp["events"])

    def format_history(self) -> str:
        """Format the execution history for printing.

        Returns:
            history formatted
        """

        events = self.get_history()
        lines = []
        for event in events:
            ds = event.details_str
            line = ("%s:\n  %s" % (event, ds)) if ds else str(event)
            lines.append(line)
        self.update()
        if self._output != _default:
            line = "Output: %s" % json.dumps(self._output)
            lines.append(line)
        return "\n".join(lines)
