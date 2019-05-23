# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/12

"""Activity task polling and execution.

You can provide you're own workers: the interface to the activities is
public. This module's worker implementation uses threading, and is
designed to be resource-managed outside of Python.
"""

import json
import uuid
import time
import socket
import threading
import traceback
import typing as T
import logging as lg

from botocore import exceptions as bc_exc

from . import _util

_logger = lg.getLogger(__name__)
_host_name = socket.getfqdn(socket.gethostname())


class WorkerCancel(KeyboardInterrupt):  # TODO: unit-test
    """Workflow execution interrupted by user."""
    def __init__(self, *args, **kwargs):
        _msg = (
            "Activity execution cancelled by user. "
            "This could be due to a `KeyboardInterrupt` during execution, "
            "or the worker was killed during task polling.")
        super().__init__(_msg, *args, **kwargs)


class TaskExecution:  # TODO: unit-test
    """Execute a task, providing heartbeats and catching failures.

    Args:
        activity (sfini.activity.CallableActivity): activity to execute
            task of
        task_token: task token for execution identification
        task_input: task input
        session: session to use for AWS communication
    """

    def __init__(
            self,
            activity,
            task_token: str,
            task_input: _util.JSONable,
            *,
            session: _util.AWSSession = None):
        self.activity = activity
        self.task_token = task_token
        self.task_input = task_input
        self.session = session or _util.AWSSession()

        self._heartbeat_thread = threading.Thread(target=self._heartbeat)
        self._request_stop = False

    def __str__(self):
        return "%s - %s" % (self.activity.name, self.task_token)

    __repr__ = _util.easy_repr

    def _send(self, send_fn: T.Callable, **kw):
        """Send execution update to SFN."""
        if self._request_stop:
            _logger.warning("Skipping sending update as we've already quit")
            return
        return send_fn(taskToken=self.task_token, **kw)

    def _report_exception(self, exc: BaseException):
        """Report failure."""
        _logger.info("Reporting task failure for '%s'" % self, exc_info=exc)
        tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
        self._send(
            self.session.sfn.send_task_failure,
            error=type(exc).__name__,
            cause="".join(tb))
        self._request_stop = True

    def report_cancelled(self):
        """Cancel a task execution: stop interaction with SFN."""
        fmt = "Reporting task failure for '%s' due to cancellation"
        _logger.info(fmt % self)
        self._send(
            self.session.sfn.send_task_failure,
            error=WorkerCancel.__name__,
            cause=str(WorkerCancel()))
        self._request_stop = True

    def _report_success(self, res: _util.JSONable):
        """Report success."""
        fmt = "Reporting task success for '%s' with output: %s"
        _logger.debug(fmt % (self, res))
        self._send(self.session.sfn.send_task_success, output=json.dumps(res))
        self._request_stop = True

    def _send_heartbeat(self):
        """Send a heartbeat."""
        _logger.debug("Sending heartbeat for '%s'" % self)

        try:
            self._send(self.session.sfn.send_task_heartbeat)
        except bc_exc.ClientError as e:
            if e.response["Error"]["Code"] != "TaskTimedOut":
                raise
            _logger.error("Task execution '%s' timed-out" % self)
            self._request_stop = True

    def _heartbeat(self):
        """Run heartbeat sending."""
        heartbeat = self.activity.heartbeat
        heartbeat = min(max(heartbeat - 5.0, 1.0), heartbeat)
        while True:
            t = time.time()
            if self._request_stop:
                break
            self._send_heartbeat()
            time.sleep(heartbeat - (time.time() - t))

    def run(self):
        """Run task."""
        self._heartbeat_thread.start()
        t = time.time()

        try:
            res = self.activity.call_with(self.task_input)
        except KeyboardInterrupt:
            self.report_cancelled()
            return
        except Exception as e:
            self._report_exception(e)
            return

        fmt = "Task '%s' completed in %.6f seconds"
        _logger.debug(fmt % (self, time.time() - t))
        self._report_success(res)


class Worker:  # TODO: unit-test
    """Worker to poll for activity task executions.

    Args:
        activity (sfini.activity.CallableActivity): activity to poll and
            run executions of
        name: name of worker, used for identification, default: a
            combination of UUID and host's FQDN
        session: session to use for AWS communication
    """

    _task_execution_class = TaskExecution

    def __init__(
            self,
            activity,
            name: str = None,
            *,
            session: _util.AWSSession = None):
        self.activity = activity
        self.name = name or "%s-%s" % (_host_name, str(str(uuid.uuid4()))[:8])
        self.session = session or _util.AWSSession()

        self._poller = threading.Thread(target=self._worker)
        self._request_finish = False
        self._exc = None

    def __str__(self):
        return "%s [%s]" % (self.name, self.activity.name)

    __repr__ = _util.easy_repr

    def _execute_on(self, task_input: _util.JSONable, task_token: str):
        """Execute the provided task.

        Args:
            task_input: activity task execution input
            task_token: task execution identifier
        """

        _logger.debug("Got task input: %s" % task_input)

        execution = self._task_execution_class(
            self.activity,
            task_token,
            task_input,
            session=self.session)
        if self._request_finish:
            execution.report_cancelled()
        else:
            execution.run()

    def _poll_and_execute(self):
        """Poll for tasks to execute, then execute any found."""
        while not self._request_finish:
            fmt = "Polling for activity '%s' executions"
            _logger.debug(fmt % self.activity)
            resp = self.session.sfn.get_activity_task(
                activityArn=self.activity.arn,
                workerName=self.name)
            if resp.get("taskToken", None) is not None:
                input_ = json.loads(resp["input"])
                self._execute_on(input_, resp["taskToken"])

    def _worker(self):
        """Run polling, catching exceptins."""
        try:
            self._poll_and_execute()
        except (Exception, KeyboardInterrupt) as e:
            self._exc = e  # send exception to main thread
            self.end()

    def start(self):
        """Start polling."""
        from . import activity
        if not isinstance(self.activity, activity.CallableActivity):
            raise TypeError("Activity '%s' cannot be executed" % self.activity)
        _util.assert_valid_name(self.name)
        _logger.info("Worker '%s': waiting on final poll to finish" % self)
        self._poller.start()

    def join(self):
        """Block until polling exit."""
        try:
            self._poller.join()
        except KeyboardInterrupt:
            _logger.info("Quitting polling due to KeyboardInterrupt")
            self._request_finish = True
            return
        except Exception:
            self._request_finish = True
            raise
        if self._exc is not None:
            raise self._exc

    def end(self):
        """End polling."""
        _logger.info("Worker '%s': waiting on final poll to finish" % self)
        self._request_finish = True

    def run(self):
        """Run worker to poll for and execute specified tasks."""
        self.start()
        self.join()
