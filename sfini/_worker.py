# --- 80 characters ----------------------------------------------------------
# Created by: Laurie 2018/08/12

"""Task runner."""

import json
import uuid
import time
import socket
import threading
import traceback
import logging as lg

from botocore import exceptions as bc_exc

from . import _util
from . import _state_error

_logger = lg.getLogger(__name__)
_host_name = socket.getfqdn(socket.gethostname())


class _TaskExecution:  # TODO: unit-test
    """Execute a task, providing heartbeats and catching failures.

    Args:
        activity (Activity): activity to execute task of
        task_token (str): task token for execution identification
        task_input: task input
        session (_util.AWSSession): session to communicate to AWS with
    """

    def __init__(self, activity, task_token, task_input, *, session=None):
        self.activity = activity
        self.task_token = task_token
        self.task_input = task_input
        self.session = session or _util.AWSSession()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat)
        self._request_stop = False

    def __str__(self):
        return "Execution '%s' of '%s'" % (self.task_token, self.activity)

    def __repr__(self):
        return "%s(%s, %s, len(task_input)=%s, session=%s)" % (
            type(self).__name__,
            repr(self.activity),
            repr(self.task_token),
            len(self.task_input),
            repr(self.session))

    def _send_heartbeat(self):
        """Send a heartbeat."""
        _logger.debug("Sending heartbeat for '%s'" % self)

        try:
            _ = self.session.sfn.send_task_heartbeat(taskToken=self.task_token)
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

    def _send_failure(self, exc):
        """Report failure."""
        if self._request_stop:
            _logger.warning("Skipping sending failure as we're quitting")
            return

        _logger.info("Report task failure for '%s'" % self, exc_info=exc)

        self._request_stop = True
        cause = traceback.format_exception(type(exc), exc, exc.__traceback__)
        cause = "".join(cause)
        self.session.sfn.send_task_failure(
            taskToken=self.task_token,
            error=type(exc).__name__,
            cause=cause)

    def _send_success(self, res):
        """Report success."""
        if self._request_stop:
            _logger.warning("Skipping sending failure as we're quitting")
            return

        _s = "Report task success for '%s' with output: %s"
        _logger.info(_s % (self, res))

        self._request_stop = True
        self.session.sfn.send_task_success(
            taskToken=self.task_token,
            output=json.dumps(res))

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
            self._send_failure(e)
            return

        _s = "Task '%s' completed in %.6f seconds"
        _logger.debug(_s % (self, time.time() - t))
        self._send_success(res)

    def report_cancelled(self):
        """Cancel a task execution before beginning."""
        if self._request_stop:
            _logger.warning("Skipping sending cancellation as we're quitting")
            return

        _s = "Reporting task failure for '%s' due to cancellation"
        _logger.info(_s % self)

        self._request_stop = True
        self.session.sfn.send_task_failure(
            taskToken=self.task_token,
            error=_state_error.WorkerCancel.__name__,
            cause=str(_state_error.WorkerCancel()))


class Worker:  # TODO: unit-test
    """Worker to poll for task executions.

    Args:
        activity (Activity): activity to poll and run executions of
        name (str): name of worker, used for identification, default: a
            combination of UUID and host's FQDN
        session (_util.Session): session to use for AWS communication
    """

    _task_execution_class = _TaskExecution

    def __init__(self, activity, name=None, *, session=None):
        self.activity = activity
        self.name = name or "%s-%s" % (_host_name, uuid.uuid4())
        self.session = session or _util.AWSSession()

        self._poller = threading.Thread(target=self._worker)
        self._request_finish = False
        self._exc = None

    def __str__(self):
        _s = "%s '%s' on '%s'"
        return _s % (type(self).__name__, self.name, self.activity)

    def __repr__(self):
        return "%s(%s, %s, session=%s)" % (
            type(self).__name__,
            repr(self.activity),
            repr(self.name),
            repr(self.session))

    def _exectute_on(self, task_input, task_token):
        """Execute the provided task.

        Arguments:
            task_input: activity task execution input
            task_token (str): task execuion identifier
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
        while True:
            if self._request_finish:
                break
            _s = "Polling for activity '%s' executions"
            _logger.debug(_s % self.activity)
            resp = self.session.sfn.get_activity_task(
                activityArn=self.activity.arn,
                workerName=self.name)
            if resp.get("taskToken", None) is not None:
                self._exectute_on(json.loads(resp["input"]), resp["taskToken"])

    def _worker(self):
        """Run polling, catching exceptins."""
        _util.assert_valid_name(self.name)
        try:
            self._poll_and_execute()
        except (Exception, KeyboardInterrupt) as e:
            self._exc = e  # send exception to main thread
            self.end()

    def start(self):
        """Start polling."""
        self._poller.start()

    def join(self):
        """Block until polling exit."""
        try:
            self._poller.join()
        except KeyboardInterrupt:
            _logger.info("Quitting polling due to KeyboardInterrupt")
            self.end()
        except Exception:
            self.end()
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
