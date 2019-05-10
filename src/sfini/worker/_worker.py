"""Activity task polling and execution.

You can provide you're own workers: the interface to the activities is
public. This module's worker implementation uses threading, and is
designed to be resource-managed outside of Python.
"""

import json
import uuid
import time
import socket
import traceback
import typing as T
import logging as lg

from botocore import exceptions as bc_exc

from .. import _util
from . import _poll

_logger = lg.getLogger(__name__)
_host_name = socket.getfqdn(socket.gethostname())
threading = None


def _import_threading():
    """Import ``threading`` multi-threading module."""
    global threading
    if threading is None:
        import threading


class WorkerCancel(KeyboardInterrupt):
    """Workflow execution interrupted by user."""
    def __init__(self, *args, **kwargs):
        msg = (
            "Activity execution cancelled by user. "
            "This could be due to a `KeyboardInterrupt` during execution, "
            "or the worker was killed during task polling.")
        super().__init__(msg, *args, **kwargs)


class TaskExecution:
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

        _import_threading()
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
            self._request_stop = True
            if e.response["Error"]["Code"] != "TaskTimedOut":
                raise
            _logger.error("Task execution '%s' timed-out" % self)

    def _heartbeat(self):
        """Run heartbeat sending."""
        heartbeat = self.activity.heartbeat
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
            # print("%s: [%s] %s" % (self, type(e), e))
            self.report_cancelled()
            return
        except Exception as e:
            # print("%s: [%s] %s" % (self, type(e), e))
            self._report_exception(e)
            return

        fmt = "Task '%s' completed in %.6f seconds"
        _logger.debug(fmt % (self, time.time() - t))
        self._report_success(res)


class Worker:
    """Worker to poll for activity task executions.

    Args:
        activity (sfini.activity.CallableActivity): activity to poll and
            run executions of
        name: name of worker, used for identification, default: a
            combination of UUID and host's FQDN
        session: session to use for AWS communication

    Attributes:
        pre_execute_hooks: functions to call before task execution
        post_execute_hooks: functions to call after task execution
    """

    _task_execution_class = TaskExecution
    _task_poll_class = _poll.TaskPoll

    def __init__(
            self,
            activity,
            name: str = None,
            *,
            session: _util.AWSSession = None):
        self.activity = activity
        self.name = name or "%s-%s" % (_host_name, str(str(uuid.uuid4()))[:8])
        self.session = session or _util.AWSSession()

        _import_threading()
        self._poller = threading.Thread(target=self._worker)
        self._request_finish = False
        self.pre_execute_hooks: T.List[T.Callable] = []
        self.post_execute_hooks: T.List[T.Callable] = []
        self._allow_poll = threading.RLock()
        self._poll = self._task_poll_class(
            self.activity.arn,
            self.name,
            session=self.session)

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

        [fn() for fn in self.pre_execute_hooks]

        execution = self._task_execution_class(
            self.activity,
            task_token,
            task_input,
            session=self.session)
        if self._request_finish:
            execution.report_cancelled()
        else:
            execution.run()

        [fn() for fn in self.post_execute_hooks]

    def _poll_and_execute(self):
        """Poll for tasks to execute, then execute any found."""
        self._poll.start()
        while True:
            with self._allow_poll:
                if self._request_finish:
                    break
                resp = self._poll.get(timeout=5.0)
            if resp:
                input_ = json.loads(resp["input"])
                self._execute_on(input_, resp["task_token"])

    def _worker(self):
        """Run polling, catching exceptins."""
        try:
            self._poll_and_execute()
        except (Exception, KeyboardInterrupt) as e:
            _logger.warning("Polling/execution failed", exc_info=e)
            self._exc = e  # send exception to main thread
            self._request_finish = True

    def cancel_poll(self):
        """Cancel the current poll for tasks, and block polling."""
        if self._poller.is_alive():
            _logger.debug("Cancelling polling")
            self._poll.pause()
            self._allow_poll.acquire()  # waits for poll to be cancelled

    def allow_poll(self):
        """Unblock polling."""
        _logger.debug("Resuming polling")
        self._poll.unpause()
        self._allow_poll.release()

    def start(self):
        """Start polling."""
        from .. import activity
        if not isinstance(self.activity, activity.CallableActivity):
            raise TypeError("Activity '%s' cannot be executed" % self.activity)
        _util.assert_valid_name(self.name)
        _logger.info("%s: starting polling" % self)
        self._poller.start()

    def join(self):
        """Block until polling exit."""
        _logger.debug("%s: waiting on polling to finish" % self)
        try:
            self._poller.join()
        except KeyboardInterrupt:
            _logger.info("Quitting polling due to KeyboardInterrupt")
            self.end()
        except Exception as e:
            _logger.error("%s failed" % self, exc_info=e)
            self.end()
            raise

    def end(self):
        """End polling."""
        _logger.info("%s: waiting on final poll to finish" % self)
        self._request_finish = True
        self._poll.stop()

    def run(self):
        """Run worker to poll for and execute specified tasks."""
        self.start()
        self.join()


class WorkersManager:  # TODO: unit-test
    """Simultaneously poll for multiple task executions.

    Args:
        activities (list[sfini._activity.CallableActivity]): activities to
            poll and run executions of
        name: name of worker, used for identification, default: a
            combination of a short UUID and host's FQDN
        session: session to use for AWS communication
    """

    _worker_class = Worker

    def __init__(
            self,
            activities,
            name: str = None,
            *,
            session: _util.AWSSession = None):
        self.activities = activities
        self.name = name or "%s-%s" % (_host_name, str(uuid.uuid4())[:8])
        self.session = session or _util.AWSSession()

    def __str__(self):
        _as = ", ".join(a.name for a in self.activities)
        return "%s '%s' polling: %s" % (type(self).__name__, self.name, _as)

    __repr__ = _util.easy_repr

    @_util.cached_property
    def workers(self) -> T.List[_worker_class]:
        """Activity workers."""
        _n, _s = self.name, self.session
        workers = []
        for j, activity in enumerate(self.activities):
            worker = self._worker_class(activity, name=_n, session=_s)
            worker.pre_execute_hooks.append(lambda: self._cancel_poll(j))
            worker.post_execute_hooks.append(lambda: self._resume_poll(j))
            workers.append(worker)
        return workers

    def _cancel_poll(self, j: int):
        """Cancel polling for all but one worker.

        Args:
            j: index of worker to not cancel polling for
        """

        [w.cancel_poll() for k, w in enumerate(self.workers) if k != j]

    def _resume_poll(self, j: int):
        """Resume polling for all but one worker.

        Arguments:
            j: index of worker to not resume polling for
        """

        [w.allow_poll() for k, w in enumerate(self.workers) if k != j]

    def start(self):
        """Start workers."""
        [w.start() for w in self.workers]

    def join(self):
        """Wait for workers to finish, ending workers on exception."""
        try:
            [w.join() for w in self.workers]
        except KeyboardInterrupt:
            _logger.info("Quitting running due to KeyboardInterrupt")
            self.end()
        except Exception as e:
            print("%s: [%s] %s" % (self, type(e), e))
            self.end()
            raise

    def end(self):
        """Notify workers to end."""
        [w.end() for w in self.workers]

    def run(self):
        """Run workers and wait to finish."""
        self.start()
        self.join()
