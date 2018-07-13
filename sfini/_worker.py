# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/12

"""Task runner."""

import json
import uuid
import time
import socket
import threading
import logging as lg

from . import _util

_logger = lg.getLogger(__name__)
_host_name = socket.getfqdn(socket.gethostname())


class Worker:  # TODO: unit-test
    """Worker to poll to execute tasks.

    Args:
        state_machine (sfini._state_machine.StateMachine): state-machine
            containing tasks to poll
        tasks (list[Task]): tasks to poll and execute
        name (str): name of worker, used for identification
        session (_util.Session): session to use for AWS communication
    """

    def __init__(self, state_machine, tasks, name=None, *, session=None):
        self.state_machine = state_machine
        self.tasks = tasks
        self.name = name or "%s-%s" % (_host_name, uuid.uuid4())
        self.session = session or state_machine.session

        self._task_runners = None
        self._task_runner_threads = None

    def end(self):
        """End polling."""
        for runner in self._task_runners:
            runner.end()

    def _join(self):
        try:
            for thread in self._task_runner_threads:
                thread.join()
        except KeyboardInterrupt:
            self.end()

    def run(self, block=True):
        """Run worker to poll for and execute specified tasks.

        Args:
            block (bool): run worker synchronously
        """

        self._task_runners = []
        self._task_runner_threads = []
        for task in self.tasks:
            runner = _TaskRunner(task, self.name, session=self.session)
            thread = threading.Thread(target=runner.poll)
            thread.start()
            self._task_runners.append(runner)
            self._task_runner_threads.append(thread)

        if block:
            self._join()

        raise NotImplementedError


class _TaskRunner:  # TODO: unit-test
    """Worker to poll for task executions.

    Args:
        task (Task): task to poll and execute
        worker_name (str): name of worker, used for identification
        session (_util.AWSSession): session to communicate to AWS with
    """

    def __init__(self, task, worker_name, *, session=None):
        self.task = task
        self.worker_name = worker_name
        self.session = session or _util.AWSSession()
        self._task_executions = []
        self._task_execution_threads = []
        self._end = False

    def end(self):
        """End polling."""
        self._end = True
        self.stop_executions()

    def stop_executions(self):
        """Tell current executions to stop."""
        [e.request_stop() for e in self._task_executions]

    def _execute(self, task_token, task_input):
        execution = _TaskExecution(
            self.task,
            task_token,
            task_input,
            session=self.session)
        thread = threading.Thread(target=execution.run)
        thread.start()
        self._task_executions.append(execution)
        self._task_execution_threads.append(thread)

    def _poll(self):
        while True:
            if self._end:
                break
            resp = self.session.sfn.get_activity_task(
                activityArn=self.task.arn,
                workerName=self.worker_name)
            if resp["taskToken"] is not None:
                self._execute(resp["taskToken"], json.loads(resp["input"]))

    def poll(self):
        """Poll for executions."""
        try:
            self._poll()
        except KeyboardInterrupt:
            _logger.info("Waiting for executions to finish")
            self.stop_executions()
            [t.join() for t in self._task_execution_threads]


class _TaskExecution:  # TODO: unit-test
    """Execute a task.

    Args:
        task (Task): task to poll and execute
        task_token (str): task token for execution identification
        session (_util.AWSSession): session to communicate to AWS with
    """

    def __init__(self, task, task_token, task_input, *, session=None):
        self.task = task
        self.task_token = task_token
        self.task_input = task_input
        self.session = session or _util.AWSSession()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat)
        self._request_stop = False

    def request_stop(self):
        """Tell execution to stop communicating with AWS.

        Returns:
            bool: ``True`` if communication was already stopped
        """

        if self._request_stop:
            return False
        self._request_stop = True
        return True

    def _send_heartbeat(self):
        self.session.sfn.send_task_heartbeat(taskToken=self.task_token)

    def _heartbeat(self):
        heartbeat = self.task.heartbeat
        heartbeat = min(max(heartbeat - 5.0, 1.0), heartbeat)
        while True:
            t = time.time()
            if self._request_stop:
                break
            self._send_heartbeat()
            time.sleep(heartbeat - (time.time() - t))

    def _send_failure(self, exc):
        if self._request_stop:
            _logger.warning("Skipping sending failure as we're quitting")
            return
        self._request_stop = True
        self.session.sfn.send_task_failure(
            taskToken=self.task_token,
            error=type(exc).__name__,
            cause=str(exc))

    def _send_success(self, res):
        if self._request_stop:
            _logger.warning("Skipping sending failure as we're quitting")
            return
        self._request_stop = True
        self.session.sfn.send_task_success(
            taskToken=self.task_token,
            output=json.dumps(res))

    def run(self):
        """Run task."""
        self._heartbeat_thread.start()
        try:
            kwargs = self.task.get_input_from(self.task_input)
        except KeyError as e:
            self._send_failure(e)
            return
        try:
            res = self.task.activity.fn(**kwargs)
        except Exception as e:
            self._send_failure(e)
            return
        self._send_success(res)
