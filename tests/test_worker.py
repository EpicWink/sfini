"""Test ``sfini.worker``."""

from sfini import worker as tscr
import pytest
from unittest import mock
import sfini
from sfini import _util as sfini_util
import json
from botocore import exceptions as bc_exc
import threading


@pytest.fixture
def activity_mock():
    act = mock.MagicMock(spec=sfini.activity.CallableActivity)
    act.name = "spamActivity"
    return act


@pytest.fixture
def session_mock():
    return mock.MagicMock(spec=sfini.AWSSession)


class TestWorkerCancel:
    """Test ``sfini.worker.WorkerCancel``."""
    def test_raising(self):
        exp_msg = (
            "Activity execution cancelled by user. "
            "This could be due to a `KeyboardInterrupt` during execution, "
            "or the worker was killed during task polling.")
        with pytest.raises(KeyboardInterrupt) as e:
            raise tscr.WorkerCancel()
        assert str(e.value) == exp_msg


class TestTaskExecution:
    """Test ``sfini.worker.TaskExecution``."""
    @pytest.fixture
    def task(self, activity_mock, session_mock):
        """An example TaskExecution instance."""
        return tscr.TaskExecution(
            activity_mock,
            "taskToken",
            {"a": 42, "b": "spam", "c": {"foo": [1, 2], "bar": None}},
            session=session_mock)

    def test_init(self, task, activity_mock, session_mock):
        """TaskExecution initialisation."""
        exp_input = {"a": 42, "b": "spam", "c": {"foo": [1, 2], "bar": None}}
        assert task.activity is activity_mock
        assert task.task_token == "taskToken"
        assert task.task_input == exp_input
        assert task.session is session_mock
        assert task._request_stop is False

    def test_str(self, task, activity_mock):
        """TaskExecution stringification."""
        res = str(task)
        assert "spamActivity" in res
        assert "taskToken" in res

    def test_repr(self):
        """TaskExecution string representation."""
        assert tscr.TaskExecution.__repr__ is sfini_util.easy_repr

    class TestSend:
        """AWS task status post."""
        def test_not_stopped(self, task):
            """Task is not known to stopped."""
            send_fn = mock.Mock()
            kw = {"a": 42, "b": "spam"}
            task._send(send_fn, **kw)
            send_fn.assert_called_once_with(taskToken="taskToken", **kw)

        def test_stopped(self, task):
            """Task is stopped."""
            task._request_stop = True
            send_fn = mock.Mock()
            kw = {"a": 42, "b": "spam"}
            task._send(send_fn, **kw)
            send_fn.assert_not_called()

    def test_report_exception(self, task, session_mock):
        """Exception reporting."""
        task._send = mock.Mock()
        try:
            raise ValueError("spambla42")
        except ValueError as e:
            exc = e
        task._report_exception(exc)
        assert task._request_stop is True
        task._send.assert_called_once_with(
            session_mock.sfn.send_task_failure,
            error="ValueError",
            cause=mock.ANY)
        res_send_call = task._send.call_args_list[0]
        assert "ValueError: spambla42" in res_send_call[1]["cause"]

    def test_report_cancelled(self, task, session_mock):
        """Task cancelling."""
        task._send = mock.Mock()
        task.report_cancelled()
        assert task._request_stop is True
        task._send.assert_called_once_with(
            session_mock.sfn.send_task_failure,
            error="WorkerCancel",
            cause=mock.ANY)
        res_send_call = task._send.call_args_list[0]
        assert res_send_call[1]["cause"] == str(tscr.WorkerCancel())

    def test_report_success(self, task, session_mock):
        """Task finish reporting."""
        task._send = mock.Mock()
        res = {"a": 42, "b": "spam", "c": {"foo": [1, 2], "bar": None}}
        task._report_success(res)
        assert task._request_stop is True
        task._send.assert_called_once_with(
            session_mock.sfn.send_task_success,
            output=mock.ANY)
        res_send_call = task._send.call_args_list[0]
        res_output = json.loads(res_send_call[1]["output"])
        assert res_output == res

    class TestSendHeartbeat:
        """Heartbeat sending."""
        def test_success(self, task, session_mock):
            """Successful heartbeat."""
            task._send = mock.Mock()
            task._send_heartbeat()
            assert task._request_stop is False

        def test_timed_out(self, task, session_mock):
            """Task timed-out."""
            exc = bc_exc.ClientError(
                {"Error": {"Code": "TaskTimedOut", "Message": "spambla42"}},
                "sendTaskHeartbeat")
            task._send = mock.Mock(side_effect=exc)
            task._send_heartbeat()
            assert task._request_stop is True

        def test_no_exist(self, task, session_mock):
            """Task doesn't exist."""
            exc = bc_exc.ClientError(
                {"Error": {"Code": "TaskDoesNotExist"}},
                "sendTaskHeartbeat")
            task._send = mock.Mock(side_effect=exc)
            with pytest.raises(bc_exc.ClientError) as e:
                task._send_heartbeat()
            assert e.value is exc
            assert task._request_stop is True

    @pytest.mark.timeout(1.0)
    def test_heartbeat(self, task, activity_mock):
        """Hearbeat sending loop."""
        _shared = {"j": 0}

        def _send_heartbeat():
            if _shared["j"] > 3:
                task._request_stop = True
            _shared["j"] += 1

        task._send_heartbeat = mock.Mock(side_effect=_send_heartbeat)
        activity_mock.heartbeat = 0.1
        exp_sh_calls = [mock.call() for _ in range(5)]
        task._heartbeat()
        assert task._send_heartbeat.call_args_list == exp_sh_calls

    class TestRun:
        """Task running."""
        def test_success(self, task, activity_mock):
            """Task succeeds."""
            task._heartbeat_thread = mock.Mock(spec=threading.Thread)
            task.report_cancelled = mock.Mock()
            task._report_exception = mock.Mock()
            task._report_success = mock.Mock()
            res = {"a": 42, "b": "spam", "c": {"foo": [1, 2], "bar": None}}
            activity_mock.call_with.return_value = res
            task.run()
            task.report_cancelled.assert_not_called()
            task._report_exception.assert_not_called()
            task._report_success.assert_called_once_with(res)

        def test_fail(self, task, activity_mock):
            """Task fails."""
            task._heartbeat_thread = mock.Mock(spec=threading.Thread)
            task.report_cancelled = mock.Mock()
            task._report_exception = mock.Mock()
            task._report_success = mock.Mock()
            exc = ValueError("spambla42")
            activity_mock.call_with.side_effect = exc
            task.run()
            task.report_cancelled.assert_not_called()
            task._report_exception.assert_called_once_with(exc)
            task._report_success.assert_not_called()

        def test_cancelled(self, task, activity_mock):
            """Task is interrupted."""
            task._heartbeat_thread = mock.Mock(spec=threading.Thread)
            task.report_cancelled = mock.Mock()
            task._report_exception = mock.Mock()
            task._report_success = mock.Mock()
            activity_mock.call_with.side_effect = KeyboardInterrupt()
            task.run()
            task.report_cancelled.assert_called_once_with()
            task._report_exception.assert_not_called()
            task._report_success.assert_not_called()


class TestWorker:
    """Test ``sfini.worker.Worker``."""
    @pytest.fixture
    def worker(self, activity_mock, session_mock):
        """An example Worker instance."""
        return tscr.Worker(activity_mock, name="spam", session=session_mock)

    def test_init(self, worker, activity_mock, session_mock):
        """Worker initialisation."""
        assert worker.activity is activity_mock
        assert worker.name == "spam"
        assert worker.session is session_mock
        assert worker._request_finish is False
        assert worker._exc is None

    def test_str(self, worker):
        """Worker stringification."""
        res = str(worker)
        assert "spam" in res
        assert "spamActivity" in res

    class TestExecuteOn:
        """Task execution."""
        def test_not_finished(self, worker, activity_mock, session_mock):
            """Activity isn't finishing."""
            te_mock = mock.Mock(spec=tscr.TaskExecution)
            worker._task_execution_class = mock.Mock(return_value=te_mock)
            task_input = {"a": 42, "b": "bla"}
            task_token = "taskToken"
            worker._execute_on(task_input, task_token)
            worker._task_execution_class.assert_called_once_with(
                activity_mock,
                task_token,
                task_input,
                session=session_mock)
            te_mock.run.assert_called_once_with()
            te_mock.report_cancelled.assert_not_called()

        def test_finished(self, worker, activity_mock, session_mock):
            """Activity is finishing."""
            worker._request_finish = True
            te_mock = mock.Mock(spec=tscr.TaskExecution)
            worker._task_execution_class = mock.Mock(return_value=te_mock)
            task_input = {"a": 42, "b": "bla"}
            task_token = "taskToken"
            worker._execute_on(task_input, task_token)
            worker._task_execution_class.assert_called_once_with(
                activity_mock,
                task_token,
                task_input,
                session=session_mock)
            te_mock.run.assert_not_called()
            te_mock.report_cancelled.assert_called_once_with()

    @pytest.mark.timeout(1.0)
    def test_poll_and_execute(self, worker, activity_mock, session_mock):
        """Polling and executing."""
        # Setup environment
        _shared = {"j": 0}

        def get_activity_task(activityArn, workerName):
            if _shared["j"] > 3:
                worker._request_finish = True
            _shared["j"] += 1
            if _shared["j"] == 2:
                return {
                    "taskToken": "taskToken1",
                    "input": '{"a": 42, "b": "bla"}'}
            if _shared["j"] == 3:
                return {
                    "taskToken": "taskToken2",
                    "input": '{"foo": [1, 2], "bar": null}'}
            return {}

        activity_mock.arn = "spamActivity:arn"
        gat_mock = session_mock.sfn.get_activity_task
        gat_mock.side_effect = get_activity_task
        worker._execute_on = mock.Mock()

        # Build expectation
        exp_gat_calls = [
            mock.call(activityArn="spamActivity:arn", workerName="spam")
            for _ in range(5)]
        exp_eo_calls = [
            mock.call({"a": 42, "b": "bla"}, "taskToken1"),
            mock.call({"foo": [1, 2], "bar": None}, "taskToken2")]

        # Run function
        worker._poll_and_execute()

        # Check result
        assert gat_mock.call_args_list == exp_gat_calls
        assert worker._execute_on.call_args_list == exp_eo_calls

    class TestWorker:
        """Worker running."""
        def test_succeeds(self, worker):
            """Worker finishes polling successfully."""
            worker._poll_and_execute = mock.Mock()
            worker._worker()
            assert worker._exc is None
            assert worker._request_finish is False
            worker._poll_and_execute.assert_called_once_with()

        def test_fails(self, worker):
            """Worker finishes polling successfully."""
            exc = ValueError("spambla42")
            worker._poll_and_execute = mock.Mock(side_effect=exc)
            worker._worker()
            assert worker._exc == exc
            assert worker._request_finish is True
            worker._poll_and_execute.assert_called_once_with()

    class TestStart:
        """Worker starting."""
        def test_callable_activity(self, worker):
            """Activity is callable (ie has implementer)."""
            worker._poller = mock.Mock(spec=threading.Thread)
            worker.start()
            worker._poller.start.assert_called_once_with()

        def test_not_callable_activity(self, worker):
            """Activity is not callable (ie has no implementer)."""
            worker.activity = mock.Mock(spec=sfini.activity.Activity)
            worker._poller = mock.Mock(spec=threading.Thread)
            with pytest.raises(TypeError) as e:
                worker.start()
            assert str(worker.activity) in str(e.value)
            worker._poller.start.assert_not_called()

    class TestJoin:
        """Waiting for worker to finish."""
        def test_succeeds(self, worker):
            """Worker finishes successfully."""
            worker._poller = mock.Mock(spec=threading.Thread)
            worker.join()
            assert worker._request_finish is False
            worker._poller.join.assert_called_once_with()

        def test_cancelled(self, worker):
            """Worker is cancelled by main thread."""
            worker._poller = mock.Mock(spec=threading.Thread)
            exc = KeyboardInterrupt()
            worker._poller.join.side_effect = exc
            worker.join()
            assert worker._request_finish is True
            worker._poller.join.assert_called_once_with()

        def test_error(self, worker):
            """Unknown failure on worker joining."""
            worker._poller = mock.Mock(spec=threading.Thread)
            exc = ValueError("spambla42")
            worker._poller.join.side_effect = exc
            with pytest.raises(ValueError) as e:
                worker.join()
            assert e.value == exc
            assert worker._request_finish is True
            worker._poller.join.assert_called_once_with()

        def test_poller_error(self, worker):
            """Failure during polling/execution."""
            worker._poller = mock.Mock(spec=threading.Thread)
            worker._exc = ValueError("spambla42")
            with pytest.raises(ValueError) as e:
                worker.join()
            assert e.value == worker._exc
            worker._poller.join.assert_called_once_with()

    def test_end(self, worker):
        """Request to stop."""
        worker.end()
        assert worker._request_finish is True

    def test_run(self, worker):
        """Worker start and wait."""
        worker.start = mock.Mock()
        worker.join = mock.Mock()
        worker.run()
        worker.start.assert_called_once_with()
        worker.join.assert_called_once_with()
