# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""Test ``sfini.execution._execution``."""

from sfini.execution import _execution as tscr
import pytest
from unittest import mock
import sfini
import datetime
import json
from sfini.execution import history


@pytest.fixture
def session():
    """AWS session mock."""
    return mock.MagicMock(autospec=sfini.AWSSession)


class TestExecution:
    """Test ``sfini.execution._execution.Execution``."""
    @pytest.fixture
    def eg_input(self):
        """Example execution input."""
        return {"a": 42, "b": "bla", "c": {"foo": [1, 2], "bar": None}}

    @pytest.fixture
    def execution(self, session, eg_input):
        """An example Execution instance."""
        return tscr.Execution(
            "spam",
            "bla-sm:arn",
            eg_input,
            arn="spam:arn",
            session=session)

    def test_init(self, execution, session, eg_input):
        """Execution initialisation."""
        assert execution.name == "spam"
        assert execution.state_machine_arn == "bla-sm:arn"
        assert execution.execution_input == eg_input
        assert execution.session is session

    class TestStr:
        """Execution stringification."""
        def test_no_status(self, execution):
            """Execution status is unknown."""
            res = str(execution)
            assert "spam" in res

        def test_with_status(self, execution):
            """Execution status is known."""
            execution._status = "SUCCEEDED"
            res = str(execution)
            assert "spam" in res
            assert "SUCCEEDED" in res

    class TestRepr:
        """Execution string representation."""
        def test_with_arn_container_input(self, execution, session):
            """ARN provided and execution input is a container."""
            execution.execution_input = {"a": 42, "b": "bla", "c": [1, 2] * 20}
            exp_pref = "Execution("
            exp_pos = "'spam', 'bla-sm:arn', len(execution_input)=3"
            exp_kw_a = ", arn='spam:arn', session=%r)" % session
            exp_kw_b = ", session=%r, arn='spam:arn')" % session
            exp_a = exp_pref + exp_pos + exp_kw_a
            exp_b = exp_pref + exp_pos + exp_kw_b
            res = repr(execution)
            assert res in (exp_a, exp_b)

        def test_no_arn_container_input(self, execution, session):
            """ARN provided and execution input is a container."""
            execution.execution_input = {"a": 42, "b": "bla", "c": [1, 2] * 20}
            execution.arn = None
            exp_pref = "Execution("
            exp_pos = "'spam', 'bla-sm:arn', len(execution_input)=3"
            exp_kw = ", session=%r)" % session
            exp = exp_pref + exp_pos + exp_kw
            res = repr(execution)
            assert res == exp

        def test_with_arn_scalar_input(self, execution, session):
            """ARN provided and execution input is a scalar."""
            execution.execution_input = 42
            exp_pref = "Execution("
            exp_pos = "'spam', 'bla-sm:arn'"
            exp_kw_1 = "execution_input=42"
            exp_kw_2 = "arn='spam:arn'"
            exp_kw_3 = "session=%r" % session
            exp_kws = [
                ", " + exp_kw_1 + ", " + exp_kw_2 + ", " + exp_kw_3 + ")",
                ", " + exp_kw_1 + ", " + exp_kw_3 + ", " + exp_kw_2 + ")",
                ", " + exp_kw_2 + ", " + exp_kw_1 + ", " + exp_kw_3 + ")",
                ", " + exp_kw_2 + ", " + exp_kw_3 + ", " + exp_kw_1 + ")",
                ", " + exp_kw_3 + ", " + exp_kw_1 + ", " + exp_kw_2 + ")",
                ", " + exp_kw_3 + ", " + exp_kw_2 + ", " + exp_kw_1 + ")"]
            exps = [exp_pref + exp_pos + exp_kw for exp_kw in exp_kws]
            res = repr(execution)
            assert res in exps

    def test_from_arn(self, session):
        """Construction of Execution by querying AWS."""
        # Setup environment
        now = datetime.datetime.now()
        input_ = {"a": 42, "b": "bla", "c": {"foo": [1, 2], "bar": None}}
        output = {"foo": [1, 2], "bar": None}
        resp = {
            "executionArn": "spam:arn",
            "stateMachineArn": "bla-sm:arn",
            "name": "spam",
            "status": "SUCCEEDED",
            "startDate": now - datetime.timedelta(hours=1),
            "stopDate": now - datetime.timedelta(minutes=50),
            "input": json.dumps(input_),
            "output": json.dumps(output)}
        session.sfn.describe_execution.return_value = resp

        # Build input
        arn = "spam:arn"

        # Run function
        res = tscr.Execution.from_arn(arn, session=session)

        # Check result
        assert isinstance(res, tscr.Execution)
        assert res.name == "spam"
        assert res.state_machine_arn == "bla-sm:arn"
        assert res.execution_input == input_
        assert res.arn == "spam:arn"
        assert res.session is session
        assert res._status == "SUCCEEDED"
        assert res._start_date == now - datetime.timedelta(hours=1)
        assert res._stop_date == now - datetime.timedelta(minutes=50)
        assert res._output == {"foo": [1, 2], "bar": None}
        session.sfn.describe_execution.assert_called_once_with(
            executionArn="spam:arn")

    def test_from_list_item(self, session):
        """Construction of Execution after querying AWS."""
        now = datetime.datetime.now()
        item = {
            "executionArn": "spam:arn",
            "stateMachineArn": "bla-sm:arn",
            "name": "spam",
            "status": "SUCCEEDED",
            "startDate": now - datetime.timedelta(hours=1),
            "stopDate": now - datetime.timedelta(minutes=50)}

        # Run function
        res = tscr.Execution.from_list_item(item, session=session)

        # Check result
        assert isinstance(res, tscr.Execution)
        assert res.name == "spam"
        assert res.state_machine_arn == "bla-sm:arn"
        assert res.execution_input is res._not_provided
        assert res.arn == "spam:arn"
        assert res.session is session
        assert res._status == "SUCCEEDED"
        assert res._start_date == now - datetime.timedelta(hours=1)
        assert res._stop_date == now - datetime.timedelta(minutes=50)

    class TestStatus:
        """Execution status provided by AWS."""
        @pytest.mark.parametrize("status", [None, "RUNNING"])
        def test_unknown(self, execution, status):
            """Execution status is not currently known."""
            def _update():
                execution._status = "TIMED_OUT"
            execution._update = mock.Mock(side_effect=_update)
            execution._status = status
            res = execution.status
            assert res == "TIMED_OUT"
            execution._update.assert_called_once_with()

        @pytest.mark.parametrize(
            "status",
            ["SUCCEEDED", "FAILED", "ABORTED", "TIMED_OUT"])
        def test_known(self, execution, status):
            """Execution status is known."""
            execution._update = mock.Mock()
            execution._status = status
            res = execution.status
            assert res == status
            execution._update.assert_not_called()

    class TestStartTime:
        """Execution start-time provided by AWS."""
        def test_unknown(self, execution):
            """Execution start-time is not already known."""
            def _update():
                execution._start_date = now - datetime.timedelta(minutes=10)
            now = datetime.datetime.now()
            execution._update = mock.Mock(side_effect=_update)
            execution._start_date = None
            res = execution.start_date
            assert res == now - datetime.timedelta(minutes=10)
            execution._update.assert_called_once_with()

        def test_known(self, execution):
            """Execution start-time is known."""
            now = datetime.datetime.now()
            execution._update = mock.Mock()
            execution._start_date = now - datetime.timedelta(minutes=10)
            res = execution.start_date
            assert res == now - datetime.timedelta(minutes=10)
            execution._update.assert_not_called()

    class TestStopTime:
        """Execution stop-time provided by AWS."""
        def test_unknown(self, execution):
            """Execution stop-time is not already known."""
            def _update():
                execution._stop_date = now - datetime.timedelta(minutes=10)
            now = datetime.datetime.now()
            execution._update = mock.Mock(side_effect=_update)
            execution._raise_unfinished = mock.Mock()
            execution._stop_date = None
            res = execution.stop_date
            assert res == now - datetime.timedelta(minutes=10)
            execution._update.assert_called_once_with()
            execution._raise_unfinished.assert_called_once_with()

        def test_known(self, execution):
            """Execution stop-time is known."""
            now = datetime.datetime.now()
            execution._update = mock.Mock()
            execution._raise_unfinished = mock.Mock()
            execution._stop_date = now - datetime.timedelta(minutes=10)
            res = execution.stop_date
            assert res == now - datetime.timedelta(minutes=10)
            execution._update.assert_not_called()
            execution._raise_unfinished.assert_not_called()

    class TestOutput:
        """Execution output provided by AWS."""
        def test_unknown(self, execution):
            """Execution output is not already known."""
            def _update():
                execution._output = {"foo": [1, 2], "bar": None}
            execution._update = mock.Mock(side_effect=_update)
            execution._raise_unfinished = mock.Mock()
            execution._raise_on_failure = mock.Mock()
            execution._output = tscr._default
            res = execution.output
            assert res == {"foo": [1, 2], "bar": None}
            execution._update.assert_called_once_with()
            execution._raise_unfinished.assert_called_once_with()
            execution._raise_on_failure.assert_called_once_with()

        def test_known(self, execution):
            """Execution output is known."""
            execution._update = mock.Mock()
            execution._raise_unfinished = mock.Mock()
            execution._raise_on_failure = mock.Mock()
            execution._output = {"foo": [1, 2], "bar": None}
            res = execution.output
            assert res == {"foo": [1, 2], "bar": None}
            execution._update.assert_not_called()
            execution._raise_unfinished.assert_not_called()
            execution._raise_on_failure.assert_not_called()

    class TestUpdate:
        """Execution details updating by querying AWS."""
        @pytest.mark.parametrize(
            ("status", "input_"),
            [
                (None, tscr._default),
                ("RUNNING", tscr._default),
                (None, {"a": 42, "c": {"foo": [1, 2], "bar": None}}),
                ("SUCCEEDED", tscr._default)])
        def test_query(self, execution, session, status, input_):
            """A query of AWS is performed."""
            # Setup environment
            now = datetime.datetime.now()
            rinput_ = {"a": 42, "c": {"foo": [1, 2], "bar": None}}
            output = {"foo": [1, 2], "bar": None}
            resp = {
                "executionArn": "spam:arn",
                "stateMachineArn": "bla-sm:arn",
                "name": "spam",
                "status": "SUCCEEDED",
                "startDate": now - datetime.timedelta(hours=1),
                "stopDate": now - datetime.timedelta(minutes=50),
                "input": json.dumps(rinput_),
                "output": json.dumps(output)}
            session.sfn.describe_execution.return_value = resp
            execution._raise_no_arn = mock.Mock()
            execution._status = status
            execution.execution_input = input_

            # Run function
            execution._update()

            # Check result
            assert execution._status == "SUCCEEDED"
            assert execution._start_date == now - datetime.timedelta(hours=1)
            assert execution._stop_date == now - datetime.timedelta(minutes=50)
            assert execution._output == {"foo": [1, 2], "bar": None}
            session.sfn.describe_execution.assert_called_once_with(
                executionArn="spam:arn")
            execution._raise_no_arn.assert_called_once_with()

        def test_finished(self, execution, session):
            """No query of AWS is performed."""
            execution._raise_no_arn = mock.Mock()
            execution._status = "SUCCEEDED"
            execution._update()
            session.sfn.describe_execution.assert_not_called()
            execution._raise_no_arn.assert_not_called()

    class TestRaiseOnFailure:
        """Raising on execution failure."""
        @pytest.mark.parametrize("status", ["FAILED", "ABORTED", "TIMED_OUT"])
        def test_failure(self, execution, status):
            """Execution has failed."""
            execution._status = status
            with pytest.raises(RuntimeError) as e:
                execution._raise_on_failure()
            assert "spam" in str(e.value)
            assert status in str(e.value)

        @pytest.mark.parametrize("status", ["RUNNING", "SUCCEEDED"])
        def test_not_failure(self, execution, status):
            """Execution has not failed."""
            execution._status = status
            execution._raise_on_failure()

    class TestRaiseUnfinished:
        """Raising when execution is unfinished."""
        def test_unfinished(self, execution):
            """Execution hasn't finished."""
            execution._status = "RUNNING"
            with pytest.raises(RuntimeError) as e:
                execution._raise_unfinished()
            assert "spam" in str(e.value)
            assert "finish" in str(e.value)

        @pytest.mark.parametrize(
            "status",
            ["FAILED", "ABORTED", "TIMED_OUT", "SUCCEEDED"])
        def test_finished(self, execution, status):
            """Execution has finished."""
            execution._status = status
            execution._raise_unfinished()

    class TestRaiseNoArn:
        """Raising when no ARN is provided to execution."""
        def test_no_arn(self, execution):
            """Execution has no associated ARN."""
            execution.arn = None
            with pytest.raises(RuntimeError) as e:
                execution._raise_no_arn()
            assert "ARN" in str(e.value)
            assert "spam" in str(e.value)

        def test_finished(self, execution):
            """Execution has finished."""
            execution._raise_no_arn()

    def test_start(self, execution, session, eg_input):
        """Execution starting."""
        # Setup environment
        now = datetime.datetime.now()
        resp = {"executionArn": "spam:arn", "startDate": now}
        session.sfn.start_execution.return_value = resp
        execution.arn = None

        # Run function
        execution.start()

        # Check result
        assert execution.arn == "spam:arn"
        assert execution._start_date == now
        assert execution._status == "RUNNING"
        session.sfn.start_execution.assert_called_once_with(
            stateMachineArn="bla-sm:arn",
            name="spam",
            input=mock.ANY)
        res_se_call = session.sfn.start_execution.call_args_list[0]
        res_input_str = res_se_call[1]["input"]
        assert json.loads(res_input_str) == eg_input

    def test_start_default_input(self, execution, session):
        """Execution starting."""
        # Setup environment
        now = datetime.datetime.now()
        resp = {"executionArn": "spam:arn", "startDate": now}
        session.sfn.start_execution.return_value = resp
        execution.arn = None
        execution.execution_input = tscr._default

        # Run function
        execution.start()

        # Check result
        assert execution.arn == "spam:arn"
        assert execution._start_date == now
        assert execution._status == "RUNNING"
        session.sfn.start_execution.assert_called_once_with(
            stateMachineArn="bla-sm:arn",
            name="spam",
            input="{}")
        assert execution.execution_input == {}

    class TestWait:
        """Waiting on execution to finish."""
        @pytest.mark.timeout(1.0)
        def test_running(self, execution):
            """Execution is running."""
            # Setup environment
            _shared = {"j": 0}

            def _update():
                if _shared["j"] > 3:
                    execution._status = "FAILED"
                    return
                execution._status = "RUNNING"
                _shared["j"] += 1

            execution._update = mock.Mock(side_effect=_update)
            execution._raise_on_failure = mock.Mock()
            execution._wait_sleep_time = 0.01

            # Build expectation
            exp_ud_calls = [mock.call() for _ in range(5)]

            # Run function
            execution.wait()

            # Check result
            assert execution._update.call_args_list == exp_ud_calls
            execution._raise_on_failure.assert_called_once_with()

        @pytest.mark.timeout(1.0)
        def test_no_raise_on_failure(self, execution):
            """Execution is running, then doesn't raise on failure."""
            # Setup environment
            _shared = {"j": 0}

            def _update():
                if _shared["j"] > 3:
                    execution._status = "FAILED"
                    return
                execution._status = "RUNNING"
                _shared["j"] += 1

            execution._update = mock.Mock(side_effect=_update)
            execution._raise_on_failure = mock.Mock()
            execution._wait_sleep_time = 0.01

            # Build expectation
            exp_ud_calls = [mock.call() for _ in range(5)]

            # Run function
            execution.wait(raise_on_failure=False)

            # Check result
            assert execution._update.call_args_list == exp_ud_calls
            execution._raise_on_failure.assert_not_called()

        @pytest.mark.timeout(1.0)
        def test_timeout(self, execution):
            """Execution is running, and doesn't finish before time-out."""
            # Setup environment
            _shared = {"j": 0}

            def _update():
                if _shared["j"] > 3:
                    execution._status = "FAILED"
                    return
                execution._status = "RUNNING"
                _shared["j"] += 1

            execution._update = mock.Mock(side_effect=_update)
            execution._raise_on_failure = mock.Mock()
            execution._wait_sleep_time = 0.01

            # Build expectation
            exp_ud_calls = [mock.call() for _ in range(3)]

            # Run function
            with pytest.raises(RuntimeError) as e:
                execution.wait(timeout=0.02)
            assert "imeout" in str(e.value) or "ime-out" in str(e.value)
            assert "spam" in str(e.value)

            # Check result
            assert execution._update.call_args_list == exp_ud_calls
            execution._raise_on_failure.assert_not_called()

        @pytest.mark.timeout(1.0)
        def test_finished(self, execution):
            """Execution is finished, then doesn't raise on failure."""
            # Setup environment
            execution._update = mock.Mock()
            execution._raise_on_failure = mock.Mock()
            execution._wait_sleep_time = 0.01
            execution._status = "SUCCEEDED"

            # Run function
            execution.wait(raise_on_failure=False)

            # Check result
            execution._update.assert_called_once_with()
            execution._raise_on_failure.assert_not_called()

    @pytest.mark.parametrize(
        ("kwargs", "exp_kwargs"),
        [
            ({}, {}),
            ({"error_code": "SpamError"}, {"error": "SpamError"}),
            ({"details": "A spam occured"}, {"cause": "A spam occured"}),
            (
                {"error_code": "SpamError", "details": "A spam occured"},
                {"error": "SpamError", "cause": "A spam occured"})])
    def test_stop(self, execution, session, kwargs, exp_kwargs):
        """Execution stopping."""
        # Setup environment
        now = datetime.datetime.now()
        resp = {"stopDate": now}
        session.sfn.stop_execution.return_value = resp
        execution._raise_no_arn = mock.Mock()

        # Run function
        execution.stop(**kwargs)

        # Check result
        assert execution._stop_date == now
        session.sfn.stop_execution.assert_called_once_with(
            executionArn="spam:arn",
            **exp_kwargs)
        execution._raise_no_arn.assert_called_once_with()

    def test_get_history(self, execution, session):
        """Execution history querying."""
        # Setup environment
        resp = {"events": [{"id": j} for j in range(4)]}
        session.sfn.get_execution_history.return_value = resp
        events = [mock.Mock(spec=history.Event) for _ in range(4)]
        ph_mock = mock.Mock(return_value=events)
        execution._raise_no_arn = mock.Mock()

        # Run function
        with mock.patch.object(history, "parse_history", ph_mock):
            res = execution.get_history()

        # Check result
        assert res == events
        ph_mock.assert_called_once_with([{"id": j} for j in range(4)])
        session.sfn.get_execution_history.assert_called_once_with(
            executionArn="spam:arn")
        execution._raise_no_arn.assert_called_once_with()

    @pytest.mark.parametrize(
        ("output", "exp_suff"),
        [
            (
                {"foo": [1, 2], "bar": None},
                "\nOutput: {\"foo\": [1, 2], \"bar\": null}"),
            (tscr._default, "")])
    def test_format_history(self, execution, output, exp_suff):
        """Execution history formatting."""
        # Setup environment
        class Event:
            def __init__(self, name, details_str):
                self.name = name
                self.details_str = details_str

            def __str__(self):
                return self.name

        events = [
            Event("ev0", "Event details 0"),
            Event("ev1", ""),
            Event("ev2", "Event details 2"),
            Event("ev3", "Event details 3"),
            Event("ev4", "")]
        execution.get_history = mock.Mock(return_value=events)
        execution._update = mock.Mock()
        execution._output = output

        # Build expectation
        exp = (
            "ev0:\n"
            "  Event details 0\n"
            "ev1\n"
            "ev2:\n"
            "  Event details 2\n"
            "ev3:\n"
            "  Event details 3\n"
            "ev4")
        exp += exp_suff

        # Test function
        res = execution.format_history()

        # Check result
        assert res == exp
        execution.get_history.assert_called_once_with()
        execution._update.assert_called_once_with()
