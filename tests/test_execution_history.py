"""Test ``sfini.execution.history``."""

from sfini.execution import history as tscr
import pytest
from unittest import mock
import datetime


@pytest.fixture
def timestamp():
    """An example time-stamp."""
    return datetime.datetime.utcnow() - datetime.timedelta(minutes=42)


@pytest.fixture
def input_():
    """Example input."""
    return {"a": 42, "b": "spam", "c": {"foo": [1, 2], "bar": None}}


class TestEvent:
    """Test ``sfini.execution.history.Event``."""
    @pytest.fixture
    def event(self, timestamp):
        """An example Event instance."""
        return tscr.Event(timestamp, "BlaSpammed", 2, previous_event_id=1)

    def test_init(self, event, timestamp):
        """Event initalisation."""
        assert event.timestamp == timestamp
        assert event.event_type == "BlaSpammed"
        assert event.event_id == 2
        assert event.previous_event_id == 1

    def test_str(self, event, timestamp):
        """Event stringification."""
        res = str(event)
        assert "BlaSpammed" in res
        assert str(timestamp) in res
        assert "2" in res[:res.index(str(timestamp))]

    class TestRepr:
        """Event string representation."""
        def test_with_prev(self, event, timestamp):
            """Event has preceding event."""
            exp = f"Event({timestamp!r}, 'BlaSpammed', 2, previous_event_id=1)"
            assert repr(event) == exp

        def test_no_prev(self, event, timestamp):
            """Event has no preceding event."""
            event.previous_event_id = None
            exp = f"Event({timestamp!r}, 'BlaSpammed', 2)"
            assert repr(event) == exp

    class TestGetArgs:
        """Getting instantiation arguments from AWS API HistoryEvent."""
        def test_no_details(self, timestamp):
            """No event details."""
            # Setup environment
            type_keys = {}

            # Build input
            history_event = {
                "timestamp": timestamp,
                "type": "BlaSpammed",
                "id": 2,
                "previousEventId": 1}

            # Build expectation
            exp_args = (timestamp, "BlaSpammed", 2)
            exp_kwargs = {"previous_event_id": 1}
            exp_details = {}

            # Run function
            with mock.patch.object(tscr, "_type_keys", type_keys):
                res = tscr.Event._get_args(history_event)

            # Check result
            res_args, res_kwargs, res_details = res
            assert res_args == exp_args
            assert res_kwargs == exp_kwargs
            assert res_details == exp_details

        def test_with_details(self, timestamp):
            """Event comes with details."""
            # Setup environment
            type_keys = {"BlaSpammed": "BlaSpammedEventDetails"}

            # Build input
            history_event = {
                "timestamp": timestamp,
                "type": "BlaSpammed",
                "id": 2,
                "previousEventId": 1,
                "BlaSpammedEventDetails": {"a": 42, "b": "spam"}}

            # Build expectation
            exp_args = (timestamp, "BlaSpammed", 2)
            exp_kwargs = {"previous_event_id": 1}
            exp_details = {"a": 42, "b": "spam"}

            # Run function
            with mock.patch.object(tscr, "_type_keys", type_keys):
                res = tscr.Event._get_args(history_event)

            # Check result
            res_args, res_kwargs, res_details = res
            assert res_args == exp_args
            assert res_kwargs == exp_kwargs
            assert res_details == exp_details

        def test_no_previous_event(self, timestamp):
            """Event has no precedent."""
            # Setup environment
            type_keys = {}

            # Build input
            history_event = {
                "timestamp": timestamp,
                "type": "BlaSpammed",
                "id": 2}

            # Build expectation
            exp_args = (timestamp, "BlaSpammed", 2)
            exp_kwargs = {}
            exp_details = {}

            # Run function
            with mock.patch.object(tscr, "_type_keys", type_keys):
                res = tscr.Event._get_args(history_event)

            # Check result
            res_args, res_kwargs, res_details = res
            assert res_args == exp_args
            assert res_kwargs == exp_kwargs
            assert res_details == exp_details

    def test_from_history_event(self, timestamp):
        """Instantiation from AWS API history event."""
        # Setup environment
        args = (timestamp, "BlaSpammed", 2)
        kwargs = {"previous_event_id": 1}
        details = {"a": 42, "b": "spam"}
        ga_mock = mock.Mock(return_value=(args, kwargs, details))

        # Build input
        history_event = {"timestamp": timestamp, "type": "BlaSpammed", "id": 2}

        # Run function
        with mock.patch.object(tscr.Event, "_get_args", ga_mock):
            res = tscr.Event.from_history_event(history_event)

        # Check result
        assert isinstance(res, tscr.Event)
        assert res.timestamp == timestamp
        assert res.event_type == "BlaSpammed"
        assert res.event_id == 2
        assert res.previous_event_id == 1
        ga_mock.assert_called_once_with(history_event)

    def test_details_str(self, event):
        """Event details formatting."""
        assert not event.details_str


class TestFailed:
    """Test ``sfini.execution.history.Failed``."""
    @pytest.fixture
    def event(self, timestamp):
        """An example Failed instance."""
        return tscr.Failed(
            timestamp,
            "BlaSpammed",
            2,
            previous_event_id=1,
            error="spamError",
            cause="a spam occurred")

    def test_init(self, event, timestamp):
        """Failed initalisation."""
        assert event.timestamp == timestamp
        assert event.event_type == "BlaSpammed"
        assert event.event_id == 2
        assert event.previous_event_id == 1
        assert event.error == "spamError"
        assert event.cause == "a spam occurred"

    @pytest.mark.parametrize(
        ("details", "exp_kwargs"),
        [
            ({}, {}),
            ({"error": "spamError"}, {"error": "spamError"}),
            ({"cause": "a spam occurred"}, {"cause": "a spam occurred"}),
            (
                {"error": "spamError", "cause": "a spam occurred"},
                {"error": "spamError", "cause": "a spam occurred"})])
    def test_get_args(self, timestamp, details, exp_kwargs):
        """Getting instantiation arguments from AWS API HistoryEvent."""
        # Setup environment
        type_keys = {"BlaFailed": "BlaFailedEventDetails"}

        # Build input
        history_event = {
            "timestamp": timestamp,
            "type": "BlaFailed",
            "id": 2,
            "previousEventId": 1,
            "BlaFailedEventDetails": details}

        # Build expectation
        exp_args = (timestamp, "BlaFailed", 2)
        exp_kwargs.update({"previous_event_id": 1})
        exp_details = details

        # Run function
        with mock.patch.object(tscr, "_type_keys", type_keys):
            res = tscr.Failed._get_args(history_event)

        # Check result
        res_args, res_kwargs, res_details = res
        assert res_args == exp_args
        assert res_kwargs == exp_kwargs
        assert res_details == exp_details

    def test_details_str(self, event):
        """Failed details formatting."""
        assert event.details_str == "error: spamError"


class TestLambdaFunctionScheduled:
    """Test ``sfini.execution.history.LambdaFunctionScheduled``."""
    @pytest.fixture
    def event(self, timestamp, input_):
        """An example LambdaFunctionScheduled instance."""
        return tscr.LambdaFunctionScheduled(
            timestamp,
            "BlaSpammed",
            2,
            "lambdaResource:arn",
            previous_event_id=1,
            task_input=input_,
            timeout=60)

    def test_init(self, event, timestamp, input_):
        """LambdaFunctionScheduled initalisation."""
        assert event.timestamp == timestamp
        assert event.event_type == "BlaSpammed"
        assert event.event_id == 2
        assert event.previous_event_id == 1
        assert event.task_input == input_
        assert event.timeout == 60

    @pytest.mark.parametrize(
        ("details", "exp_kwargs"),
        [
            ({"resource": "lambdaResource:arn"}, {}),
            (
                {"resource": "lambdaResource:arn", "input": '"spamInput"'},
                {"task_input": "spamInput"}),
            (
                {"resource": "lambdaResource:arn", "timeoutInSeconds": 60},
                {"timeout": 60}),
            (
                {
                    "resource": "lambdaResource:arn",
                    "input": '"spamInput"',
                    "timeoutInSeconds": 60},
                {"task_input": "spamInput", "timeout": 60})])
    def test_get_args(self, timestamp, details, exp_kwargs):
        """Getting instantiation arguments from AWS API HistoryEvent."""
        # Setup environment
        type_keys = {"LambdaScheduled": "LambdaScheduledEventDetails"}

        # Build input
        history_event = {
            "timestamp": timestamp,
            "type": "LambdaScheduled",
            "id": 2,
            "previousEventId": 1,
            "LambdaScheduledEventDetails": details}

        # Build expectation
        exp_args = (timestamp, "LambdaScheduled", 2, "lambdaResource:arn")
        exp_kwargs.update({"previous_event_id": 1})
        exp_details = details

        # Run function
        with mock.patch.object(tscr, "_type_keys", type_keys):
            res = tscr.LambdaFunctionScheduled._get_args(history_event)

        # Check result
        res_args, res_kwargs, res_details = res
        assert res_args == exp_args
        assert res_kwargs == exp_kwargs
        assert res_details == exp_details

    def test_details_str(self, event):
        """LambdaFunctionScheduled details formatting."""
        assert event.details_str == "resource: lambdaResource:arn"


class TestActivityScheduled:
    """Test ``sfini.execution.history.ActivityScheduled``."""
    @pytest.fixture
    def event(self, timestamp, input_):
        """An example ActivityScheduled instance."""
        return tscr.ActivityScheduled(
            timestamp,
            "BlaSpammed",
            2,
            "lambdaResource:arn",
            previous_event_id=1,
            task_input=input_,
            timeout=60,
            heartbeat=10)

    def test_init(self, event, timestamp, input_):
        """Event initalisation."""
        assert event.timestamp == timestamp
        assert event.event_type == "BlaSpammed"
        assert event.event_id == 2
        assert event.previous_event_id == 1
        assert event.task_input == input_
        assert event.timeout == 60
        assert event.heartbeat == 10

    @pytest.mark.parametrize(
        ("details", "exp_kwargs"),
        [
            (
                {
                    "resource": "lambdaResource:arn",
                    "input": '"spamInput"',
                    "timeoutInSeconds": 60},
                {"task_input": "spamInput", "timeout": 60}),
            (
                {
                    "resource": "lambdaResource:arn",
                    "input": '"spamInput"',
                    "timeoutInSeconds": 60,
                    "heartbeatInSeconds": 10},
                {"task_input": "spamInput", "timeout": 60, "heartbeat": 10})])
    def test_get_args(self, timestamp, details, exp_kwargs):
        """Getting instantiation arguments from AWS API HistoryEvent."""
        # Setup environment
        type_keys = {"ActivityScheduled": "ActivityScheduledEventDetails"}

        # Build input
        history_event = {
            "timestamp": timestamp,
            "type": "ActivityScheduled",
            "id": 2,
            "previousEventId": 1,
            "ActivityScheduledEventDetails": details}

        # Build expectation
        exp_args = (timestamp, "ActivityScheduled", 2, "lambdaResource:arn")
        exp_kwargs.update({"previous_event_id": 1})
        exp_details = details

        # Run function
        with mock.patch.object(tscr, "_type_keys", type_keys):
            res = tscr.ActivityScheduled._get_args(history_event)

        # Check result
        res_args, res_kwargs, res_details = res
        assert res_args == exp_args
        assert res_kwargs == exp_kwargs
        assert res_details == exp_details


class TestActivityStarted:
    """Test ``sfini.execution.history.ActivityStarted``."""
    @pytest.fixture
    def event(self, timestamp):
        """An example ActivityStarted instance."""
        return tscr.ActivityStarted(
            timestamp,
            "BlaSpammed",
            2,
            "spamWorker",
            previous_event_id=1)

    def test_init(self, event, timestamp):
        """ActivityStarted initalisation."""
        assert event.timestamp == timestamp
        assert event.event_type == "BlaSpammed"
        assert event.event_id == 2
        assert event.worker_name == "spamWorker"
        assert event.previous_event_id == 1

    def test_get_args(self, timestamp):
        """Getting instantiation arguments from AWS API HistoryEvent."""
        # Setup environment
        type_keys = {"ActivityStarted": "ActivityStartedEventDetails"}

        # Build input
        details = {"workerName": "spamWorker"}
        history_event = {
            "timestamp": timestamp,
            "type": "ActivityStarted",
            "id": 2,
            "previousEventId": 1,
            "ActivityStartedEventDetails": details}

        # Build expectation
        exp_args = (timestamp, "ActivityStarted", 2, "spamWorker")
        exp_kwargs = {"previous_event_id": 1}
        exp_details = details

        # Run function
        with mock.patch.object(tscr, "_type_keys", type_keys):
            res = tscr.ActivityStarted._get_args(history_event)

        # Check result
        res_args, res_kwargs, res_details = res
        assert res_args == exp_args
        assert res_kwargs == exp_kwargs
        assert res_details == exp_details

    def test_details_str(self, event):
        """ActivityStarted details formatting."""
        assert event.details_str == "worker: spamWorker"


class TestObjectSucceeded:
    """Test ``sfini.execution.history.ObjectSucceeded``."""
    @pytest.fixture
    def event(self, timestamp):
        """An example ObjectSucceeded instance."""
        return tscr.ObjectSucceeded(
            timestamp,
            "BlaSpammed",
            2,
            previous_event_id=1,
            output={"foo": [1, 2], "bar": None})

    def test_init(self, event, timestamp):
        """ObjectSucceeded initalisation."""
        assert event.timestamp == timestamp
        assert event.event_type == "BlaSpammed"
        assert event.event_id == 2
        assert event.previous_event_id == 1
        assert event.output == {"foo": [1, 2], "bar": None}

    @pytest.mark.parametrize(
        ("details", "exp_kwargs"),
        [
            ({}, {}),
            (
                {"output": '{"foo": [1, 2], "bar": null}'},
                {"output": {"foo": [1, 2], "bar": None}})])
    def test_get_args(self, timestamp, details, exp_kwargs):
        """Getting instantiation arguments from AWS API HistoryEvent."""
        # Setup environment
        type_keys = {"BlaSucceeded": "BlaSucceededEventDetails"}

        # Build input
        history_event = {
            "timestamp": timestamp,
            "type": "BlaSucceeded",
            "id": 2,
            "previousEventId": 1,
            "BlaSucceededEventDetails": details}

        # Build expectation
        exp_args = (timestamp, "BlaSucceeded", 2)
        exp_kwargs.update({"previous_event_id": 1})
        exp_details = details

        # Run function
        with mock.patch.object(tscr, "_type_keys", type_keys):
            res = tscr.ObjectSucceeded._get_args(history_event)

        # Check result
        res_args, res_kwargs, res_details = res
        assert res_args == exp_args
        assert res_kwargs == exp_kwargs
        assert res_details == exp_details


class TestExecutionStarted:
    """Test ``sfini.execution.history.ExecutionStarted``."""
    @pytest.fixture
    def event(self, timestamp, input_):
        """An example ExecutionStarted instance."""
        return tscr.ExecutionStarted(
            timestamp,
            "BlaSpammed",
            2,
            previous_event_id=1,
            execution_input=input_,
            role_arn="role:arn")

    def test_init(self, event, timestamp, input_):
        """ExecutionStarted initalisation."""
        assert event.timestamp == timestamp
        assert event.event_type == "BlaSpammed"
        assert event.event_id == 2
        assert event.previous_event_id == 1
        assert event.execution_input == input_
        assert event.role_arn == "role:arn"

    @pytest.mark.parametrize(
        ("details", "exp_kwargs"),
        [
            ({}, {}),
            ({"input": '"spamInput"'}, {"execution_input": "spamInput"}),
            ({"roleArn": "role:arn"}, {"role_arn": "role:arn"}),
            (
                {"input": '"spamInput"', "roleArn": "role:arn"},
                {"execution_input": "spamInput", "role_arn": "role:arn"})])
    def test_get_args(self, timestamp, details, exp_kwargs):
        """Getting instantiation arguments from AWS API HistoryEvent."""
        # Setup environment
        type_keys = {"ExecutionStarted": "ExecutionStartedEventDetails"}

        # Build input
        history_event = {
            "timestamp": timestamp,
            "type": "ExecutionStarted",
            "id": 2,
            "previousEventId": 1,
            "ExecutionStartedEventDetails": details}

        # Build expectation
        exp_args = (timestamp, "ExecutionStarted", 2)
        exp_kwargs.update({"previous_event_id": 1})
        exp_details = details

        # Run function
        with mock.patch.object(tscr, "_type_keys", type_keys):
            res = tscr.ExecutionStarted._get_args(history_event)

        # Check result
        res_args, res_kwargs, res_details = res
        assert res_args == exp_args
        assert res_kwargs == exp_kwargs
        assert res_details == exp_details


class TestStateEntered:
    """Test ``sfini.execution.history.StateEntered``."""
    @pytest.fixture
    def event(self, timestamp, input_):
        """An example StateEntered instance."""
        return tscr.StateEntered(
            timestamp,
            "BlaSpammed",
            2,
            "spamState",
            previous_event_id=1,
            state_input=input_)

    def test_init(self, event, timestamp, input_):
        """StateEntered initalisation."""
        assert event.timestamp == timestamp
        assert event.event_type == "BlaSpammed"
        assert event.event_id == 2
        assert event.state_name == "spamState"
        assert event.previous_event_id == 1
        assert event.state_input == input_

    @pytest.mark.parametrize(
        ("details", "exp_kwargs"),
        [
            ({"name": "spamState"}, {}),
            (
                {"name": "spamState", "input": '"spamInput"'},
                {"state_input": "spamInput"})])
    def test_get_args(self, timestamp, details, exp_kwargs):
        """Getting instantiation arguments from AWS API HistoryEvent."""
        # Setup environment
        type_keys = {"StateEntered": "StateEnteredEventDetails"}

        # Build input
        history_event = {
            "timestamp": timestamp,
            "type": "StateEntered",
            "id": 2,
            "previousEventId": 1,
            "StateEnteredEventDetails": details}

        # Build expectation
        exp_args = (timestamp, "StateEntered", 2, "spamState")
        exp_kwargs.update({"previous_event_id": 1})
        exp_details = details

        # Run function
        with mock.patch.object(tscr, "_type_keys", type_keys):
            res = tscr.StateEntered._get_args(history_event)

        # Check result
        res_args, res_kwargs, res_details = res
        assert res_args == exp_args
        assert res_kwargs == exp_kwargs
        assert res_details == exp_details

    def test_details_str(self, event):
        """StateEntered details formatting."""
        assert event.details_str == "name: spamState"


class TestStateExited:
    """Test ``sfini.execution.history.StateExited``."""
    @pytest.fixture
    def event(self, timestamp):
        """An example StateExited instance."""
        return tscr.StateExited(
            timestamp,
            "BlaSpammed",
            2,
            "spamState",
            previous_event_id=1,
            output={"foo": [1, 2], "bar": None})

    def test_init(self, event, timestamp):
        """StateExited initalisation."""
        assert event.timestamp == timestamp
        assert event.event_type == "BlaSpammed"
        assert event.event_id == 2
        assert event.state_name == "spamState"
        assert event.previous_event_id == 1
        assert event.output == {"foo": [1, 2], "bar": None}

    @pytest.mark.parametrize(
        ("details", "exp_kwargs"),
        [
            ({"name": "spamState"}, {}),
            (
                {
                    "name": "spamState",
                    "output": '{"foo": [1, 2], "bar": null}'},
                {"output": {"foo": [1, 2], "bar": None}})])
    def test_get_args(self, timestamp, details, exp_kwargs):
        """Getting instantiation arguments from AWS API HistoryEvent."""
        # Setup environment
        type_keys = {"StateExited": "StateExitedEventDetails"}

        # Build input
        history_event = {
            "timestamp": timestamp,
            "type": "StateExited",
            "id": 2,
            "previousEventId": 1,
            "StateExitedEventDetails": details}

        # Build expectation
        exp_args = (timestamp, "StateExited", 2, "spamState")
        exp_kwargs.update({"previous_event_id": 1})
        exp_details = details

        # Run function
        with mock.patch.object(tscr, "_type_keys", type_keys):
            res = tscr.StateExited._get_args(history_event)

        # Check result
        res_args, res_kwargs, res_details = res
        assert res_args == exp_args
        assert res_kwargs == exp_kwargs
        assert res_details == exp_details

    def test_details_str(self, event):
        """StateExited details formatting."""
        assert event.details_str == "name: spamState"


def test_parse_history():
    """History event item processing."""
    # Setup environment
    event_mocks = {
        "type1": [mock.Mock(spec=tscr.Event), mock.Mock(spec=tscr.Event)],
        "type2": [mock.Mock(spec=tscr.Event), mock.Mock(spec=tscr.Event)],
        "type3": [mock.Mock(spec=tscr.Event)]}
    type_classes = {n: mock.Mock() for n in event_mocks}
    type_classes["type1"].from_history_event.side_effect = event_mocks["type1"]
    type_classes["type2"].from_history_event.side_effect = event_mocks["type2"]
    type_classes["type3"].from_history_event.side_effect = event_mocks["type3"]

    # Build input
    history_events = [
        {"type": "type1", "a": 42, "b": "spam"},
        {"type": "type2", "c": [1, 2], "b": "bla"},
        {"type": "type1", "a": 17, "b": "foo"},
        {"type": "type3", "d": None, "b": "bar"},
        {"type": "type2", "c": [3, 5], "b": "eggs"}]

    # Build expectation
    exp = [
        event_mocks["type1"][0],
        event_mocks["type2"][0],
        event_mocks["type1"][1],
        event_mocks["type3"][0],
        event_mocks["type2"][1]]
    exp_calls = {
        "type1": [
            mock.call.from_history_event(history_events[0]),
            mock.call.from_history_event(history_events[2])],
        "type2": [
            mock.call.from_history_event(history_events[1]),
            mock.call.from_history_event(history_events[4])],
        "type3": [mock.call.from_history_event(history_events[3])]}

    # Test function
    with mock.patch.object(tscr, "_type_classes", type_classes):
        res = tscr.parse_history(history_events)

    # Check result
    assert res == exp
    for k, c_mock in type_classes.items():
        assert c_mock.method_calls == exp_calls[k]
