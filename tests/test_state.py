# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""Test ``sfini.state._state``."""

from sfini.state import _state as tscr
import pytest
from unittest import mock
from sfini.state import _base
import datetime
import sfini


class TestFail:
    """Test ``sfini.state._state.Fail``."""
    @pytest.fixture
    def state(self):
        """An example Fail instance."""
        return tscr.Fail(
            "spam",
            comment="a state",
            input_path="$.spam.input",
            output_path="$.spam.output",
            error="BlaSpammed",
            cause="a bla has spammed")

    def test_init(self, state):
        """Fail initialisation."""
        assert state.name == "spam"
        assert state.comment == "a state"
        assert state.input_path == "$.spam.input"
        assert state.output_path == "$.spam.output"
        assert state.error == "BlaSpammed"
        assert state.cause == "a bla has spammed"

    @pytest.mark.parametrize(
        ("error", "cause", "exp"),
        [
            (tscr._default, tscr._default, {}),
            (
                tscr._default,
                "a bla has spammed",
                {"Cause": "a bla has spammed"}),
            ("BlaSpammed", tscr._default, {"Error": "BlaSpammed"}),
            (
                "BlaSpammed",
                "a bla has spammed",
                {"Cause": "a bla has spammed", "Error": "BlaSpammed"})])
    def test_to_dict(self, state, error, cause, exp):
        """Definition dictionary construction."""
        state.error = error
        state.cause = cause
        exp["Type"] = "Fail"
        exp["Comment"] = "a state"
        exp["InputPath"] = "$.spam.input"
        exp["OutputPath"] = "$.spam.output"
        res = state.to_dict()
        assert res == exp


class TestPass:
    """Test ``sfini.state._state.Pass``."""
    @pytest.fixture
    def state(self):
        """An example Pass instance."""
        return tscr.Pass(
            "spam",
            comment="a state",
            input_path="$.spam.input",
            output_path="$.spam.output",
            result_path="$.result",
            result={"foo": [1, 2], "bar": None})

    def test_init(self, state):
        """Pass initialisation."""
        assert state.name == "spam"
        assert state.comment == "a state"
        assert state.input_path == "$.spam.input"
        assert state.output_path == "$.spam.output"
        assert state.result_path == "$.result"
        assert state.result == {"foo": [1, 2], "bar": None}

    @pytest.mark.parametrize(
        ("result", "exp"),
        [
            (tscr._default, {}),
            (
                {"foo": [1, 2], "bar": None},
                {"Result": {"foo": [1, 2], "bar": None}})])
    def test_to_dict(self, state, result, exp):
        """Definition dictionary construction."""
        state.next = mock.Mock(spec=_base.State)
        state.next.name = "bla"
        state.result = result
        exp["Type"] = "Pass"
        exp["Comment"] = "a state"
        exp["InputPath"] = "$.spam.input"
        exp["OutputPath"] = "$.spam.output"
        exp["ResultPath"] = "$.result"
        exp["Next"] = "bla"
        res = state.to_dict()
        assert res == exp


class TestWait:
    """Test ``sfini.state._state.Wait``."""
    @pytest.fixture
    def state(self):
        """An example Wait instance."""
        return tscr.Wait(
            "spam",
            42,
            comment="a state",
            input_path="$.spam.input",
            output_path="$.spam.output")

    def test_init(self, state):
        """Wait initialisation."""
        assert state.name == "spam"
        assert state.until == 42
        assert state.comment == "a state"
        assert state.input_path == "$.spam.input"
        assert state.output_path == "$.spam.output"

    class TestToDict:
        """Definition dictionary construction."""
        _now = datetime.datetime.now(tz=datetime.timezone.utc)
        _now += datetime.timedelta(hours=24)

        @pytest.mark.parametrize(
            ("until", "exp"),
            [
                (42, {"Seconds": 42}),
                (_now, {"Timestamp": _now.isoformat("T")}),
                ("$.spam.waitDate", {"TimestampPath": "$.spam.waitDate"}),
                pytest.param(
                    "$.spam.waitTime",
                    {"SecondsPath": "$.spam.waitTime"},
                    marks=pytest.mark.xfail(
                        reason=(
                            "Need to implement seconds-variable wait-time")))])
        def test_valid(self, state, until, exp):
            """Provided 'wait until' is valid."""
            state.next = mock.Mock(spec=_base.State)
            state.next.name = "bla"
            state.until = until
            exp["Type"] = "Wait"
            exp["Comment"] = "a state"
            exp["InputPath"] = "$.spam.input"
            exp["OutputPath"] = "$.spam.output"
            exp["Next"] = "bla"
            res = state.to_dict()
            assert res == exp

        @pytest.mark.parametrize(
            "until",
            [None, [1, 2], {"SecondsPath": "$.spam.waitTime"}])
        def test_invalid(self, state, until):
            """Provided 'wait until' is invalid."""
            state.until = until
            with pytest.raises(TypeError) as e:
                _ = state.to_dict()
            assert str(type(until)) in str(e.value)

        def test_naive_datetime(self, state):
            """Until date-time is naive."""
            state.next = mock.Mock(spec=_base.State)
            state.next.name = "bla"
            state.until = datetime.datetime.now()
            with pytest.raises(ValueError) as e:
                _ = state.to_dict()
            assert "aware" in str(e.value)


class TestParallel:
    """Test ``sfini.state._state.Parallel``."""
    @pytest.fixture
    def state(self):
        """An example Parallel instance."""
        return tscr.Parallel(
            "spam",
            comment="a state",
            input_path="$.spam.input",
            output_path="$.spam.output",
            result_path="$.result")

    def test_init(self, state):
        """Parallel initialisation."""
        assert state.name == "spam"
        assert state.comment == "a state"
        assert state.input_path == "$.spam.input"
        assert state.output_path == "$.spam.output"
        assert state.result_path == "$.result"
        assert state.next is None
        assert state.retriers == []
        assert state.catchers == []

    def test_add(self, state):
        """Branch adding."""
        state.branches = []
        state_machine = mock.Mock(spec=sfini.state_machine.StateMachine)
        state.add(state_machine)

    class TestToDict:
        """Definition dictionary construction."""
        def test_no_error_handlers(self, state):
            """No retriers and catchers."""
            # Setup environment
            state.branches = [
                mock.Mock(spec=sfini.state_machine.StateMachine)
                for _ in range(3)]

            state.next = mock.Mock()
            state.next.name = "bla"

            state._get_retrier_defns = mock.Mock(return_value=[])
            state._get_catcher_defns = mock.Mock(return_value=[])

            # Build expectation
            exp = {
                "Type": "Parallel",
                "Comment": "a state",
                "InputPath": "$.spam.input",
                "OutputPath": "$.spam.output",
                "ResultPath": "$.result",
                "Next": "bla",
                "Branches": [sm.to_dict.return_value for sm in state.branches]}

            # Run function
            res = state.to_dict()

            # Check result
            assert res == exp
            state._get_retrier_defns.assert_called_once_with()
            state._get_catcher_defns.assert_called_once_with()
            [sm.to_dict.assert_called_once_with() for sm in state.branches]

        def test_retry_catch(self, state):
            """With retriers and catchers."""
            # Setup environment
            state.branches = [
                mock.Mock(spec=sfini.state_machine.StateMachine)
                for _ in range(3)]

            state.next = mock.Mock()
            state.next.name = "bla"

            state.retriers = [
                (
                    ["BlaSpammed", "FooBarred"],
                    {"interval": 5, "max_attempts": 10, "backoff_rate": 2.0}),
                (
                    ["States.ALL"],
                    {
                        "interval": tscr._default,
                        "max_attempts": 3,
                        "backoff_rate": tscr._default})]

            retry_defns = [
                {
                    "ErrorEquals": ["BlaSpammed", "FooBarred"],
                    "IntervalSeconds": 5,
                    "MaxAttempts": 10,
                    "BackoffRate": 2.0},
                {"ErrorEquals": ["States.ALL"], "MaxAttempts": 3}]
            state._get_retrier_defns = mock.Mock(return_value=retry_defns)

            foo_state = mock.Mock(spec=_base.State)
            bar_state = mock.Mock(spec=_base.State)
            state.catchers = [
                (
                    ["BlaSpammed", "FooBarred"],
                    {"next_state": foo_state, "result_path": "$.error-info"}),
                (
                    ["States.ALL"],
                    {"next_state": bar_state, "result_path": tscr._default})]
            catch_defns = [
                {
                    "ErrorEquals": ["BlaSpammed", "FooBarred"],
                    "Next": "foo",
                    "ResultPath": "$.error-info"},
                {"ErrorEquals": ["States.ALL"], "Next": "bla"}]
            state._get_catcher_defns = mock.Mock(return_value=catch_defns)

            # Build expectation
            exp = {
                "Type": "Parallel",
                "Comment": "a state",
                "InputPath": "$.spam.input",
                "OutputPath": "$.spam.output",
                "ResultPath": "$.result",
                "Next": "bla",
                "Retry": retry_defns,
                "Catch": catch_defns,
                "Branches": [sm.to_dict.return_value for sm in state.branches]}

            # Run function
            res = state.to_dict()

            # Check result
            assert res == exp
            state._get_retrier_defns.assert_called_once_with()
            state._get_catcher_defns.assert_called_once_with()
            [sm.to_dict.assert_called_once_with() for sm in state.branches]


class TestChoice:
    """Test ``sfini.state._state.Choice``."""
    @pytest.fixture
    def state(self):
        """An example Choice instance."""
        return tscr.Choice(
            "spam",
            comment="a state",
            input_path="$.spam.input",
            output_path="$.spam.output")

    def test_init(self, state):
        """Choice initialisation."""
        assert state.name == "spam"
        assert state.comment == "a state"
        assert state.input_path == "$.spam.input"
        assert state.output_path == "$.spam.output"
        assert state.choices == []
        assert state.default is None

    class TestAddTo:
        """Add state to collection."""
        def test_no_default(self, state):
            """No default state."""
            # Setup environmnent
            foo_rule = mock.Mock(spec=sfini.state.choice.ChoiceRule)
            foo_rule.next_state = mock.Mock(spec=_base.State)
            foo_rule.next_state.name = "fooNext"
            bar_rule = mock.Mock(spec=sfini.state.choice.ChoiceRule)
            bar_rule.next_state = mock.Mock(spec=_base.State)
            bar_rule.next_state.name = "barNext"
            state.choices = [foo_rule, bar_rule]

            # Build input
            states = {
                "bla": mock.Mock(spec=_base.State),
                "barNext": foo_rule.next_state}

            # Build expectation
            exp_states = {
                "bla": states["bla"],
                "barNext": foo_rule.next_state,
                "spam": state}

            # Run function
            state.add_to(states)

            # Check result
            assert states == exp_states
            foo_rule.next_state.add_to.assert_called_once_with(states)
            bar_rule.next_state.add_to.assert_not_called()

        def test_has_default(self, state):
            """Has default state."""
            # Setup environmnent
            foo_rule = mock.Mock(spec=sfini.state.choice.ChoiceRule)
            foo_rule.next_state = mock.Mock(spec=_base.State)
            foo_rule.next_state.name = "fooNext"
            bar_rule = mock.Mock(spec=sfini.state.choice.ChoiceRule)
            bar_rule.next_state = mock.Mock(spec=_base.State)
            bar_rule.next_state.name = "barNext"
            state.choices = [foo_rule, bar_rule]
            state.default = mock.Mock(spec=_base.State)
            state.default.name = "default"

            # Build input
            states = {
                "bla": mock.Mock(spec=_base.State),
                "barNext": foo_rule.next_state}

            # Build expectation
            exp_states = {
                "bla": states["bla"],
                "barNext": foo_rule.next_state,
                "spam": state}

            # Run function
            state.add_to(states)

            # Check result
            assert states == exp_states
            state.default.add_to.assert_called_once_with(states)
            foo_rule.next_state.add_to.assert_called_once_with(states)
            bar_rule.next_state.add_to.assert_not_called()

    class TestAdd:
        """Choice-rule adding."""
        def test_has_next_state(self, state):
            """Rule has a next-state."""
            state.choices = []
            rule = mock.Mock(spec=sfini.state.choice.ChoiceRule)
            rule.next_state = mock.Mock(spec=_base.State)
            exp_choices = [rule]
            state.add(rule)
            assert state.choices == exp_choices

        def test_no_next_state(self, state):
            """Rule has no a next-state."""
            state.choices = []
            rule = mock.Mock(spec=sfini.state.choice.ChoiceRule)
            rule.next_state = None
            exp_choices = []
            with pytest.raises(RuntimeError) as e:
                state.add(rule)
            assert str(rule) in str(e.value)
            assert state.choices == exp_choices

    class TestRemove:
        """Choice-rule removal."""
        def test_registered(self, state):
            """Rule is an existing branch."""
            rule = mock.Mock(spec=sfini.state.choice.ChoiceRule)
            state.choices = [rule]
            exp_choices = []
            state.remove(rule)
            assert state.choices == exp_choices

        def test_not_registered(self, state):
            """Rule is not an existing branch."""
            rule = mock.Mock(spec=sfini.state.choice.ChoiceRule)
            foo_rule = mock.Mock(spec=sfini.state.choice.ChoiceRule)
            state.choices = [foo_rule]
            exp_choices = [foo_rule]
            with pytest.raises(ValueError) as e:
                state.remove(rule)
            assert str(rule) in str(e.value)
            assert state.choices == exp_choices

    @pytest.mark.parametrize(
        "prev_default",
        [None, mock.Mock(spec=_base.State)])
    def test_set_default(self, state, prev_default):
        """Default state-setting."""
        state.default = prev_default
        default_state = mock.Mock(spec=_base.State)
        state.set_default(default_state)
        assert state.default is default_state

    class TestToDict:
        """Definition dictionary construction."""
        def test_no_default(self, state):
            """No default state registered."""
            state.choices = [
                mock.Mock(spec=sfini.state.choice.ChoiceRule)
                for _ in range(3)]
            exp = {
                "Type": "Choice",
                "Comment": "a state",
                "InputPath": "$.spam.input",
                "OutputPath": "$.spam.output",
                "Choices": [c.to_dict.return_value for c in state.choices]}
            res = state.to_dict()
            assert res == exp
            [c.to_dict.assert_called_once_with() for c in state.choices]

        def test_with_default(self, state):
            """Default state registered."""
            state.default = mock.Mock(spec=_base.State)
            state.default.name = "bla"
            state.choices = [
                mock.Mock(spec=sfini.state.choice.ChoiceRule)
                for _ in range(3)]
            exp = {
                "Type": "Choice",
                "Comment": "a state",
                "InputPath": "$.spam.input",
                "OutputPath": "$.spam.output",
                "Default": "bla",
                "Choices": [c.to_dict.return_value for c in state.choices]}
            res = state.to_dict()
            assert res == exp
            [c.to_dict.assert_called_once_with() for c in state.choices]

        def test_no_next_path(self, state):
            """No transition available for choice."""
            with pytest.raises(RuntimeError) as e:
                state.to_dict()
            assert " no" in str(e.value)
            assert "path" in str(e.value) or "transition" in str(e.value)
            assert str(state) in str(e.value)


class TestTask:
    """Test ``sfini.state._state.Task``."""
    @pytest.fixture
    def resource_mock(self):
        """Task resource mock."""
        return mock.Mock(spec=sfini.task_resource.TaskResource)

    @pytest.fixture
    def state(self, resource_mock):
        """An example Task instance."""
        return tscr.Task(
            "spam",
            resource_mock,
            comment="a state",
            input_path="$.spam.input",
            output_path="$.spam.output",
            result_path="$.result",
            timeout=42)

    def test_init(self, state, resource_mock):
        """Task initialisation."""
        assert state.name == "spam"
        assert state.resource is resource_mock
        assert state.comment == "a state"
        assert state.input_path == "$.spam.input"
        assert state.output_path == "$.spam.output"
        assert state.result_path == "$.result"
        assert state.timeout == 42
        assert state.next is None
        assert state.retriers == []
        assert state.catchers == []

    class TestToDict:
        """Definition dictionary construction."""
        @pytest.mark.parametrize(
            ("timeout", "heartbeat", "exp"),
            [
                (tscr._default, None, {}),
                (tscr._default, 10, {"HeartbeatSeconds": 15}),
                (42, None, {"TimeoutSeconds": 42}),
                (42, 10, {"TimeoutSeconds": 42, "HeartbeatSeconds": 15})])
        def test_no_error_handlers(
                self,
                state,
                resource_mock,
                timeout,
                heartbeat,
                exp):
            """No retriers and catchers."""
            # Setup environment
            state.timeout = timeout

            state.next = mock.Mock()
            state.next.name = "bla"

            if heartbeat is not None:
                resource_mock.heartbeat = heartbeat
            resource_mock.arn = "resource:arn"

            state._get_retrier_defns = mock.Mock(return_value=[])
            state._get_catcher_defns = mock.Mock(return_value=[])

            # Build expectation
            exp["Type"] = "Task"
            exp["Resource"] = "resource:arn"
            exp["Comment"] = "a state"
            exp["InputPath"] = "$.spam.input"
            exp["OutputPath"] = "$.spam.output"
            exp["ResultPath"] = "$.result"
            exp["Next"] = "bla"

            # Run function
            res = state.to_dict()

            # Check result
            assert res == exp
            state._get_retrier_defns.assert_called_once_with()
            state._get_catcher_defns.assert_called_once_with()

        @pytest.mark.parametrize(
            ("timeout", "heartbeat", "exp"),
            [
                (tscr._default, None, {}),
                (tscr._default, 10, {"HeartbeatSeconds": 15}),
                (42, None, {"TimeoutSeconds": 42}),
                (42, 10, {"TimeoutSeconds": 42, "HeartbeatSeconds": 15})])
        def test_retry_catch(
                self,
                state,
                resource_mock,
                timeout,
                heartbeat,
                exp):
            """With retriers and catchers."""
            # Setup environment
            state.timeout = timeout

            state.next = mock.Mock()
            state.next.name = "bla"

            if heartbeat is not None:
                resource_mock.heartbeat = heartbeat
            resource_mock.arn = "resource:arn"

            state.retriers = [
                (
                    ["BlaSpammed", "FooBarred"],
                    {"interval": 5, "max_attempts": 10, "backoff_rate": 2.0}),
                (
                    ["States.ALL"],
                    {
                        "interval": tscr._default,
                        "max_attempts": 3,
                        "backoff_rate": tscr._default})]

            retry_defns = [
                {
                    "ErrorEquals": ["BlaSpammed", "FooBarred"],
                    "IntervalSeconds": 5,
                    "MaxAttempts": 10,
                    "BackoffRate": 2.0},
                {"ErrorEquals": ["States.ALL"], "MaxAttempts": 3}]
            state._get_retrier_defns = mock.Mock(return_value=retry_defns)

            foo_state = mock.Mock(spec=_base.State)
            bar_state = mock.Mock(spec=_base.State)
            state.catchers = [
                (
                    ["BlaSpammed", "FooBarred"],
                    {"next_state": foo_state, "result_path": "$.error-info"}),
                (
                    ["States.ALL"],
                    {"next_state": bar_state, "result_path": tscr._default})]
            catch_defns = [
                {
                    "ErrorEquals": ["BlaSpammed", "FooBarred"],
                    "Next": "foo",
                    "ResultPath": "$.error-info"},
                {"ErrorEquals": ["States.ALL"], "Next": "bla"}]
            state._get_catcher_defns = mock.Mock(return_value=catch_defns)

            # Build expectation
            exp["Type"] = "Task"
            exp["Resource"] = "resource:arn"
            exp["Comment"] = "a state"
            exp["InputPath"] = "$.spam.input"
            exp["OutputPath"] = "$.spam.output"
            exp["ResultPath"] = "$.result"
            exp["Next"] = "bla"
            exp["Retry"] = retry_defns
            exp["Catch"] = catch_defns

            # Run function
            res = state.to_dict()

            # Check result
            assert res == exp
            state._get_retrier_defns.assert_called_once_with()
            state._get_catcher_defns.assert_called_once_with()


@pytest.mark.parametrize(
    ("klass", "parent"),
    [
        (tscr.Succeed, _base.State),
        (tscr.Fail, _base.State),
        (tscr.Pass, _base.State),
        (tscr.Pass, _base.HasNext),
        (tscr.Pass, _base.HasResultPath),
        (tscr.Wait, _base.State),
        (tscr.Wait, _base.HasNext),
        (tscr.Parallel, _base.State),
        (tscr.Parallel, _base.CanCatch),
        (tscr.Parallel, _base.CanRetry),
        (tscr.Parallel, _base.HasNext),
        (tscr.Parallel, _base.HasResultPath),
        (tscr.Choice, _base.State),
        (tscr.Task, _base.State),
        (tscr.Task, _base.CanCatch),
        (tscr.Task, _base.CanRetry),
        (tscr.Task, _base.HasNext),
        (tscr.Task, _base.HasResultPath)])
def test_inheritance(klass, parent):
    """State inheritance."""
    assert issubclass(klass, parent)
