"""Test ``sfini.state._base``."""

from sfini.state import _base as tscr
import pytest
from unittest import mock


class TestState:
    """Test ``sfini.state._base.State``."""
    @pytest.fixture
    def state(self):
        """An example State instance."""
        return tscr.State(
            "spam",
            comment="a state",
            input_path="$.spam.input",
            output_path="$.spam.output")

    def test_init(self, state):
        """State initialisation."""
        assert state.name == "spam"
        assert state.comment == "a state"
        assert state.input_path == "$.spam.input"
        assert state.output_path == "$.spam.output"

    def test_str(self, state):
        """State stringification."""
        res = str(state)
        assert "spam" in res
        assert "State" in res

    def test_repr(self, state):
        """State string representation."""
        exp = (
            "State('spam', comment='a state', input_path='$.spam.input', "
            "output_path='$.spam.output')")
        assert repr(state) == exp

    class TestAddTo:
        """Add state to collection."""
        def test_new(self, state):
            """Not existing."""
            states = {"bla": mock.Mock(spec=tscr.State)}
            exp_states = {"bla": states["bla"], "spam": state}
            state.add_to(states)
            assert states == exp_states

        def test_old(self, state):
            """Already existing."""
            states = {"bla": mock.Mock(spec=tscr.State), "spam": state}
            exp_states = {"bla": states["bla"], "spam": state}
            state.add_to(states)
            assert states == exp_states

        def test_other(self, state):
            """Name already provided."""
            states = {
                "bla": mock.Mock(spec=tscr.State),
                "spam": mock.Mock(spec=tscr.State)}
            exp_states = {"bla": states["bla"], "spam": states["spam"]}
            with pytest.raises(ValueError) as e:
                state.add_to(states)
            assert "spam" in str(e.value)
            assert states == exp_states

    @pytest.mark.parametrize(
        ("comment", "input_path", "output_path", "exp"),
        [
            (tscr._default, tscr._default, tscr._default, {}),
            (
                tscr._default,
                tscr._default,
                "$.spam.output",
                {"OutputPath": "$.spam.output"}),
            (
                tscr._default,
                "$.spam.input",
                tscr._default,
                {"InputPath": "$.spam.input"}),
            (
                tscr._default,
                "$.spam.input",
                "$.spam.output",
                {"InputPath": "$.spam.input", "OutputPath": "$.spam.output"}),
            (
                "a state",
                tscr._default,
                tscr._default,
                {"Comment": "a state"}),
            (
                "a state",
                tscr._default,
                "$.spam.output",
                {"Comment": "a state", "OutputPath": "$.spam.output"}),
            (
                "a state",
                "$.spam.input",
                tscr._default,
                {"Comment": "a state", "InputPath": "$.spam.input"}),
            (
                "a state",
                "$.spam.input",
                "$.spam.output",
                {
                    "Comment": "a state",
                    "InputPath": "$.spam.input",
                    "OutputPath": "$.spam.output"})])
    def test_to_dict(self, state, comment, input_path, output_path, exp):
        """Definition dictionary construction."""
        state.comment = comment
        state.input_path = input_path
        state.output_path = output_path
        exp["Type"] = "State"
        res = state.to_dict()
        assert res == exp


class TestHasNext:
    """Test ``sfini.state._base.HasNext``."""
    @pytest.fixture
    def state(self):
        """An example HasNext instance."""
        return tscr.HasNext(
            "spam",
            comment="a state",
            input_path="$.spam.input",
            output_path="$.spam.output")

    def test_init(self, state):
        """HasNext initialisation."""
        assert state.name == "spam"
        assert state.comment == "a state"
        assert state.input_path == "$.spam.input"
        assert state.output_path == "$.spam.output"
        assert state.next is None

    class TestAddTo:
        """Add state to collection."""
        def test_terminal(self, state):
            """No next state."""
            states = {"bla": mock.Mock(spec=tscr.State)}
            exp_states = {"bla": states["bla"], "spam": state}
            state.add_to(states)
            assert states == exp_states

        def test_not_terminal(self, state):
            """Has next state."""
            state.next = mock.Mock(spec=tscr.State)
            state.next.name = "spamNext"
            states = {"bla": mock.Mock(spec=tscr.State)}
            exp_states = {"bla": states["bla"], "spam": state}
            state.add_to(states)
            assert states == exp_states
            state.next.add_to.assert_called_once_with(states)

        def test_not_terminal_already_registered(self, state):
            """Has next state."""
            state.next = mock.Mock(spec=tscr.State)
            state.next.name = "foo"
            states = {"bla": mock.Mock(spec=tscr.State), "foo": state.next}
            exp_states = {
                "bla": states["bla"],
                "foo": state.next,
                "spam": state}
            state.add_to(states)
            assert states == exp_states
            state.next.add_to.assert_not_called()

    @pytest.mark.parametrize(
        "prev_next_state",
        [None, mock.Mock(spec=tscr.State)])
    def test_goes_to(self, state, prev_next_state):
        """Next-state setting."""
        state.next = prev_next_state
        next_state = mock.Mock(spec=tscr.State)
        state.goes_to(next_state)
        assert state.next is next_state

    def test_remove_next(self, state):
        """Next-state removing."""
        state.next = mock.Mock(spec=tscr.State)
        state.remove_next()
        assert state.next is None

    class TestToDict:
        """Definition dictionary construction."""
        def test_terminal(self, state):
            """No next state."""
            exp = {
                "Type": "HasNext",
                "Comment": "a state",
                "InputPath": "$.spam.input",
                "OutputPath": "$.spam.output",
                "End": True}
            res = state.to_dict()
            assert res == exp

        def test_not_terminal(self, state):
            """Has next state."""
            state.next = mock.Mock(spec=tscr.State)
            state.next.name = "spam"
            exp = {
                "Type": "HasNext",
                "Comment": "a state",
                "InputPath": "$.spam.input",
                "OutputPath": "$.spam.output",
                "Next": "spam"}
            res = state.to_dict()
            assert res == exp


class TestHasResultPath:
    """Test ``sfini.state._base.HasResultPath``."""
    @pytest.fixture
    def state(self):
        """An example HasResultPath instance."""
        return tscr.HasResultPath(
            "spam",
            comment="a state",
            input_path="$.spam.input",
            output_path="$.spam.output",
            result_path="$.result")

    def test_init(self, state):
        """HasResultPath initialisation."""
        assert state.name == "spam"
        assert state.comment == "a state"
        assert state.input_path == "$.spam.input"
        assert state.output_path == "$.spam.output"
        assert state.result_path == "$.result"

    @pytest.mark.parametrize(
        ("result_path", "exp"),
        [(tscr._default, {}), ("$.result", {"ResultPath": "$.result"})])
    def test_to_dict(self, state, result_path, exp):
        """Definition dictionary construction."""
        state.result_path = result_path
        exp["Type"] = "HasResultPath"
        exp["Comment"] = "a state"
        exp["InputPath"] = "$.spam.input"
        exp["OutputPath"] = "$.spam.output"
        res = state.to_dict()
        assert res == exp


class TestCanRetry:
    """Test ``sfini.state._base.CanRetry``."""
    @pytest.fixture
    def state(self):
        """An example CanRetry instance."""
        return tscr.CanRetry(
            "spam",
            comment="a state",
            input_path="$.spam.input",
            output_path="$.spam.output")

    def test_init(self, state):
        """CanRetry initialisation."""
        assert state.name == "spam"
        assert state.comment == "a state"
        assert state.input_path == "$.spam.input"
        assert state.output_path == "$.spam.output"
        assert state.retriers == []

    def test_retry_for(self, state):
        """Retry handler adding."""
        state.retriers = []
        errors = ["BlaSpammed", "FooBarred"]
        interval = 5
        max_attempts = 10
        backoff_rate = 2.0
        exp_retriers = [
            (
                ["BlaSpammed", "FooBarred"],
                {"interval": 5, "max_attempts": 10, "backoff_rate": 2.0})]
        state.retry_for(
            errors,
            interval=interval,
            max_attempts=max_attempts,
            backoff_rate=backoff_rate)
        assert state.retriers == exp_retriers

    @pytest.mark.parametrize(
        ("policy", "exp"),
        [
            (
                {
                    "interval": tscr._default,
                    "max_attempts": tscr._default,
                    "backoff_rate": tscr._default},
                {}),
            (
                {
                    "interval": tscr._default,
                    "max_attempts": tscr._default,
                    "backoff_rate": 2.0},
                {"BackoffRate": 2.0}),
            (
                {
                    "interval": tscr._default,
                    "max_attempts": 10,
                    "backoff_rate": tscr._default},
                {"MaxAttempts": 10}),
            (
                {
                    "interval": tscr._default,
                    "max_attempts": 10,
                    "backoff_rate": 2.0},
                {"MaxAttempts": 10, "BackoffRate": 2.0}),
            (
                {
                    "interval": 5,
                    "max_attempts": tscr._default,
                    "backoff_rate": tscr._default},
                {"IntervalSeconds": 5}),
            (
                {
                    "interval": 5,
                    "max_attempts": tscr._default,
                    "backoff_rate": 2.0},
                {"IntervalSeconds": 5, "BackoffRate": 2.0}),
            (
                {
                    "interval": 5,
                    "max_attempts": 10,
                    "backoff_rate": tscr._default},
                {"IntervalSeconds": 5, "MaxAttempts": 10}),
            (
                {
                    "interval": 5,
                    "max_attempts": 10,
                    "backoff_rate": 2.0},
                {
                    "IntervalSeconds": 5,
                    "MaxAttempts": 10,
                    "BackoffRate": 2.0})])
    def test_retrier_defn(self, state, policy, exp):
        """Retry handler definition generation."""
        ve_mock = mock.Mock()
        errors = ["BlaSpammed", "FooBarred"]
        exp["ErrorEquals"] = errors
        with mock.patch.object(tscr, "_validate_errors", ve_mock):
            res = state._retrier_defn(errors, policy)
        assert res == exp
        ve_mock.assert_called_once_with(errors)

    def test_get_retrier_defns(self, state):
        """Retry handler definitions generation."""
        # Setup environment
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
        defns = [
            {
                "ErrorEquals": ["BlaSpammed", "FooBarred"],
                "IntervalSeconds": 5,
                "MaxAttempts": 10,
                "BackoffRate": 2.0},
            {"ErrorEquals": ["States.ALL"], "MaxAttempts": 3}]
        state._retrier_defn = mock.Mock(side_effect=defns)

        # Build expectation
        exp_rd_calls = [
            mock.call(
                ["BlaSpammed", "FooBarred"],
                {"interval": 5, "max_attempts": 10, "backoff_rate": 2.0}),
            mock.call(
                ["States.ALL"],
                {
                    "interval": tscr._default,
                    "max_attempts": 3,
                    "backoff_rate": tscr._default})]

        # Run function
        res = state._get_retrier_defns()

        # Check result
        assert res == defns
        assert state._retrier_defn.call_args_list == exp_rd_calls

    class TestToDict:
        """Definition dictionary construction."""
        def test_no_retriers(self, state):
            """No retry handlers registered."""
            state._get_retrier_defns = mock.Mock(return_value=[])
            exp = {
                "Type": "CanRetry",
                "Comment": "a state",
                "InputPath": "$.spam.input",
                "OutputPath": "$.spam.output"}
            res = state.to_dict()
            assert res == exp
            state._get_retrier_defns.assert_called_once_with()

        def test_has_retriers(self, state):
            """Has retry handlers registered."""
            # Setup environment
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

            defns = [
                {
                    "ErrorEquals": ["BlaSpammed", "FooBarred"],
                    "IntervalSeconds": 5,
                    "MaxAttempts": 10,
                    "BackoffRate": 2.0},
                {"ErrorEquals": ["States.ALL"], "MaxAttempts": 3}]
            state._get_retrier_defns = mock.Mock(return_value=defns)

            # Build expectation
            exp = {
                "Type": "CanRetry",
                "Comment": "a state",
                "InputPath": "$.spam.input",
                "OutputPath": "$.spam.output",
                "Retry": defns}

            # Run function
            res = state.to_dict()

            # Check result
            assert res == exp
            state._get_retrier_defns.assert_called_once_with()


class TestCanCatch:
    """Test ``sfini.state._base.CanCatch``."""
    @pytest.fixture
    def state(self):
        """An example CanCatch instance."""
        return tscr.CanCatch(
            "spam",
            comment="a state",
            input_path="$.spam.input",
            output_path="$.spam.output")

    def test_init(self, state):
        """CanCatch initialisation."""
        assert state.name == "spam"
        assert state.comment == "a state"
        assert state.input_path == "$.spam.input"
        assert state.output_path == "$.spam.output"
        assert state.catchers == []

    def test_add_to(self, state):
        """Add state to collection."""
        # Setup environment
        foo_state = mock.Mock(spec=tscr.State)
        foo_state.name = "foo"
        bar_state = mock.Mock(spec=tscr.State)
        bar_state.name = "bar"
        state.catchers = [
            ([], {"next_state": foo_state}),
            ([], {"next_state": bar_state})]

        # Build input
        states = {"bla": mock.Mock(spec=tscr.State), "bar": bar_state}

        # Build expectation
        exp_states = {"bla": states["bla"], "bar": bar_state, "spam": state}

        # Run function
        state.add_to(states)

        # Check result
        assert states == exp_states
        foo_state.add_to.assert_called_once_with(states)
        bar_state.add_to.assert_not_called()

    def test_catch(self, state):
        """Catch handler adding."""
        state.catchers = [(["FooBarred", "BarFooed"], {})]
        errors = ["BlaSpammed", "FooBarred"]
        next_state = mock.Mock(spec=tscr.State)
        result_path = "$.error-info"
        exp_catchers = [
            (["FooBarred", "BarFooed"], {}),
            (
                ["BlaSpammed", "FooBarred"],
                {"next_state": next_state, "result_path": "$.error-info"})]
        state.catch(errors, next_state, result_path=result_path)
        assert state.catchers == exp_catchers

    @pytest.mark.parametrize(
        ("policy", "exp"),
        [
            ({"result_path": tscr._default}, {}),
            ({"result_path": "$.error-info"}, {"ResultPath": "$.error-info"})])
    def test_catcher_defn(self, state, policy, exp):
        """Policy definition generation."""
        policy["next_state"] = mock.Mock(spec=tscr.State)
        policy["next_state"].name = "bla"
        exp["Next"] = "bla"
        ve_mock = mock.Mock()
        errors = ["BlaSpammed", "FooBarred"]
        exp["ErrorEquals"] = errors
        with mock.patch.object(tscr, "_validate_errors", ve_mock):
            res = state._catcher_defn(errors, policy)
        assert res == exp
        ve_mock.assert_called_once_with(errors)

    def test_get_catcher_defns(self, state):
        """Error handler definitions generation."""
        # Setup environment
        foo_state = mock.Mock(spec=tscr.State)
        bar_state = mock.Mock(spec=tscr.State)
        state.catchers = [
            (
                ["BlaSpammed", "FooBarred"],
                {"next_state": foo_state, "result_path": "$.error-info"}),
            (
                ["States.ALL"],
                {"next_state": bar_state, "result_path": tscr._default})]
        defns = [
            {
                "ErrorEquals": ["BlaSpammed", "FooBarred"],
                "Next": "foo",
                "ResultPath": "$.error-info"},
            {"ErrorEquals": ["States.ALL"], "Next": "bla"}]
        state._catcher_defn = mock.Mock(side_effect=defns)

        # Build expectation
        exp_rd_calls = [
            mock.call(
                ["BlaSpammed", "FooBarred"],
                {"next_state": foo_state, "result_path": "$.error-info"}),
            mock.call(
                ["States.ALL"],
                {"next_state": bar_state, "result_path": tscr._default})]

        # Run function
        res = state._get_catcher_defns()

        # Check result
        assert res == defns
        assert state._catcher_defn.call_args_list == exp_rd_calls

    class TestToDict:
        """Definition dictionary construction."""
        def test_no_catchers(self, state):
            """No retry handlers registered."""
            state._get_catcher_defns = mock.Mock(return_value=[])
            exp = {
                "Type": "CanCatch",
                "Comment": "a state",
                "InputPath": "$.spam.input",
                "OutputPath": "$.spam.output"}
            res = state.to_dict()
            assert res == exp
            state._get_catcher_defns.assert_called_once_with()

        def test_has_catchers(self, state):
            """Has retry handlers registered."""
            # Setup environment
            state.catchers = [
                (
                    ["BlaSpammed", "FooBarred"],
                    {"next_state": "foo", "result_path": "$.error-info"}),
                (
                    ["States.ALL"],
                    {"next_state": "bar", "result_path": tscr._default})]

            defns = [
                {
                    "ErrorEquals": ["BlaSpammed", "FooBarred"],
                    "Next": "foo",
                    "ResultPath": "$.error-info"},
                {"ErrorEquals": ["States.ALL"], "Next": "bar"}]
            state._get_catcher_defns = mock.Mock(return_value=defns)

            # Build expectation
            exp = {
                "Type": "CanCatch",
                "Comment": "a state",
                "InputPath": "$.spam.input",
                "OutputPath": "$.spam.output",
                "Catch": defns}

            # Run function
            res = state.to_dict()

            # Check result
            assert res == exp
            state._get_catcher_defns.assert_called_once_with()


@pytest.mark.parametrize(
    ("klass", "parent"),
    [
        (tscr.HasNext, tscr.State),
        (tscr.HasResultPath, tscr.State),
        (tscr.CanRetry, tscr.State),
        (tscr.CanCatch, tscr.State)])
def test_inheritance(klass, parent):
    """State inheritance."""
    assert issubclass(klass, parent)


class TestValidateErrors:
    """Error condition comparison value validation."""
    def test_empty(self):
        """No errors provided."""
        errors = []
        with pytest.raises(ValueError) as e:
            tscr._validate_errors(errors)
        assert "empty" in str(e.value) or " no" in str(e.value)

    def test_all_with_extra(self):
        """Trying to handle all and extra errors."""
        errors = ["States.ALL", "WorkerCancel"]
        with pytest.raises(ValueError) as e:
            tscr._validate_errors(errors)
        assert "States.ALL" in str(e.value)

    def test_invalid_predefined(self):
        """'States' error not one of the pre-defined."""
        errors = ["States.BlaSpammed"]
        with pytest.raises(ValueError) as e:
            tscr._validate_errors(errors)
        assert "BlaSpammed" in str(e.value)

    @pytest.mark.parametrize(
        "errors",
        [
            ["States.ALL"],
            ["States.NoChoiceMatched", "States.ParameterPathFailure"],
            ["WorkerCancel"],
            ["BlaSpammed", "FooBarred"]])
    def test_all(self, errors):
        """Handling example errors."""
        tscr._validate_errors(errors)
