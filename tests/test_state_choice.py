"""Test ``sfini.state.choice``."""

from sfini.state import choice as tscr
import pytest
from unittest import mock
import sfini
from sfini import _util as sfini_util
import datetime


@pytest.fixture
def state_mock():
    """A State mock."""
    return mock.MagicMock(spec=sfini.state.State)


class TestChoiceRule:
    """Test ``sfini.state.choice.ChoiceRule``."""
    @pytest.fixture
    def rule(self, state_mock):
        """An example ChoiceRule instance."""
        return tscr.ChoiceRule(next_state=state_mock)

    def test_init(self, rule, state_mock):
        """ChoiceRule initialisation."""
        assert rule.next_state is state_mock

    def test_repr(self):
        """ChoiceRule string representation."""
        assert tscr.ChoiceRule.__repr__ is sfini_util.easy_repr

    def test_get_comparison(self, rule):
        """Comparison definition generation."""
        with pytest.raises(NotImplementedError):
            _ = rule._get_comparison()

    class TestToDict:
        """Definition construction."""
        def test_final_has_next(self, rule, state_mock):
            """Finalised choice with next-state."""
            rule._final = True
            rule._get_comparison = mock.Mock(return_value=42)
            state_mock.name = "spamState"
            exp = {"ChoiceRule": 42, "Next": "spamState"}
            res = rule.to_dict()
            assert res == exp
            rule._get_comparison.assert_called_once_with()

        def test_final_no_next(self, rule):
            """Finalised choice with no next-state."""
            rule._final = True
            rule._get_comparison = mock.Mock(return_value=42)
            rule.next_state = None
            exp = {"ChoiceRule": 42}
            res = rule.to_dict()
            assert res == exp
            rule._get_comparison.assert_called_once_with()

        def test_not_final(self, rule):
            """Unfinalised choice with no next-state."""
            rule._final = False
            rule._get_comparison = mock.Mock()
            with pytest.raises(RuntimeError) as e:
                _ = rule.to_dict()
            assert "choice" in str(e.value)
            rule._get_comparison.assert_not_called()


class TestComparison:
    """Test ``sfini.state.choice.Comparison``."""
    @pytest.fixture
    def rule(self, state_mock):
        """An example Comparison instance."""
        return tscr.Comparison("$.varPath", 42, next_state=state_mock)

    def test_init(self, rule, state_mock):
        """Comparison initialisation."""
        assert rule.variable_path == "$.varPath"
        assert rule.comparison_value == 42
        assert rule.next_state is state_mock

    def test_str(self, rule, state_mock):
        """Comparison stringification."""
        res = str(rule)
        assert "$.varPath" in res
        assert "Comparison" in res
        assert "42" in res
        assert str(state_mock) in res

    class TestGetComparison:
        """Comparison value constructing."""
        def test_correct_type(self, rule):
            """Comparison value is the correct type."""
            rule._expected_value_type = int
            assert rule._get_comparison() == 42

        def test_wrong_type(self, rule):
            """Comparison value is not the correct type."""
            rule._expected_value_type = float
            with pytest.raises(TypeError) as e:
                _ = rule._get_comparison()
            assert "42" in str(e.value)
            assert "type" in str(e.value)

    def test_to_dict(self, rule, state_mock):
        """Definition construction."""
        rule._final = True
        rule._get_comparison = mock.Mock(return_value=42)
        state_mock.name = "spamState"
        exp = {"Comparison": 42, "Next": "spamState", "Variable": "$.varPath"}
        res = rule.to_dict()
        assert res == exp
        rule._get_comparison.assert_called_once_with()


class TestTimestampRule:
    """Test ``sfini.state.choice._TimestampRule``."""
    @pytest.fixture
    def timestamp(self):
        """An example time-stamp."""
        return datetime.datetime.now(tz=datetime.timezone.utc)

    @pytest.fixture
    def rule(self, state_mock, timestamp):
        """An example _TimestampRule instance."""
        return tscr._TimestampRule(
            "$.varPath",
            timestamp,
            next_state=state_mock)

    class TestGetComparison:
        """Comparison timestamp constructing."""
        def test_aware(self, rule, timestamp):
            """Comparison timestamp is aware."""
            exp = timestamp.isoformat("T")
            assert rule._get_comparison() == exp

        def test_naive(self, rule):
            """Comparison timestamp is naive."""
            rule.comparison_value = datetime.datetime.now()
            with pytest.raises(ValueError) as e:
                _ = rule._get_comparison()
            assert "aware" in str(e.value)


class TestLogical:
    """Test ``sfini.state.choice.Logical``."""
    @pytest.fixture
    def choice_rule_mock(self):
        """ChoiceRule mock."""
        return mock.Mock(spec=tscr.ChoiceRule)

    class TestGetRulDefn:
        """Passed choice-rule defeinition building."""
        def test_no_next_state(self):
            """Choice-rule has no next-state."""
            rule = mock.Mock(spec=tscr.ChoiceRule)
            rule.next_state = None
            rule.to_dict.return_value = {"Comparison": 42}
            res = tscr.Logical._get_rule_defn(rule)
            assert res == {"Comparison": 42}
            rule.to_dict.assert_called_once_with()

        def test_with_next_state(self):
            """Choice-rule has next-state."""
            rule = mock.Mock(spec=tscr.ChoiceRule)
            rule.next_state = mock.Mock(spec=sfini.state.State)
            rule.to_dict.return_value = {"Comparison": 42, "Next": "spam"}
            with pytest.raises(RuntimeError) as e:
                _ = tscr.Logical._get_rule_defn(rule)
            assert "top" in str(e.value)
            assert "next" in str(e.value)
            rule.to_dict.assert_not_called()


class TestNonUnary:
    """Test ``sfini.state.choice._NonUnary``."""
    @pytest.fixture
    def choice_rule_mocks(self):
        """ChoiceRule mocks."""
        return [mock.Mock(spec=tscr.ChoiceRule) for _ in range(4)]

    @pytest.fixture
    def rule(self, state_mock, choice_rule_mocks):
        """An example _NonUnary instance."""
        return tscr._NonUnary(choice_rule_mocks, next_state=state_mock)

    def test_init(self, rule, choice_rule_mocks, state_mock):
        """_NonUnary initialisation."""
        assert rule.choice_rules == choice_rule_mocks
        assert rule.next_state is state_mock

    def test_str(self, rule, choice_rule_mocks, state_mock):
        """_NonUnary stringification."""
        res = str(rule)
        assert "_NonUnary" in res
        assert all(str(r) in res for r in choice_rule_mocks)
        assert str(state_mock) in res

    class TestGetComparison:
        """Logical operation arguments construction."""
        def test_provided_rules(self, rule, choice_rule_mocks):
            """Non-zero number of choice-rules."""
            rule_defns = [
                {"A": 42, "Variable": "$.varPathA"},
                {"B": "spam", "Variable": "$.varPathB"},
                {"C": [
                    {"Foo": 1, "Variable": "$.varPathFoo"},
                    {"Bar": 2, "Variable": "$.varPathBar"}]},
                {"D": {"E": "bla", "Variable": "$.varPathE"}}]
            rule._get_rule_defn = mock.Mock(side_effect=rule_defns)
            exp_grd_calls = [mock.call(r) for r in choice_rule_mocks]
            res = rule._get_comparison()
            assert res == rule_defns
            assert rule._get_rule_defn.call_args_list == exp_grd_calls

        def test_no_provided_rules(self, rule):
            """Zero choice-rules."""
            rule.choice_rules = []
            rule._get_rule_defn = mock.Mock()
            with pytest.raises(ValueError):
                _ = rule._get_comparison()
            rule._get_rule_defn.assert_not_called()


class TestNot:
    """Test ``sfini.state.choice.Not``."""
    @pytest.fixture
    def choice_rule_mock(self):
        """ChoiceRule mock."""
        return mock.Mock(spec=tscr.ChoiceRule)

    @pytest.fixture
    def rule(self, state_mock, choice_rule_mock):
        """An example Not instance."""
        return tscr.Not(choice_rule_mock, next_state=state_mock)

    def test_init(self, rule, choice_rule_mock, state_mock):
        """Not initialisation."""
        assert rule.choice_rule is choice_rule_mock
        assert rule.next_state is state_mock

    def test_str(self, rule, choice_rule_mock, state_mock):
        """Not stringification."""
        res = str(rule)
        assert "Not" in res
        assert str(choice_rule_mock) in res
        assert str(state_mock) in res

    def test_provided_rules(self, rule, choice_rule_mock):
        """Logical operation arguments construction."""
        rule_defn = {"A": 42, "Variable": "$.varPathA"}
        rule._get_rule_defn = mock.Mock(return_value=rule_defn)
        res = rule._get_comparison()
        assert res == rule_defn
        rule._get_rule_defn.assert_called_once_with(choice_rule_mock)


@pytest.mark.parametrize(
    ("klass", "exp"),
    [
        (tscr.Comparison, tscr.ChoiceRule),
        (tscr.BooleanEquals, tscr.Comparison),
        (tscr.NumericEquals, tscr.Comparison),
        (tscr.NumericGreaterThan, tscr.Comparison),
        (tscr.NumericGreaterThanEquals, tscr.Comparison),
        (tscr.NumericLessThan, tscr.Comparison),
        (tscr.NumericLessThanEquals, tscr.Comparison),
        (tscr.StringEquals, tscr.Comparison),
        (tscr.StringGreaterThan, tscr.Comparison),
        (tscr.StringGreaterThanEquals, tscr.Comparison),
        (tscr.StringLessThan, tscr.Comparison),
        (tscr.StringLessThanEquals, tscr.Comparison),
        (tscr._TimestampRule, tscr.Comparison),
        (tscr.TimestampEquals, tscr._TimestampRule),
        (tscr.TimestampGreaterThan, tscr._TimestampRule),
        (tscr.TimestampGreaterThanEquals, tscr._TimestampRule),
        (tscr.TimestampLessThan, tscr._TimestampRule),
        (tscr.TimestampLessThanEquals, tscr._TimestampRule),
        (tscr.Logical, tscr.ChoiceRule),
        (tscr._NonUnary, tscr.Logical),
        (tscr.Not, tscr.Logical),
        (tscr.And, tscr._NonUnary),
        (tscr.Or, tscr._NonUnary)])
def test_inheritence(klass, exp):
    """Comparison choice-rules are correct subclass."""
    assert issubclass(klass, exp)


@pytest.mark.parametrize(
    ("klass", "exp"),
    [
        (tscr.BooleanEquals, bool),
        (tscr.NumericEquals, (float, int)),
        (tscr.NumericGreaterThan, (float, int)),
        (tscr.NumericGreaterThanEquals, (float, int)),
        (tscr.NumericLessThan, (float, int)),
        (tscr.NumericLessThanEquals, (float, int)),
        (tscr.StringEquals, str),
        (tscr.StringGreaterThan, str),
        (tscr.StringGreaterThanEquals, str),
        (tscr.StringLessThan, str),
        (tscr.StringLessThanEquals, str),
        (tscr.TimestampEquals, datetime.datetime),
        (tscr.TimestampGreaterThan, datetime.datetime),
        (tscr.TimestampGreaterThanEquals, datetime.datetime),
        (tscr.TimestampLessThan, datetime.datetime),
        (tscr.TimestampLessThanEquals, datetime.datetime)])
def test_expected_value_type(klass, exp):
    """Comparison choice-rules have expected value type."""
    assert klass._expected_value_type == exp


@pytest.mark.parametrize(
    "klass",
    [
        tscr.BooleanEquals,
        tscr.NumericEquals,
        tscr.NumericGreaterThan,
        tscr.NumericGreaterThanEquals,
        tscr.NumericLessThan,
        tscr.NumericLessThanEquals,
        tscr.StringEquals,
        tscr.StringGreaterThan,
        tscr.StringGreaterThanEquals,
        tscr.StringLessThan,
        tscr.StringLessThanEquals,
        tscr.TimestampEquals,
        tscr.TimestampGreaterThan,
        tscr.TimestampGreaterThanEquals,
        tscr.TimestampLessThan,
        tscr.TimestampLessThanEquals,
        tscr.And,
        tscr.Or,
        tscr.Not])
def test_final(klass):
    """Concrete choice-rules are finalised."""
    assert klass._final is True
