"""SFN choice rules.

These rules are used in the 'Choice' state of a state-machine, and
allow for conditional branching in the state-machine. There are two
types of choice rule: comparisons and logical operations.
"""

import datetime
import typing as T
import logging as lg

from .. import _util

_logger = lg.getLogger(__name__)


class ChoiceRule:
    """A choice case for the 'Choice' state.

    Args:
        next_state (sfini.state.State): state to execute on success
    """

    _final = False

    def __init__(self, next_state=None):
        self.next_state = next_state

    __repr__ = _util.easy_repr

    def _get_comparison(self) -> _util.JSONable:
        """Get this rule's comparison.

        Returns:
            comparison definition
        """

        raise NotImplementedError

    def to_dict(self) -> T.Dict[str, _util.JSONable]:
        """Convert this rule to a definition dictionary.

        Returns:
            definition
        """

        op_name = type(self).__name__
        if not self._final:
            raise RuntimeError(f"'{op_name}' is not a valid choice rule")
        comp = self._get_comparison()
        defn = {op_name: comp}
        if self.next_state:
            defn["Next"] = self.next_state.name
        return defn


class Comparison(ChoiceRule):
    """Compare variable value.

    Args:
        variable_path: path of variable to compare
        comparison_value: value to compare against
        next_state: state to execute on success
    """

    _expected_value_type = None

    def __init__(
            self,
            variable_path: str,
            comparison_value,
            next_state=None):
        super().__init__(next_state)
        self.variable_path = variable_path
        self.comparison_value = comparison_value

    def __str__(self):
        has_no_next = self.next_state is None
        next_state_str = "" if has_no_next else f" -> {self.next_state}"
        return (
            f"'{self.variable_path}' {type(self).__name__} "
            f"{self.comparison_value}{next_state_str}")

    def _get_comparison(self):
        if not isinstance(self.comparison_value, self._expected_value_type):
            raise TypeError(
                f"Comparison value must be type `{self.comparison_value}`: "
                f"{self._expected_value_type.__name__}")
        return self.comparison_value

    def to_dict(self):
        defn = super().to_dict()
        defn["Variable"] = self.variable_path
        return defn


class BooleanEquals(Comparison):
    _final = True
    _expected_value_type = bool


class NumericEquals(Comparison):
    _final = True
    _expected_value_type = (float, int)


class NumericGreaterThan(Comparison):
    _final = True
    _expected_value_type = (float, int)


class NumericGreaterThanEquals(Comparison):
    _final = True
    _expected_value_type = (float, int)


class NumericLessThan(Comparison):
    _final = True
    _expected_value_type = (float, int)


class NumericLessThanEquals(Comparison):
    _final = True
    _expected_value_type = (float, int)


class StringEquals(Comparison):
    _final = True
    _expected_value_type = str


class StringGreaterThan(Comparison):
    _final = True
    _expected_value_type = str


class StringGreaterThanEquals(Comparison):
    _final = True
    _expected_value_type = str


class StringLessThan(Comparison):
    _final = True
    _expected_value_type = str


class StringLessThanEquals(Comparison):
    _final = True
    _expected_value_type = str


class _TimestampRule(Comparison):
    _expected_value_type = datetime.datetime

    def _get_comparison(self) -> str:
        dt = super()._get_comparison()
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            raise ValueError("Comparison time must be aware")
        return dt.isoformat("T")


class TimestampEquals(_TimestampRule):
    _final = True


class TimestampGreaterThan(_TimestampRule):
    _final = True


class TimestampGreaterThanEquals(_TimestampRule):
    _final = True


class TimestampLessThan(_TimestampRule):
    _final = True


class TimestampLessThanEquals(_TimestampRule):
    _final = True


class Logical(ChoiceRule):
    @staticmethod
    def _get_rule_defn(choice_rule: ChoiceRule) -> T.Dict[str, _util.JSONable]:
        """Get choice rule definition.

        Args:
            choice_rule: choice rule to process

        Returns:
            choice rule definition
        """

        if choice_rule.next_state is not None:
            msg = "Only top-level choice rules can have next state"
            raise RuntimeError(msg)
        return choice_rule.to_dict()


class _NonUnary(Logical):
    """Logical operation on choice rules.

    Args:
        choice_rules: choice rules to operate on
        next_state: state to execute on success
    """

    def __init__(self, choice_rules: T.List[ChoiceRule], next_state=None):
        super().__init__(next_state=next_state)
        self.choice_rules = choice_rules

    def __str__(self):
        _t = f" {type(self).__name__} "
        _s = _t.join(f"({r})" for r in self.choice_rules)
        _n = "" if self.next_state is None else f" -> {self.next_state}"
        return _s + _n

    def _get_comparison(self) -> T.List[T.Dict[str, _util.JSONable]]:
        if not self.choice_rules:
            msg = "Must provide at least one choice-rule to logical choice"
            raise ValueError(msg)
        return [self._get_rule_defn(r) for r in self.choice_rules]


class And(_NonUnary):
    _final = True


class Or(_NonUnary):
    _final = True


class Not(Logical):
    """Logical 'not' operation on a choice rule.

    Args:
        choice_rule: choice rule to operate on
        next_state: state to execute on success
    """

    _final = True

    def __init__(self, choice_rule: ChoiceRule, next_state=None):
        super().__init__(next_state=next_state)
        self.choice_rule = choice_rule

    def __str__(self):
        _n = "" if self.next_state is None else f" -> {self.next_state}"
        return f"{type(self).__name__} {self.choice_rule}{_n}"

    def _get_comparison(self) -> T.Dict[str, _util.JSONable]:
        return self._get_rule_defn(self.choice_rule)
