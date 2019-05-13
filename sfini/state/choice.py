# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

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


class ChoiceRule:  # TODO: unit-test
    """A choice case for the 'Choice' state.

    Args:
        next_state (sfini.state.State): state to execute on success
    """

    def __init__(self, next_state):
        self.next_state = next_state

    def to_dict(self) -> T.Dict[str, _util.JSONable]:
        """Convert this rule to a definition dictionary.

        Returns:
            definition
        """

        raise NotImplementedError


class Comparison(ChoiceRule):  # TODO: unit-test
    def __init__(
            self,
            variable_name: str,
            comparison_value,
            next_state):
        super().__init__(next_state)
        self.variable_name = variable_name
        self.comparison_value = comparison_value

    def __str__(self):
        return "'%s' %s %s [%s]" % (
            self.variable_name,
            type(self).__name__,
            self.comparison_value,
            self.next_state)

    def __repr__(self):
        return "%s(%s, %s, %s)" % (
            type(self).__name__,
            repr(self.variable_name),
            repr(self.comparison_value),
            repr(self.next_state))

    def to_dict(self):
        op_name = type(self).__name__
        if op_name.startswith("_"):
            raise RuntimeError("'%s' is not a valid choice rule" % op_name)
        return {
            "Variable": self.variable_name,
            op_name: self.comparison_value,
            "Next": self.next_state.name}


class BooleanEquals(Comparison):  # TODO: unit-test
    """Compare boolean variable value.

    Args:
        variable_name: name of variable to compare
        comparison_value: value to compare against
        next_state: state to execute on success
    """

    def __init__(self, variable_name: str, comparison_value: bool, next_state):
        super().__init__(variable_name, comparison_value, next_state)
        if not isinstance(comparison_value, bool):
            raise TypeError("Boolean comparison value must be `bool`")


class _NumericRule(Comparison):  # TODO: unit-test
    """Compare numeric variable value.

    Args:
        variable_name: name of variable to compare
        comparison_value: value to compare against
        next_state: state to execute on success
    """

    def __init__(
            self,
            variable_name: str,
            comparison_value: T.Union[int, float],
            next_state):
        super().__init__(variable_name, comparison_value, next_state)
        if not isinstance(comparison_value, (int, float)):
            _s = "Numeric comparison value must be `int` or `float`"
            raise TypeError(_s)


class NumericEquals(_NumericRule):
    pass


class NumericGreaterThan(_NumericRule):
    pass


class NumericGreaterThanEquals(_NumericRule):
    pass


class NumericLessThan(_NumericRule):
    pass


class NumericLessThanEquals(_NumericRule):
    pass


class _StringRule(Comparison):  # TODO: unit-test
    """Compare string variable value.

    Args:
        variable_name: name of variable to compare
        comparison_value: value to compare against
        next_state: state to execute on success
    """

    def __init__(
            self,
            variable_name: str,
            comparison_value: str,
            next_state):
        super().__init__(variable_name, comparison_value, next_state)
        if not isinstance(comparison_value, str):
            raise TypeError("String comparison value must be `str`")


class StringEquals(_StringRule):
    pass


class StringGreaterThan(_StringRule):
    pass


class StringGreaterThanEquals(_StringRule):
    pass


class StringLessThan(_StringRule):
    pass


class StringLessThanEquals(_StringRule):
    pass


class _TimestampRule(Comparison):  # TODO: unit-test
    """Compare date/time variable value.

    Args:
        variable_name: name of variable to compare
        comparison_value: value to compare against
        next_state: state to execute on success
    """

    def __init__(
            self,
            variable_name: str,
            comparison_value: datetime.datetime,
            next_state):
        super().__init__(variable_name, comparison_value, next_state)
        if not isinstance(comparison_value, datetime.datetime):
            _s = "Timestamp comparison value must be `datetime.datetime`"
            raise TypeError(_s)

    def to_dict(self):
        op_name = type(self).__name__
        if op_name.startswith("_"):
            raise RuntimeError("'%s' is not a valid choice rule")
        t = self.comparison_value
        if t.tzinfo is None or t.tzinfo.utcoffset(t) is None:
            raise ValueError("Comparison time must be aware")
        return {
            "Variable": self.variable_name,
            op_name: t.isoformat("T"),
            "Next": self.next_state.name}


class TimestampEquals(_TimestampRule):
    pass


class TimestampGreaterThan(_TimestampRule):
    pass


class TimestampGreaterThanEquals(_TimestampRule):
    pass


class TimestampLessThan(_TimestampRule):
    pass


class TimestampLessThanEquals(_TimestampRule):
    pass


class Logical(ChoiceRule):  # TODO: unit-test
    """Logical operation on choice rules.

    Args:
        choice_rules: choice rules to operate on
        next_state: state to execute on success
    """

    def __init__(self, choice_rules: T.List[Comparison], next_state):
        super().__init__(next_state)
        self.choice_rules = choice_rules

    def __str__(self):
        _t = " %s " % type(self).__name__
        _s = _t.join("(%s)" % r for r in self.choice_rules)
        return _s + " [%s]" % self.next_state

    def __repr__(self):
        return "%s(%s, %s)" % (
            type(self).__name__,
            repr(self.choice_rules),
            repr(self.next_state))

    def _get_choice_rule_defns(self) -> T.List[T.Dict[str, _util.JSONable]]:
        """Build choice rule definitions.

        Returns:
            choice rule definitions
        """

        choice_rule_defns = []
        for choice_rule in self.choice_rules:
            defn = choice_rule.to_dict().copy()
            del defn["Next"]
            choice_rule_defns.append(defn)
        return choice_rule_defns

    def to_dict(self):
        op_name = type(self).__name__
        if op_name.startswith("_"):
            raise RuntimeError("'%s' is not a valid choice rule")
        choice_rule_defns = self._get_choice_rule_defns()
        return {op_name: choice_rule_defns, "Next": self.next_state.name}


class And(Logical):
    pass


class Or(Logical):
    pass


class Not(Logical):  # TODO: unit-test
    """Logical 'not' operation on a choice rule.

    Args:
        choice_rule: choice rule to operate on
        next_state: state to execute on success
    """

    def __init__(self, choice_rule: Comparison, next_state):
        super().__init__([choice_rule], next_state)

    def __str__(self):
        _f = (type(self).__name__, self.choice_rules[0], self.next_state)
        return "%s %s [%s]" % _f

    def __repr__(self):
        return "%s(%s, %s)" % (
            type(self).__name__,
            repr(self.choice_rules[0]),
            repr(self.next_state))

    def _get_choice_rule_defns(self) -> T.Dict[str, _util.JSONable]:
        defn = self.choice_rules[0].to_dict().copy()
        del defn["Next"]
        return defn
