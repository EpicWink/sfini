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
            raise RuntimeError("'%s' is not a valid choice rule" % op_name)
        comp = self._get_comparison()
        defn = {op_name: comp}
        if self.next_state:
            defn["Next"] = self.next_state.name
        return defn


class Comparison(ChoiceRule):  # TODO: unit-test
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
        return "'%s' %s %s%s" % (
            self.variable_path,
            type(self).__name__,
            self.comparison_value,
            "" if self.next_state is None else (" -> %s" % self.next_state))

    def _get_comparison(self):
        if not isinstance(self.comparison_value, self._expected_value_type):
            fmt = "Comparison value must be type `%s`: %s"
            exp_type_name = self._expected_value_type.__name__
            raise TypeError(fmt % (self.comparison_value, exp_type_name))
        return self.comparison_value

    def to_dict(self):
        defn = super().to_dict()
        defn["Variable"] = self.variable_path
        return defn


class BooleanEquals(Comparison):  # TODO: unit-test
    _final = True
    _expected_value_type = bool


class NumericEquals(Comparison):  # TODO: unit-test
    _final = True
    _expected_value_type = (float, int)


class NumericGreaterThan(Comparison):  # TODO: unit-test
    _final = True
    _expected_value_type = (float, int)


class NumericGreaterThanEquals(Comparison):  # TODO: unit-test
    _final = True
    _expected_value_type = (float, int)


class NumericLessThan(Comparison):  # TODO: unit-test
    _final = True
    _expected_value_type = (float, int)


class NumericLessThanEquals(Comparison):  # TODO: unit-test
    _final = True
    _expected_value_type = (float, int)


class StringEquals(Comparison):  # TODO: unit-test
    _final = True
    _expected_value_type = str


class StringGreaterThan(Comparison):  # TODO: unit-test
    _final = True
    _expected_value_type = str


class StringGreaterThanEquals(Comparison):  # TODO: unit-test
    _final = True
    _expected_value_type = str


class StringLessThan(Comparison):  # TODO: unit-test
    _final = True
    _expected_value_type = str


class StringLessThanEquals(Comparison):  # TODO: unit-test
    _final = True
    _expected_value_type = str


class _TimestampRule(Comparison):  # TODO: unit-test
    _expected_value_type = datetime.datetime

    def _get_comparison(self) -> str:
        dt = super()._get_comparison()
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            raise ValueError("Comparison time must be aware")
        return dt.isoformat("T")


class TimestampEquals(_TimestampRule):  # TODO: unit-test
    _final = True


class TimestampGreaterThan(_TimestampRule):  # TODO: unit-test
    _final = True


class TimestampGreaterThanEquals(_TimestampRule):  # TODO: unit-test
    _final = True


class TimestampLessThan(_TimestampRule):  # TODO: unit-test
    _final = True


class TimestampLessThanEquals(_TimestampRule):  # TODO: unit-test
    _final = True


class Logical(ChoiceRule):  # TODO: unit-test
    @staticmethod
    def _get_rule_defn(choice_rule: ChoiceRule) -> T.Dict[str, _util.JSONable]:
        """Get choice rule definition.

        Arguments:
            choice_rule: choice rule to process

        Returns:
            choice rule definition
        """

        if choice_rule.next_state is not None:
            msg = "Only top-level choice rules can have next state"
            raise RuntimeError(msg)
        return choice_rule.to_dict()


class _NonUnary(Logical):  # TODO: unit-test
    """Logical operation on choice rules.

    Args:
        choice_rules: choice rules to operate on
        next_state: state to execute on success
    """

    def __init__(self, choice_rules: T.List[ChoiceRule], next_state=None):
        super().__init__(next_state=next_state)
        self.choice_rules = choice_rules

    def __str__(self):
        _t = " %s " % type(self).__name__
        _s = _t.join("(%s)" % r for r in self.choice_rules)
        _n = "" if self.next_state is None else (" -> %s" % self.next_state)
        return _s + _n

    def _get_comparison(self) -> T.List[T.Dict[str, _util.JSONable]]:
        if not self.choice_rules:
            msg = "Must provide at least one choice-rule to logical choice"
            raise ValueError(msg)
        return [self._get_rule_defn(r) for r in self.choice_rules]


class And(_NonUnary):  # TODO: unit-test
    _final = True


class Or(_NonUnary):  # TODO: unit-test
    _final = True


class Not(Logical):  # TODO: unit-test
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
        _n = "" if self.next_state is None else (" -> %s" % self.next_state)
        _f = (type(self).__name__, self.choice_rule, _n)
        return "%s %s%s" % _f

    def _get_comparison(self) -> T.Dict[str, _util.JSONable]:
        return self._get_rule_defn(self.choice_rule)
