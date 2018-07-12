# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN choice rule operations."""

import datetime
import logging as lg

_logger = lg.getLogger(__name__)


class _ChoiceRule:  # TODO: unit-test
    def __init__(self, variable_name, comparison_value, next_state):
        self.variable_name = variable_name
        self.comparison_value = comparison_value
        self.next_state = next_state

    def to_dict(self):
        """Convert this rule to a definition dictionary.

        Returns:
            dict: definition
        """

        op_name = type(self).__name__
        if op_name.startswith("_"):
            raise RuntimeError("'%s' is not a valid choice rule")
        return {
            "Variable": self.variable_name,
            op_name: self.comparison_value,
            "Next": self.next_state.name}


class BooleanEquals(_ChoiceRule):  # TODO: unit-test
    """Compare boolean variable value.

    Arguments:
        variable_name (str): name of variable to compare
        comparison_value (bool): value to compare against
        next_state (_state.State): state to execute on success
    """

    def __init__(self, variable_name, comparison_value, next_state):
        super().__init__(variable_name, comparison_value, next_state)
        if not isinstance(comparison_value, bool):
            raise TypeError("Boolean comparison value must be `bool`")


class _NumericRule(_ChoiceRule):  # TODO: unit-test
    """Compare numeric variable value.

    Arguments:
        variable_name (str): name of variable to compare
        comparison_value (int or float): value to compare against
        next_state (_state.State): state to execute on success
    """

    def __init__(self, variable_name, comparison_value, next_state):
        super().__init__(variable_name, comparison_value, next_state)
        if not isinstance(comparison_value, (int, float)):
            raise TypeError(
                "Numeric comparison value must be `int` or `float`")


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


class _StringdRule(_ChoiceRule):  # TODO: unit-test
    """Compare string variable value.

    Arguments:
        variable_name (str): name of variable to compare
        comparison_value (str): value to compare against
        next_state (_state.State): state to execute on success
    """

    def __init__(self, variable_name, comparison_value, next_state):
        super().__init__(variable_name, comparison_value, next_state)
        if not isinstance(comparison_value, str):
            raise TypeError("String comparison value must be `str`")


class StringEquals(_StringdRule):
    pass


class StringGreaterThan(_StringdRule):
    pass


class StringGreaterThanEquals(_StringdRule):
    pass


class StringLessThan(_StringdRule):
    pass


class StringLessThanEquals(_StringdRule):
    pass


class _TimestampdRule(_ChoiceRule):  # TODO: unit-test
    """Compare date/time variable value.

    Arguments:
        variable_name (str): name of variable to compare
        comparison_value (datetime.datetime): value to compare against
        next_state (_state.State): state to execute on success
    """

    def __init__(self, variable_name, comparison_value, next_state):
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


class TimestampEquals(_TimestampdRule):
    pass


class TimestampGreaterThan(_TimestampdRule):
    pass


class TimestampGreaterThanEquals(_TimestampdRule):
    pass


class TimestampLessThan(_TimestampdRule):
    pass


class TimestampLessThanEquals(_TimestampdRule):
    pass


class _LogicalRule:  # TODO: unit-test
    def __init__(self, choice_rules, next_state):
        self.choice_rules = choice_rules
        self.next_state = next_state

    def _get_choice_rule_defns(self):
        choice_rule_defns = []
        for choice_rule in self.choice_rules:
            defn = choice_rule.to_dict().copy()
            del defn["Next"]
            choice_rule_defns.append(defn)
        return choice_rule_defns

    def to_dict(self):
        """Convert this rule to a definition dictionary.

        Returns:
            dict: definition
        """

        op_name = type(self).__name__
        if op_name.startswith("_"):
            raise RuntimeError("'%s' is not a valid choice rule")
        choice_rule_defns = self._get_choice_rule_defns()
        return {op_name: choice_rule_defns, "Next": self.next_state.name}


class And(_LogicalRule):
    pass


class Or(_LogicalRule):
    pass


class Not(_LogicalRule):  # TODO: unit-test
    def __init__(self, choice_rule, next_state):
        super().__init__([choice_rule], next_state)

    def _get_choice_rule_defns(self):
        defn = self.choice_rules[0].to_dict().copy()
        del defn["Next"]
        return defn
