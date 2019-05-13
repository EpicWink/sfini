# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/08/02

"""Comm state execution error utitilies."""

import typing as T
import logging as lg

from .. import _util

_logger = lg.getLogger(__name__)


class WorkerCancel(KeyboardInterrupt):  # TODO: unit-test
    """Workflow execution interrupted by user."""
    def __init__(self, *args, **kwargs):
        _msg = (
            "Activity execution cancelled by user. "
            "This could be due to a `KeyboardInterrupt` during execution, "
            "or the worker was killed during task polling.")
        super().__init__(_msg, *args, **kwargs)


class ExceptionCondition:  # TODO: unit-test
    """Exception condition and rule processing mix-in."""
    Exc = T.TypeVar("Exc", T.Type[BaseException], str)
    Rule = T.TypeVar("Rule", bound=_util.JSONable)

    states_errors = (
        "ALL",
        "Timeout",
        "TaskFailed",
        "Permissions",
        "ResultPathMatchFailure",
        "ParameterPathFailure",
        "BranchFailed",
        "NoChoiceMatched")

    def _process_exc(self, exc: Exc) -> Exc:
        """Process exception condition.

        Args:
            exc: exception type or type-name

        Returns:
            processed exception

        Raises:
            ValueError: bad type-name
        """

        errs = ("*",) + self.states_errors

        if isinstance(exc, str):
            if exc not in errs and not exc.startswith("Lambda."):
                _s = "Error name was '%s', must be one of: %s"
                raise ValueError(_s % (exc, self.states_errors))
            return "ALL" if exc == "*" else exc
        elif issubclass(exc, BaseException):
            return exc
        else:
            raise TypeError("Error must be exception or predefined string")

    @staticmethod
    def _rules_similar(rule_a: Rule, rule_b: Rule) -> bool:
        """Check if rules are similar.

        Args:
            rule_a: LHS rule
            rule_b: RHS rule

        Returns:
            rules are similar
        """

        raise NotImplementedError

    def _collapse_conditions(
            self,
            rules: T.Dict[Exc, Rule]
    ) -> T.List[T.Dict[str, T.Union[T.List[Exc], Rule]]]:
        """Combine exception rules into lists of exceptions.

        Puts "States.ALL" at the end separately, if it exists.

        Args:
            rules: exception rules to group

        Returns:
            rule groups, names put in ``excs``
        """

        all_rule = None
        rule_groups = []
        for exc, rule in rules.items():
            if exc == "ALL":
                all_rule = rule
                continue
            for rule_group in rule_groups:
                if self._rules_similar(rule, rule_group["rule"]):
                    rule_group["excs"].append(exc)
                    break
            else:
                rule_groups.append({"excs": [exc], "rule": rule})
        if all_rule is not None:
            rule_groups.append({"excs": ["ALL"], "rule": all_rule})
        return rule_groups

    def _excs_to_errors(self, excs: T.List[Exc]) -> T.List[str]:
        """Convert exceptions to error codes.

        Args:
            excs: exception conditions

        Returns:
            corresponding error codes
        """

        errors = []
        for exc in excs:
            if isinstance(exc, str):
                exc = self._process_exc(exc)
                error = "States." + exc
            elif issubclass(exc, Exception):
                error = type(exc).__name__
            else:
                raise TypeError("Error must be exception or accepted string")
            errors.append(error)
        return errors

    @staticmethod
    def _rule_defn(rule: Rule) -> T.Dict[str, _util.JSONable]:
        """Get extra definition details from a rule.

        Args:
            rule: exception rule

        Returns:
            dict: extra definition details
        """

        raise NotImplementedError

    def _rule_defns(
            self,
            conditions: T.Dict[Exc, Rule]
    ) -> T.List[T.Dict[str, _util.JSONable]]:
        """Build exception rule definitions.

        Collapses rules by similarity into groups, converts exception
        conditions to error codes, then gets the definition for each rule.

        Args:
            conditions: conditions produce definitions from

        Returns:
            definitions
        """

        cond_groups = self._collapse_conditions(conditions)

        defns = []
        for cond_group in cond_groups:
            errors = self._excs_to_errors(cond_group["excs"])
            defn = {"ErrorEquals": errors}
            defn.update(self._rule_defn(cond_group["rule"]))
            defns.append(defn)

        return defns
