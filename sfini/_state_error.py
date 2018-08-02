# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/08/02

"""SFN state execution error utitilies."""

import logging as lg

_logger = lg.getLogger(__name__)


class WorkerCancel(KeyboardInterrupt):  # TODO: unit-test
    """Workflow execution interrupted by user."""
    def __init__(self, *args, **kwargs):
        _msg = (
            "Activity execution cancelled by user. "
            "This could be due to a `KeyboardInterrupt` during execution, "
            "or the worker was killed during task polling.")
        super().__init__(_msg, *args, **kwargs)


class _ExceptionCondition:  # TODO: unit-test
    @staticmethod
    def _process_exc(exc):
        """Process exception condition.

        Args:
            exc (type or str): exception type-name

        Returns:
            type or str: process exception

        Raises:
            ValueError: bad type-name
        """

        errs = ("*", "ALL", "Timeout", "TaskFailed", "Permissions")

        if isinstance(exc, str):
            if exc not in errs:
                _s = "Error name was '%s', must be one of: %s"
                raise ValueError(_s % (exc, errs))
            return "ALL" if exc == "*" else exc
        elif issubclass(exc, Exception):
            return exc
        else:
            raise TypeError("Error must be exception or predefined string")

    @staticmethod
    def _rules_similar(rule_a, rule_b):
        """Check if rules are similar.

        Args:
            rule_a: LHS rule
            rule_b: RHS rule

        Returns:
            bool: rules are similar
        """

        raise NotImplementedError

    def _collapse_conditions(self, rules):
        """Combine exception rules into lists of exceptions.

        Puts "States.ALL" at the end separately, if it exists.

        Args:
            rules (dict[type or str]): exception rules to group

        Returns:
            list[dict]: rule groups, names put in ``excs``
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

    def _excs_to_errors(self, excs):
        """Convert exceptions to error codes.

        Arguments:
            excs (list[type or str]): exception conditions

        Returns:
            list[str]: corresponding error codes
        """

        errors = []
        for exc in excs:
            if isinstance(exc, str):
                _ = self._process_exc(exc)
                error = "States." + exc
            elif issubclass(exc, Exception):
                error = str(exc)
            else:
                raise TypeError("Error must be exception or accepted string")
            errors.append(error)
        return errors

    @staticmethod
    def _rule_defn(rule):
        """Get extra definition details from a rule.

        Args:
            rule: exception rule

        Returns:
            dict: extra definition details
        """

        raise NotImplementedError

    def _rule_defns(self, conditions):
        """Build exception rule definitions.

        Collapses rules by similarity into groups, converts exception
        conditions to error codes, then gets the definition for each rule.

        Args:
            conditions (dict[str]): conditions produce definitions from

        Returns:
            list[dict]: definitions
        """

        cond_groups = self._collapse_conditions(conditions)

        defns = []
        for cond_group in cond_groups:
            errors = self._excs_to_errors(cond_group["excs"])
            defn = {"ErrorEquals": errors}
            defn.update(self._rule_defn(cond_group["rule"]))
            defns.append(defn)

        return defns
