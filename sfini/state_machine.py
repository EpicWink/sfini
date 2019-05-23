# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""State-machine interfacing.

A state-machine defines the logic for a workflow of an application. It
is comprised of states (ie stages), and executions of which will run
the workflow over some given data.
"""

import json
import uuid
import datetime
import typing as T
import logging as lg

from . import _util
from . import execution as sfini_execution
from . import state as sfini_state

_logger = lg.getLogger(__name__)
_default = _util.DefaultParameter()


class StateMachine:  # TODO: unit-test
    """State machine structure for AWS Step Functions.

    Args:
        name: name of state-machine
        states: state-machine states
        start_state: name of start state
        comment: description of state-maching
        timeout: execution time-out (seconds)
        session: session to use for AWS communication
    """

    _execution_class = sfini_execution.Execution

    def __init__(
            self,
            name: str,
            states: T.Dict[str, sfini_state.State],
            start_state: str,
            comment: str = _default,
            timeout: int = _default,
            *,
            session: _util.AWSSession = None):
        self.name = name
        self.states = states
        self.start_state = start_state
        self.comment = comment
        self.timeout = timeout
        self.session = session or _util.AWSSession()

    def __str__(self):
        return "'%s' (%d states)" % (self.name, len(self.states))

    __repr__ = _util.easy_repr

    @_util.cached_property
    def arn(self) -> str:
        """State-machine generated ARN."""
        region = self.session.region
        account = self.session.account_id
        fmt = "arn:aws:states:%s:%s:stateMachine:%s"
        return fmt % (region, account, self.name)

    @_util.cached_property
    def default_role_arn(self) -> str:
        """``sfini``-generated state-machine IAM role ARN."""
        return "arn:aws:iam::%s:role/sfiniGenerated" % self.session.account_id

    def to_dict(self) -> T.Dict[str, _util.JSONable]:
        """Convert this state-machine to a definition dictionary.

        Returns:
            definition
        """

        _logger.debug("Converting '%s' to dictionary" % self)
        state_defns = {n: s.to_dict() for n, s in self.states.items()}
        defn = {"StartAt": self.start_state, "States": state_defns}
        if self.comment != _default:
            defn["Comment"] = self.comment
        if self.timeout != _default:
            defn["TimeoutSeconds"] = self.timeout
        return defn

    def is_registered(self) -> bool:
        """See if this state-machine is registered with AWS SFN.

        Returns:
            if this state-machine is registered
        """

        _logger.debug("Testing for registration of '%s' on SFN" % self)
        resp = _util.collect_paginated(self.session.sfn.list_state_machines)
        arns = {sm["stateMachineArn"] for sm in resp["stateMachines"]}
        return self.arn in arns

    def _sfn_create(self, role_arn: str):
        """Create this state-machine in AWS SFN.

        Args:
            role_arn: state-machine IAM role ARN
        """

        _logger.info("Creating '%s' on SFN" % self)
        resp = self.session.sfn.create_state_machine(
            name=self.name,
            definition=json.dumps(self.to_dict(), indent=4),
            roleArn=role_arn)
        assert resp["stateMachineArn"] == self.arn
        fmt = "State-machine '%s' registered with ARN '%s' at %s"
        _logger.info(fmt % (self, self.arn, resp["creationDate"]))

    def _sfn_update(self, role_arn: str = None):
        """Update this state-machine in AWS SFN.

        Args:
            role_arn: state-machine IAM role ARN to update to
        """

        _logger.info("Updating '%s' on SFN" % self)
        kwargs = {} if role_arn is None else {"roleArn": role_arn}
        resp = self.session.sfn.update_state_machine(
            definition=json.dumps(self.to_dict(), indent=4),
            stateMachineArn=self.arn,
            **kwargs)
        _logger.info("'%s' updated at %s" % (self, resp["updateDate"]))

    def register(self, role_arn: str = None, allow_update: bool = False):
        """Register state-machine with AWS SFN.

        Args:
            role_arn: state-machine IAM role ARN
            allow_update: allow overwriting of an existing state-machine with
                the same name
        """

        _util.assert_valid_name(self.name)
        if allow_update and self.is_registered():
            self._sfn_update(role_arn)
        else:
            self._sfn_create(role_arn or self.default_role_arn)

    def deregister(self):
        """Remove state-machine from AWS SFN."""
        _logger.info("Deleting state-machine '%s' from SFN" % self)
        self.session.sfn.delete_state_machine(stateMachineArn=self.arn)

    def start_execution(
            self,
            execution_input: _util.JSONable
    ) -> _execution_class:
        """Start an execution.

        Args:
            execution_input: input to first state in state-machine

        Returns:
            started execution
        """

        fmt = "Starting execution of '%s' with: %s"
        _logger.info(fmt % (self, execution_input))

        _now = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        name = self.name + "_" + _now + "_" + str(uuid.uuid4())[:8]
        execution = self._execution_class(
            name,
            self.arn,
            execution_input,
            session=self.session)
        execution.start()
        return execution

    def _build_executions(
            self,
            items: T.List[T.Dict[str, _util.JSONable]]
    ) -> T.List[_execution_class]:
        """Build executions from response list-items.

        This state-machine is manually attached to the ``state_machine``
        attribute of the resultant executions here.

        Arguments:
            items: execution list-items

        Returns:
            constructed executions
        """

        executions = []
        for item in items:
            assert item["stateMachineArn"] == self.arn
            execution = self._execution_class.from_execution_list_item(
                item,
                session=self.session)
            execution.state_machine = self
            executions.append(execution)

            fmt = "Found execution '%s' with stop-date: %s"
            _logger.debug(fmt % (execution, item.get("stopDate")))
        return executions

    def list_executions(self, status: str = None) -> T.List[_execution_class]:
        """List all executions of this state-machine.

        This state-machine is manually attached to the ``state_machine``
        attribute of the resultant executions here.

        Args:
            status: only list executions with this status. Choose from
                'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT' or 'ABORTED'

        Returns:
            executions of this state-machine
        """

        _s = " with status '%s'" % status if status else ""
        _logger.info("Listing executions of '%s'" % self + _s)

        kwargs = {"stateMachineArn": self.arn}
        if status is not None:
            kwargs["statusFilter"] = status
        fn = self.session.sfn.list_executions
        resp = _util.collect_paginated(fn, **kwargs)
        return self._build_executions(resp["executions"])


def construct_state_machine(
        name: str,
        start_state: sfini_state.State,
        comment: str = _default,
        timeout: int = _default,
        *,
        session: _util.AWSSession = None
) -> StateMachine:
    """Construct a state-machine from the starting state.

    Make sure to construct the state-machine after all states have been
    defined: subsequent states will need to be added to the state-machine
    manually.

    Only states referenced by the provided first state (and their children)
    will be in the state-machine definition. Add states via an impossible
    choice rule to include them in the definition.

    Args:
        name: name of state-machine
        start_state: starting state of state-machine
        comment: description of state-maching
        timeout: execution time-out (seconds)
        session: session to use for AWS communication

    Returns:
        constructed state-machine
    """

    states = {}
    start_state.add_to(states)
    return StateMachine(
        name,
        states,
        start_state.name,
        comment=comment,
        timeout=timeout,
        session=session)
