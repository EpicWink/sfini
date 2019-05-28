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

from botocore import exceptions as bc_exc

from . import _util
from . import execution as sfini_execution
from . import state as sfini_state

_logger = lg.getLogger(__name__)
_default = _util.DefaultParameter()


class StateMachine:
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
        self._creation_date = None

    def __str__(self):
        return "'%s' (%d states)" % (self.name, len(self.states))

    __repr__ = _util.easy_repr

    @classmethod
    def from_arn(cls, arn: str, *, session: _util.AWSSession = None):  # TODO: unit-test
        """State-machine from ARN.

        Args:
            arn: state-machine ARN
            session: session to use for AWS communication
        """

        name = arn.split(":", 6)[6]
        self = cls(name, _default, _default, session=session)
        assert self.arn == arn
        return self

    @classmethod
    def from_list_item(  # TODO: unit-test
            cls,
            item: T.Dict[str, T.Any],
            *,
            session: _util.AWSSession = None):
        """State-machine from a response state-machine list-item.

        Args:
            item: state-machine list-item
            session: session to use for AWS communication
        """

        self = cls.from_arn(item["stateMachineArn"], session=session)
        assert self.name == item["name"]
        self._creation_date = item["creationDate"]
        return self

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

    @property
    def creation_date(self) -> datetime.datetime:  # TODO: unit-test
        """State-machine creation date."""
        if self._creation_date is None:
            self.update()
        return self._creation_date

    def update(self):  # TODO: unit-test
        """Update state-machine details from AWS."""
        cdate_known = self._creation_date is not None
        definition_known = self.states != _default
        start_known = self.start_state != _default
        if cdate_known and definition_known and start_known:
            _logger.debug("State-machine specified: update is unnecessary")
            return
        resp = self.session.sfn.describe_state_machine(
            stateMachineArn=self.arn)
        assert resp["stateMachineArn"] == self.arn
        assert resp["name"] == self.name
        self._creation_date = resp["creationDate"]
        defn = resp["definition"]
        self.start_state = defn["StartAt"]
        self.comment = defn.get("Comment", _default)
        self.timeout = defn.get("TimeoutSeconds", _default)
        states_definitions = json.loads(defn["States"])
        self.states = states_definitions  # TODO: construct states
        # self.states = sfini_state.construct_states(states_definitions)

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
        try:
            self.update()
        except bc_exc.ClientError as e:
            if e.response["Error"]["Code"] != "StateMachineDoesNotExist":
                raise
            return False
        return True

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
            execution_input=execution_input,
            session=self.session)
        execution.start()
        return execution

    def list_executions(
            self,
            status: str = None
    ) -> T.List[sfini_execution.Execution]:
        """List all executions of this state-machine.

        This state-machine is manually attached to the ``state_machine``
        attribute of the resultant executions here.

        Args:
            status: only list executions with this status. Choose from
                'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT' or 'ABORTED'

        Returns:
            executions of this state-machine
        """

        return sfini_execution.list_executions(self.arn, status=status)


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


def list_state_machines(  # TODO: unit-test
        *,
        session: _util.AWSSession = None
) -> T.List[StateMachine]:
    """List state-machines.

    Args:
        session: session to use for AWS communication

    Returns:
        all registered state-machines
    """

    _logger.info("Listing state-machines")
    session = session or _util.AWSSession()
    resp = _util.collect_paginated(session.sfn.list_state_machines)
    state_machines = []
    for item in resp["stateMachines"]:
        state_machine = StateMachine.from_list_item(item, session=session)
        state_machines.append(state_machine)
    return state_machines


def get_state_machine_with_name(  # TODO: unit-test
        name: str,
        *,
        session: _util.AWSSession = None
) -> StateMachine:
    """Get a state-machine.

    Args:
        name: state-machine name
        session: session to use for AWS communication

    Returns:
        state-machine with given name
    """

    state_machines = list_state_machines(session=session)
    names = [sm.name for sm in state_machines]
    sm_idx = names.index(name)
    return state_machines[sm_idx]
