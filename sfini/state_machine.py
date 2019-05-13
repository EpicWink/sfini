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
        comment: description of state-maching
        timeout: execution time-out (seconds)
        session: session to use for AWS communication
    """

    _execution_class = sfini_execution.Execution
    _succeed_state_class = sfini_state.Succeed
    _fail_state_class = sfini_state.Fail
    _pass_state_class = sfini_state.Pass
    _wait_state_class = sfini_state.Wait
    _parallel_state_class = sfini_state.Parallel
    _choice_state_class = sfini_state.Choice
    _task_state_class = sfini_state.Task

    def __init__(
            self,
            name: str,
            comment: str = _default,
            timeout: int = _default,
            *,
            session: _util.AWSSession = None):
        self.name = name
        self.comment = comment
        self.timeout = timeout
        self.session = session or _util.AWSSession()
        self._start_state = None
        self.states: T.Dict[str, sfini_state.State] = {}

    def __str__(self):
        return "State-machine '%s' (%s states)" % (self.name, len(self.states))

    def __repr__(self):
        tottl = ", %s" % ("timeout=" if self.comment == _default else "")
        return "%s(%s, %s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            "" if self.comment == _default else ", %r" % self.comment,
            "" if self.timeout == _default else tottl + repr(self.timeout),
            ", session=%r" % self.session)

    @_util.cached_property
    def arn(self) -> str:
        """State-machine generated ARN."""
        region = self.session.region
        account = self.session.account_id
        _s = "arn:aws:states:%s:%s:stateMachine:%s"
        return _s % (region, account, self.name)

    @_util.cached_property
    def default_role_arn(self) -> str:
        """``sfini``-generated state-machine IAM role ARN."""
        return "arn:aws:iam::%s:role/sfiniGenerated" % self.session.account_id

    State = T.TypeVar("State", bound=sfini_state.State)

    def _state(self, cls: T.Type[State], name: str, *args, **kwargs) -> State:
        """Create a state in the state-machine.

        Args:
            cls: state to create
            name: name of state
            *args: positional arguments to ``cls``
            **kwargs: keyword arguments to ``cls``

        Returns:
            created state
        """

        if name in self.states:
            raise ValueError("State name '%s' already registered" % name)
        state = cls(name, *args, **kwargs, state_machine=self)
        self.states[name] = state
        return state

    def succeed(
            self,
            name: str,
            comment: str = _default,
            input_path: T.Union[str, None] = _default,
            output_path: T.Union[str, None] = _default
    ) -> _succeed_state_class:
        """Create a succeed state.

        Ends execution successfully.

        Args:
            name: name of state
            comment: state description
            input_path: state input filter JSONPath, ``None`` for empty input
            output_path: state output filter JSONPath, ``None`` for discarded
                output

        Returns:
            succeed state
        """

        return self._state(
            self._succeed_state_class,
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path)

    def fail(
            self,
            name: str,
            comment: str = _default,
            cause=_default,
            error=_default,
            input_path: T.Union[str, None] = _default,
            output_path: T.Union[str, None] = _default
    ) -> _fail_state_class:
        """Create a fail state.

        Ends execution unsuccessfully.

        Args:
            name: name of state
            comment: state description
            input_path: state input filter JSONPath, ``None`` for empty input
            output_path: state output filter JSONPath, ``None`` for discarded
                output
            cause: failure description
            error: name of failure error

        Returns:
            fail state
        """

        return self._state(
            self._fail_state_class,
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            cause=cause,
            error=error)

    def pass_(
            self,
            name: str,
            comment: str = _default,
            input_path: T.Union[str, None] = _default,
            output_path: T.Union[str, None] = _default,
            result_path: T.Union[str, None] = _default,
            result: _util.JSONable = _default
    ) -> _pass_state_class:
        """Create a pass state.

        No-op state, but can introduce data. The name specifies the location of
            the introduced data.

        Args:
            name: name of state
            comment: state description
            input_path: state input filter JSONPath, ``None`` for empty input
            output_path: state output filter JSONPath, ``None`` for discarded
                output
            result_path: task output location JSONPath, ``None`` for discarded
                output
            result: return value of state

        Returns:
            pass state
        """

        return self._state(
            self._pass_state_class,
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            result_path=result_path,
            result=result)

    def wait(
            self,
            name: str,
            until: T.Union[int, datetime.datetime, str],
            comment: str = _default,
            input_path: T.Union[str, None] = _default,
            output_path: T.Union[str, None] = _default
    ) -> _wait_state_class:
        """Create a wait state.

        Waits until a time before continuing.

        Args:
            name: name of state
            until: time to wait. If ``int``, then seconds to wait; if
                ``datetime.datetime``, then time to wait until; if ``str``,
                then name of state-variable containing time to wait until
            comment: state description
            input_path: state input filter JSONPath, ``None`` for empty input
            output_path: state output filter JSONPath, ``None`` for discarded
                output

        Returns:
            wait state
        """

        return self._state(
            self._wait_state_class,
            name,
            until,
            comment=comment,
            input_path=input_path,
            output_path=output_path)

    def parallel(
            self,
            name: str,
            comment: str = _default,
            input_path: T.Union[str, None] = _default,
            output_path: T.Union[str, None] = _default,
            result_path: T.Union[str, None] = _default
    ) -> _parallel_state_class:
        """Create a parallel state.

        Runs states-machines in parallel. These state-machines do not need to
        be registered with AWS Step Functions.

        The input to each state-machine execution is the input into this
        parallel state. The output of the parallel state is a list of each
        state-machine's output (in order of adding).

        Args:
            name: name of state
            comment: state description
            input_path: state input filter JSONPath, ``None`` for empty input
            output_path: state output filter JSONPath, ``None`` for discarded
                output
            result_path: task output location JSONPath, ``None`` for discarded
                output

        Returns:
            parallel state
        """

        return self._state(
            self._parallel_state_class,
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            result_path=result_path)

    def choice(
            self,
            name: str,
            comment: str = _default,
            input_path: T.Union[str, None] = _default,
            output_path: T.Union[str, None] = _default
    ) -> _choice_state_class:
        """Create a choice state.

        Creates branches of possible execution based on conditions.

        Args:
            name: name of state
            comment: state description
            input_path: state input filter JSONPath, ``None`` for empty input
            output_path: state output filter JSONPath, ``None`` for discarded
                output

        Returns:
            choice state
        """

        return self._state(
            self._choice_state_class,
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path)

    def task(
            self,
            name: str,
            resource,
            comment: str = _default,
            input_path: T.Union[str, None] = _default,
            output_path: T.Union[str, None] = _default,
            result_path: T.Union[str, None] = _default,
            timeout: int = _default
    ) -> _task_state_class:
        """Create a task state.

        Executes an activity.

        Args:
            name: name of state
            resource (sfini.task_resource.TaskResource): task executor, eg
                activity or Lambda function
            comment: state description
            input_path: state input filter JSONPath, ``None`` for empty input
            output_path: state output filter JSONPath, ``None`` for discarded
                output
            result_path: task output location JSONPath, ``None`` for discarded
                output
            timeout: seconds before task time-out

        Returns:
            task state
        """

        return self._state(
            self._task_state_class,
            name,
            resource,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            result_path=result_path,
            timeout=timeout)

    def start_at(self, state: sfini_state.State):
        """Define starting state.

        Args:
            state: initial state
        """

        if state.state_machine is not self:
            _s = "State '%s' is not part of this state-machine"
            raise ValueError(_s % state)
        if self._start_state is not None:
            _s = "Overriding start state %s with %s"
            _logger.warning(_s % (self._start_state, state))
        self._start_state = state

    def to_dict(self) -> T.Dict[str, _util.JSONable]:
        """Convert this state-machine to a definition dictionary.

        Returns:
            definition
        """

        _logger.debug("Converting '%s' to dictionary" % self)
        state_defns = {n: s.to_dict() for n, s in self.states.items()}
        defn = {"StartAt": self._start_state.name, "States": state_defns}
        if self.comment != _default:
            defn["Comment"] = self.comment
        if self.timeout != _default:
            defn["TimeoutSeconds"] = self.timeout
        return defn

    def is_registered(self) -> bool:
        """See if this state-machine is registered.

        Returns:
            if this state-machine is registered
        """

        _logger.debug("Testing for registration of '%s' on SFN" % self)
        resp = _util.collect_paginated(self.session.sfn.list_state_machines)
        arns = {sm["stateMachineArn"] for sm in resp["stateMachines"]}
        return self.arn in arns

    def _sfn_create(self, role_arn: str):
        """Create this state-machine in SFN.

        Args:
            role_arn: AWS ARN for state-machine IAM role
        """

        _logger.info("Creating '%s' on SFN" % self)
        resp = self.session.sfn.create_state_machine(
            name=self.name,
            definition=json.dumps(self.to_dict(), indent=4),
            roleArn=role_arn)
        assert resp["stateMachineArn"] == self.arn
        _arn = resp["stateMachineArn"]
        _date = resp["creationDate"]
        _s = "'%s' created with ARN '%s' at %s"
        _logger.info(_s % (self, _arn, _date))

    def _sfn_update(self, role_arn: str):
        """Update this state-machine in SFN.

        Args:
            role_arn: AWS ARN for state-machine IAM role
        """

        _logger.info("Updating '%s' on SFN" % self)
        resp = self.session.sfn.update_state_machine(
            definition=json.dumps(self.to_dict(), indent=4),
            roleArn=role_arn,
            stateMachineArn=self.arn)
        _s = "'%s' updated at %s"
        _logger.info(_s % (self, resp["updateDate"]))

    def register(self, role_arn: str = None, allow_update: bool = False):
        """Register state-machine with AWS Step Functions.

        Args:
            role_arn: AWS ARN for state-machine IAM role
            allow_update: allow overwriting of an existing state-machine with
                the same name
        """

        role_arn = role_arn or self.default_role_arn
        _util.assert_valid_name(self.name)

        if allow_update and self.is_registered():
            self._sfn_update(role_arn)
        else:
            self._sfn_create(role_arn)

    def deregister(self):
        """Remove state-machine from AWS SFN."""
        if not self.is_registered():
            raise RuntimeError("Cannot de-register unregistered state-machine")

        _logger.info("Deleting '%s' from SFN" % self)
        _ = self.session.sfn.delete_state_machine(stateMachineArn=self.arn)

    def start_execution(
            self,
            execution_input: _util.JSONable
    ) -> _execution_class:
        """Start an execution.

        Args:
            execution_input: input to first state in state-machine

        Returns:
            Execution: started execution
        """

        _s = "Starting execution of '%s' with: %s"
        _logger.info(_s % (self, execution_input))

        _now = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        name = self.name + "_" + _now + "_" + str(uuid.uuid4())[:8]
        execution = self._execution_class(
            name,
            self,
            execution_input,
            session=self.session)
        execution.start()
        return execution

    def list_executions(self, status: str = None) -> T.List[_execution_class]:
        """List all executions of this state-machine.

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

        executions = []
        for exec_info in resp["executions"]:
            assert exec_info["stateMachineArn"] == self.arn
            execution = self._execution_class.from_execution_list_item(
                exec_info,
                session=self.session)
            execution.state_machine = self
            executions.append(execution)

            status, stop_date = exec_info["status"], exec_info.get("stopDate")
            _s = "Found execution '%s' with status '%s' and stop-date: %s"
            _logger.debug(_s % (execution, status, stop_date))

        return executions
