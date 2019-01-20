# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""SFN state-machine."""

import json
import uuid
import datetime
import logging as lg

from . import _util
from . import _execution
from . import _states

_logger = lg.getLogger(__name__)
_default = _util.DefaultParameter()


class StateMachine:  # TODO: unit-test
    """State machine structure for AWS Step Functions.

    Args:
        name (str): name of state-machine
        comment (str): description of state-maching
        timeout (int): execution time-out (seconds)
        session (_util.AWSSession): session to use for AWS communication
    """

    _execution_class = _execution.Execution
    _succeed_state_class = _states.Succeed
    _fail_state_class = _states.Fail
    _pass_state_class = _states.Pass
    _wait_state_class = _states.Wait
    _parallel_state_class = _states.Parallel
    _choice_state_class = _states.Choice
    _task_state_class = _states.Task

    def __init__(
            self,
            name,
            comment=_default,
            timeout=_default,
            *,
            session=None):
        self.name = name
        self.comment = comment
        self.timeout = timeout
        self.session = session or _util.AWSSession()
        self._start_state = None
        self.states = {}

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
        """sfini-generated state-machine IAM role ARN."""
        return "arn:aws:iam::%s:role/sfiniGenerated" % self.session.account_id

    def _state(self, cls, name, *args, **kwargs):
        """Create a state in the state-machine.

        Args:
            cls (type): state to create
            name (str): name of state
            *args: positional arguments to ``cls``
            **kwargs: keyword arguments to ``cls``

        Returns:
            _states.State: created state
        """

        if name in self.states:
            raise ValueError("State name '%s' already registered" % name)
        state = cls(name, *args, **kwargs, state_machine=self)
        self.states[name] = state
        return state

    def succeed(
            self,
            name,
            comment=_default,
            input_path=_default,
            output_path=_default):
        """Create a succeed state.

        Ends execution successfully.

        Args:
            name (str): name of state
            comment (str): state description
            input_path (str or None): state input filter JSONPath
            output_path (str or None): state output filter JSONPath

        Returns:
            _states.Succeed: succeed state
        """

        return self._state(
            self._succeed_state_class,
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path)

    def fail(
            self,
            name,
            comment=_default,
            cause=_default,
            error=_default,
            input_path=_default,
            output_path=_default):
        """Create a fail state.

        Ends execution unsuccessfully.

        Args:
            name (str): name of state
            comment (str): state description
            input_path (str or None): state input filter JSONPath
            output_path (str or None): state output filter JSONPath
            cause (str): failure description
            error (str): name of failure error

        Returns:
            _states.Fail: fail state
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
            name,
            comment=_default,
            input_path=_default,
            output_path=_default,
            result_path=_default,
            result=_default):
        """Create a pass state.

        No-op state, but can introduce data. The name specifies the location of
            the introduced data.

        Args:
            name (str): name of state
            comment (str): state description
            input_path (str or None): state input filter JSONPath
            output_path (str or None): state output filter JSONPath
            result_path (str or None): task output location JSONPath
            result: return value of state

        Returns:
            _states.Pass: pass state
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
            name,
            until,
            comment=_default,
            input_path=_default,
            output_path=_default):
        """Create a wait state.

        Waits until a time before continuing.

        Args:
            name (str): name of state
            until (int or datetime.datetime or str): time to wait. If ``int``,
                then seconds to wait; if ``datetime.datetime``, then time to
                wait until; if ``str``, then name of variable containing
                seconds to wait for
            comment (str): state description
            input_path (str or None): state input filter JSONPath
            output_path (str or None): state output filter JSONPath

        Returns:
            _states.Wait: wait state
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
            name,
            comment=_default,
            input_path=_default,
            output_path=_default,
            result_path=_default):
        """Create a parallel state.

        Runs states-machines in parallel. These state-machines do not need to
        be registered with AWS Step Functions.

        The input to each state-machine execution is the input into this
        parallel state. The output of the parallel state is a list of each
        state-machine's output (in order of adding).

        Args:
            name (str): name of state
            comment (str): state description
            input_path (str or None): state input filter JSONPath
            output_path (str or None): state output filter JSONPath
            result_path (str or None): task output location JSONPath

        Returns:
            _states.Parallel: parallel state
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
            name,
            comment=_default,
            input_path=_default,
            output_path=_default):
        """Create a choice state.

        Creates branches of possible execution based on conditions.

        Args:
            name (str): name of state
            comment (str): state description
            input_path (str or None): state input filter JSONPath
            output_path (str or None): state output filter JSONPath

        Returns:
            _states.Choice: choice state
        """

        return self._state(
            self._choice_state_class,
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path)

    def task(
            self,
            name,
            activity,
            comment=_default,
            input_path=_default,
            output_path=_default,
            result_path=_default,
            timeout=_default):
        """Create a task state.

        Executes an activity.

        Args:
            name (str): name of state
            activity (Activity or str): activity to execute. If ``Activity``,
                the task is executed by an activity runner. If ``str``, the
                task is run by the AWS Lambda function named ``activity``
            comment (str): state description
            input_path (str or None): state input filter JSONPath
            output_path (str or None): state output filter JSONPath
            result_path (str or None): task output location JSONPath
            timeout (int): seconds before task time-out

        Returns:
            _states.Task: task state
        """

        return self._state(
            self._task_state_class,
            name,
            activity,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            result_path=result_path,
            timeout=timeout)

    def start_at(self, state):
        """Define starting state.

        Args:
            state (State): initial state
        """

        if state.state_machine is not self:
            _s = "State '%s' is not part of this state-machine"
            raise ValueError(_s % state)
        if self._start_state is not None:
            _s = "Overriding start state %s with %s"
            _logger.warning(_s % (self._start_state, state))
        self._start_state = state

    def to_dict(self):
        """Convert this state-machine to a definition dictionary.

        Returns:
            dict: definition
        """

        _logger.debug("Converting '%s' to dictionary" % self)
        state_defns = {n: s.to_dict() for n, s in self.states.items()}
        defn = {"StartAt": self._start_state.name, "States": state_defns}
        if self.comment != _default:
            defn["Comment"] = self.comment
        if self.timeout != _default:
            defn["TimeoutSeconds"] = self.timeout
        return defn

    def is_registered(self):
        """See if this state-machine is registered.

        Returns:
            bool: if this state-machine is registered
        """

        _logger.debug("Testing for registration of '%s' on SFN" % self)
        resp = _util.collect_paginated(self.session.sfn.list_state_machines)
        arns = {sm["stateMachineArn"] for sm in resp["stateMachines"]}
        return self.arn in arns

    def _sfn_create(self, role_arn):
        """Create this state-machine in SFN.

        Args:
            role_arn (str): AWS ARN for state-machine IAM role
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

    def _sfn_update(self, role_arn):
        """Update this state-machine in SFN.

        Args:
            role_arn (str): AWS ARN for state-machine IAM role
        """

        _logger.info("Updating '%s' on SFN" % self)
        resp = self.session.sfn.update_state_machine(
            definition=json.dumps(self.to_dict(), indent=4),
            roleArn=role_arn,
            stateMachineArn=self.arn)
        _s = "'%s' updated at %s"
        _logger.info(_s % (self, resp["updateDate"]))

    def register(self, role_arn=None, allow_update=False):
        """Register state-machine with AWS Step Functions.

        Args:
            role_arn (str): AWS ARN for state-machine IAM role
            allow_update (bool): allow overwriting of an existing state-machine
                with the same name
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

    def start_execution(self, execution_input):
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

    def list_executions(self, status=None):
        """List all executions of this state-machine.

        Args:
            status (str): only list executions with this status. Choose from
                'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT' or 'ABORTED'
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
