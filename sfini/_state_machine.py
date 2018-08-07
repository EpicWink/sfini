# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN state-machine."""

import json
import uuid
import datetime
import logging as lg

from . import _util
from . import _execution
from . import _states

_logger = lg.getLogger(__name__)


class StateMachine:  # TODO: unit-test
    """State machine structure for AWS Step Functions.

    Args:
        name (str): name of state-machine
        role_arn (str): AWS ARN for state-machine IAM role
        comment (str): description of state-maching
        timeout (int): execution time-out (seconds)
        session (_util.AWSSession): session to use for AWS communication
    """

    _execution_class = _execution.Execution

    def __init__(
            self,
            name,
            role_arn,
            comment=None,
            timeout=None,
            *,
            session=None):
        self.name = name
        self.role_arn = role_arn
        self.comment = comment
        self.timeout = timeout
        self.session = session or _util.AWSSession()
        self._start_state = None
        self._task_runner_threads = []
        self.states = {}

    def __str__(self):
        return "State-machine '%s' (%s states)" % (self.name, len(self.states))

    def __repr__(self):
        tottl = ", %s" % ("timeout=" if self.comment is None else "")
        return "%s(%s, %s%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            repr(self.role_arn),
            "" if self.comment is None else ", " + repr(self.comment),
            "" if self.timeout is None else tottl + repr(self.timeout),
            ", session=" + repr(self.session))

    @_util.cached_property
    def arn(self):
        """State-machine generated ARN."""
        region = self.session.region
        account = self.session.account_id
        _s = "arn:aws:states:%s:%s:stateMachine:%s"
        return _s % (region, account, self.name)

    def succeed(self, name, comment=None):
        """Create a succeed state.

        Ends execution successfully.

        Args:
            name (str): name of state
            comment (str): state description

        Returns:
            _states.Succeed: succeed state
        """

        if name in self.states:
            raise ValueError("State name '%s' already registered" % name)
        state = _states.Succeed(name, comment=comment, state_machine=self)
        self.states[name] = state
        return state

    def fail(self, name, comment=None, cause=None, error=None):
        """Create a fail state.

        Ends execution unsuccessfully.

        Args:
            name (str): name of state
            comment (str): state description
            cause (str): failure description
            error (str): name of failure error

        Returns:
            _states.Fail: fail state
        """

        if name in self.states:
            raise ValueError("State name '%s' already registered" % name)
        state = _states.Fail(
            name,
            comment=comment,
            cause=cause,
            error=error,
            state_machine=self)
        self.states[name] = state
        return state

    def pass_(self, name, comment=None, result=None):
        """Create a pass state.

        No-op state, but can introduce data. The name specifies the location of
            the introduced data.

        Args:
            name (str): name of state
            comment (str): state description
            result: return value of state, stored in the variable ``name``

        Returns:
            _states.Pass: pass state
        """

        if name in self.states:
            raise ValueError("State name '%s' already registered" % name)
        state = _states.Pass(
            name,
            comment=comment,
            result=result,
            state_machine=self)
        self.states[name] = state
        return state

    def wait(self, name, until, comment=None):
        """Create a wait state.

        Waits until a time before continuing.

        Args:
            name (str): name of state
            until (int or datetime.datetime or str): time to wait. If ``int``,
                then seconds to wait; if ``datetime.datetime``, then time to
                wait until; if ``str``, then name of variable containing
                seconds to wait for
            comment (str): state description

        Returns:
            _states.Wait: wait state
        """

        if name in self.states:
            raise ValueError("State name '%s' already registered" % name)
        state = _states.Wait(name, until, comment=comment, state_machine=self)
        self.states[name] = state
        return state

    def parallel(self, name, comment=None):
        """Create a parallel state.

        Runs states-machines in parallel. These state-machines do not need to
        be registered with AWS Step Functions.

        The input to each state-machine execution is the input into this
        parallel state. The output of the parallel state is a list of each
        state-machine's output (in order of adding).

        Args:
            name (str): name of state
            comment (str): state description

        Returns:
            _states.Parallel: parallel state
        """

        if name in self.states:
            raise ValueError("State name '%s' already registered" % name)
        state = _states.Parallel(name, comment=comment, state_machine=self)
        self.states[name] = state
        return state

    def choice(self, name, comment=None):
        """Create a choice state.

        Creates branches of possible execution based on conditions.

        Args:
            name (str): name of state
            comment (str): state description

        Returns:
            _states.Choice: choice state
        """

        if name in self.states:
            raise ValueError("State name '%s' already registered" % name)
        state = _states.Choice(name, comment=comment, state_machine=self)
        self.states[name] = state
        return state

    def task(self, name, activity, comment=None, timeout=None):
        """Create a task state.

        Executes an activity.

        Args:
            name (str): name of state
            activity (Activity or str): activity to execute. If ``Activity``,
                the task is executed by an activity runner. If ``str``, the
                task is run by the AWS Lambda function named ``activity``
            comment (str): state description
            timeout (int): seconds before task time-out

        Returns:
            _states.Task: task state
        """

        if name in self.states:
            raise ValueError("State name '%s' already registered" % name)
        state = _states.Task(
            name,
            activity,
            comment=comment,
            timeout=timeout,
            state_machine=self)
        self.states[name] = state
        return state

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

        state_defns = {n: s.to_dict() for n, s in self.states.items()}
        defn = {"StartAt": self._start_state.name, "States": state_defns}
        if self.comment is not None:
            defn["Comment"] = self.comment
        if self.timeout is not None:
            defn["TimeoutSeconds"] = self.timeout
        return defn

    def is_registered(self):
        """See if this state-machine is registered.

        Returns:
            bool: if this state-machine is registered
        """

        resp = _util.collect_paginated(self.session.sfn.list_state_machines)
        arns = {sm["stateMachineArn"] for sm in resp["stateMachines"]}
        return self.arn in arns

    def _sfn_create(self):
        """Create this state-machine in SFN."""
        _logger.info("Creating state-machine '%s' on SFN" % self)
        resp = self.session.sfn.create_state_machine(
            name=self.name,
            definition=json.dumps(self.to_dict(), indent=4),
            roleArn=self.role_arn)
        assert resp["stateMachineArn"] == self.arn
        return resp

    def _sfn_update(self):
        """Update this state-machine in SFN."""
        _logger.info("Updating state-machine '%s' on SFN" % self)
        resp = self.session.sfn.update_state_machine(
            definition=json.dumps(self.to_dict(), indent=4),
            roleArn=self.role_arn,
            stateMachineArn=self.arn)
        return resp

    def register(self, allow_update=False):
        """Register state-machine with AWS Step Functions.

        Args:
            allow_update (bool): if ``True``, allow overwriting of an
                existing state-machine with the same name
        """

        _util.assert_valid_name(self.name)

        if allow_update and self.is_registered():
            resp = self._sfn_update()
            _s = "State machine '%s' updated at %s"
            _logger.info(_s % (self, resp["updateDate"]))
        else:
            resp = self._sfn_create()
            _arn = resp["stateMachineArn"]
            _date = resp["creationDate"]
            _s = "State machine '%s' created with ARN '%s' at %s"
            _logger.info(_s % (self, _arn, _date))

    def deregister(self):
        """Remove state-machine from AWS SFN."""
        if not self.is_registered():
            raise RuntimeError("Cannot de-register unregistered state-machine")

        _logger.info("Deleting state-machine '%s' from SFN" % self)

        _ = self.session.sfn.delete_state_machine(stateMachineArn=self.arn)
        _logger.info("State-machine '%s' de-registered" % self)

    def start_execution(self, execution_input):
        """Start an execution.

        Args:
            execution_input (dict): input to first state in state-machine

        Returns:
            Execution: started execution
        """

        _s = "Starting execution of '%s' with: %s"
        _logger.info(_s % (self, execution_input))

        _now = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
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

        Arguments:
            status (str): only list executions with this status. Choose from
                'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT' or 'ABORTED'
        """

        kwargs = {"stateMachineArn": self.arn}
        if status is not None:
            kwargs["statusFilter"] = status
        fn = self.session.sfn.list_executions
        resp = _util.collect_paginated(fn, kwargs=kwargs)

        executions = []
        for exec_info in resp["executions"]:
            assert exec_info["stateMachineArn"] == self.arn
            execution = _execution.Execution(
                name=exec_info["name"],
                state_machine=self,
                execution_input=None,
                session=self)
            execution._arn = exec_info["executionArn"]
            execution._start_time = exec_info["startDate"]
            executions.append(execution)

            _s = "Found execution '%s' with status '%s' and stop-date: %s"
            _logger.debug(_s % (execution, exec_info["status"], exec_info["stopDate"]))

        return executions

