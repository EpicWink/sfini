# --- 80 characters -------------------------------------------------------
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
        self.states = None

    def __str__(self):
        n_states = len(self.states)
        return "State-machine '%s' (%s states)" % (self.name, n_states)

    def __repr__(self):
        return "%s(%s%s%s%s%s)" % (
            type(self).__name__,
            repr(self.name),
            repr(self.role_arn),
            "" if self.comment is None else (", " + repr(self.comment)),
            "" if self.timeout is None else (", " + repr(self.timeout)),
            ", session=" + repr(self.session))

    @_util.cached_property
    def arn(self):
        """State-machine generated ARN."""
        region = self.session.region
        account = self.session.account_id
        _s = "arn:aws:states:%s:%s:stateMachine:%s"
        return _s % (region, account, self.name)

    @property
    def all_tasks(self) -> list:
        """All task states."""
        states = self.states.values()
        return list(s for s in states if isinstance(s, _states.Task))

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

        No-op state, but can introduce data. The name specifies the
        location of the introduced data.

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
            until (int or datetime.datetime or str): time to wait. If
                ``int``, then seconds to wait; if ``datetime.datetime``,
                then time to wait until; if ``str``, then name of variable
                containing seconds to wait for
            comment (str): state description

        Returns:
            _states.Wait: wait state
        """

        if name in self.states:
            raise ValueError("State name '%s' already registered" % name)
        state = _states.Wait(
            name,
            until,
            comment=comment,
            state_machine=self)
        self.states[name] = state
        return state

    def parallel(self, name, comment=None):
        """Create a parallel state.

        Runs states-machines in parallel. These state-machines do not need
        to be registered with AWS Step Functions.

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

    def task(
            self,
            name,
            activity,
            comment=None,
            timeout=None,
            heartbeat=60):
        """Create a task state.

        Executes an activity.

        Args:
            name (str): name of state
            activity (Activity): activity to execute
            comment (str): state description
            timeout (int): seconds before task time-out
            heartbeat (int): second between task heartbeats

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
            heartbeat=heartbeat,
            state_machine=self)
        return state

    def start_at(self, state):
        """Define starting state.

        Args:
            state (sfini.State): initial state
        """

        if state.state_machine is not self:
            _s = "State '%s' is not part of this state-machine"
            raise ValueError(_s)
        if self._start_state is not None:
            _logger.warning(
                "Overriding start state %s with %s",
                self._start_state,
                state)
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

    def register(self):
        """Register state-machine with AWS Step Functions.

        Returns:
            dict: state-machine response
        """

        _util.assert_valid_name(self.name)
        resp = self.session.sfn.create_state_machine(
            name=self.name,
            definition=json.dumps(self.to_dict()),
            roleArn=self.role_arn)
        _logger.info(
            "State machine created with ARN '%s' at %s",
            resp["stateMachineArn"],
            resp["creationDate"])

    def start_execution(self, execution_input):
        """Start an execution.

        Args:
            execution_input (dict): input to first state in state-machine

        Returns:
            sfini.Execution: started execution
        """

        _now = datetime.datetime.now().isoformat("T")
        name = "_".join((self.name, _now, str(uuid.uuid4())))
        execution = self._execution_class(
            name,
            self,
            execution_input,
            session=self.session)
        execution.start()
        return execution
