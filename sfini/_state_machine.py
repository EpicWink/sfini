# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN state machine."""

import json
import uuid
import datetime
import logging as lg

from . import _util
from . import _execution
from . import _states
from . import _worker

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

    _worker_class = _worker.Worker
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
        self._output_variables = set()
        self._task_runner_threads = []
        self.states = None

    @_util.cached_property
    def arn(self):
        """State-machine generated ARN."""
        region = self.session.region
        account = self.session.account_id
        _s = "arn:aws:states:%s:%s:stateMachine:%s"
        return _s % (region, account, self.name)

    def start_at(self, state):
        """Define starting state.

        Args:
            state (sfini.State): initial state
        """

        if self._start_state is not None:
            _logger.warning(
                "Overriding start state %s with %s",
                self._start_state,
                state)
        self._start_state = state

    def output(self, variables):
        """Include variables in execution output.

        Note that unused return-value variables will already be included.

        Args:
            variables (list[str] or tuple[str] or set[str]): variables to
                include in execution output
        """

        self._output_variables.update(variables)

    def _discover_states(self):
        """Find all used states in state-machine."""
        if self._start_state is None:
            raise RuntimeError("Start state has not been set")
        states = {}
        self._start_state.add_to(states)
        for name, state in states.items():
            if isinstance(state, _states.Task):
                state.session = self.session
        return states

    def to_dict(self):
        """Convert this state-machine to a definition dictionary.

        Returns:
            dict: definition
        """

        states = self._discover_states()
        state_defns = {n: s.to_dict() for n, s in states.items()}
        defn = {"StartAt": self._start_state.name, "States": state_defns}
        if self.comment is not None:
            defn["Comment"] = self.comment
        if self.timeout is not None:
            defn["TimeoutSeconds"] = self.timeout
        return defn

    def to_json(self):
        """Convert this state-machine's definition to JSON.

        Returns:
            str: JSON of definition
        """

        return json.dumps(self.to_dict())

    def register(self):
        """Register state-machine with AWS Step Functions.

        Returns:
            dict: state-machine response
        """

        _util.assert_valid_name(self.name)
        resp = self.session.sfn.create_state_machine(
            name=self.name,
            definition=self.to_json(),
            roleArn=self.role_arn)
        _logger.info(
            "State machine created with ARN '%s' at %s",
            resp["stateMachineArn"],
            resp["creationDate"])

    def run_worker(self, tasks=None, block=True):
        """Run a worker to execute tasks.

        Args:
            tasks (list[_states.Task]): tasks to execute, default: all
                tasks
            block (bool): run worker synchronously
        """

        if tasks is None:
            tasks = list(self._discover_states().values())
        task_runner = self._worker_class(
            self,
            tasks=tasks,
            session=self.session)
        task_runner.run(block=block)
        return task_runner

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
