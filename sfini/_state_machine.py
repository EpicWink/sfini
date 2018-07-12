# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""SFN state machine."""

import json
import uuid
import datetime
import logging as lg

import boto3

from . import _util
from . import _execution

_logger = lg.getLogger(__name__)


class StateMachine:  # TODO: unit-test
    """State machine structure for AWS Step Functions.

    Args:
        name (str): name of state-machine
        role_arn (str): AWS ARN for state-machine IAM role
        comment (str): description of state-maching
        timeout (int): execution time-out (seconds)
        session (boto3.session.Session): session to use for AWS
            communication
    """

    _task_runner_class = None  # TODO: implement task runner class
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
        self.session = session or boto3.Session()

        self._sfn_client = self.session.client("stepfunctions")
        self._start_state = None
        self._output_variables = set()
        self._task_runner_threads = []

    @_util.cached_property
    def arn(self):
        """State-machine generated ARN."""
        region = self.session.region_name
        _sts = self.session.client("sts")
        account = _sts.get_caller_identity()["account"]
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

    def _build_definition(self):
        if self._start_state is None:
            raise RuntimeError("Start state has not been set")
        raise NotImplementedError  # TODO: implement definition building

    def to_dict(self):
        """Convert this state-machine to a definition dictionary.

        Returns:
            dict: definition
        """

        self._build_definition()
        # TODO: implement definition dict building
        raise NotImplementedError

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

        resp = self._sfn_client.create_state_machine(
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
            tasks (list[sfini._states.Task]): tasks to
                execute, default: all tasks
            block (bool): run worker synchronously
        """

        task_runner = self._task_runner_class(self, tasks=tasks)
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
            sfn_client=self._sfn_client)
        execution.start()
        return execution
