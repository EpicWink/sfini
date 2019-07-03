"""Test ``sfini.state_machine``."""

from sfini import state_machine as tscr
import pytest
from unittest import mock
import sfini
import datetime
import json


@pytest.fixture
def session_mock():
    """An AWSSession mock."""
    return mock.Mock(spec=sfini.AWSSession)


class TestStateMachine:
    """Test ``sfini.state_machine.StateMachine``."""
    @pytest.fixture
    def state_mocks(self):
        """State mocks."""
        return {n: mock.Mock(spec=sfini.state.State) for n in ["a", "b", "c"]}

    @pytest.fixture
    def state_machine(self, state_mocks, session_mock):
        """An example StateMachine instance."""
        return tscr.StateMachine(
            "spam",
            state_mocks,
            "a",
            comment="a state-machine",
            timeout=42,
            session=session_mock)

    def test_init(self, state_machine, state_mocks, session_mock):
        """StateMachine initialisation."""
        assert state_machine.name == "spam"
        assert state_machine.states == state_mocks
        assert state_machine.start_state == "a"
        assert state_machine.comment == "a state-machine"
        assert state_machine.timeout == 42
        assert state_machine.session is session_mock

    def test_str(self, state_machine):
        """StateMachine stringification."""
        res = str(state_machine)
        assert "spam" in res
        assert "3 states" in res

    def test_arn(self, state_machine, session_mock):
        """ARN construction."""
        session_mock.region = "spam-region"
        session_mock.account_id = "spamId"
        exp = "arn:aws:states:spam-region:spamId:stateMachine:spam"
        assert state_machine.arn == exp

    def test_default_role_arn(self, state_machine, session_mock):
        """Default state-machine role ARN generation."""
        session_mock.account_id = "spamId"
        exp = "arn:aws:iam::spamId:role/sfiniGenerated"
        assert state_machine.default_role_arn == exp

    @pytest.mark.parametrize(
        ("comment", "timeout", "exp"),
        [
            (tscr._default, tscr._default, {}),
            ("a state-machine", tscr._default, {"Comment": "a state-machine"}),
            (tscr._default, 42, {"TimeoutSeconds": 42}),
            (
                "a state-machine",
                42,
                {"Comment": "a state-machine", "TimeoutSeconds": 42})])
    def test_to_dict(self, state_machine, state_mocks, comment, timeout, exp):
        """Definition dictionary construction."""
        state_machine.comment = comment
        state_machine.timeout = timeout
        for j, state in enumerate(state_mocks.values()):
            state.to_dict.return_value = {"j": j}
        exp["StartAt"] = "a"
        exp["States"] = {n: {"j": j} for j, n in enumerate(state_mocks)}
        res = state_machine.to_dict()
        assert res == exp

    @pytest.mark.parametrize(
        ("names", "exp"),
        [(["foo", "bar"], False), (["foo", "spam", "bar"], True)])
    def test_is_registered(self, state_machine, session_mock, names, exp):
        """Checking for state-machine registration."""
        state_machine.arn = "spam:arn"
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        state_machines = [
            {"name": n, "stateMachineArn": n + ":arn", "creationDate": now}
            for n in names]
        session_mock.sfn.list_state_machines.return_value = {
            "stateMachines": state_machines}
        assert state_machine.is_registered() is exp

    def test_sfn_create(self, state_machine, session_mock):
        """State-machine registration."""
        # Setup environment
        state_machine.arn = "spam:arn"
        sm_definition = {
            "States": {"a": {"j": 1}, "b": {"j": 2}},
            "StartAt": "a",
            "Comment": "a state-machine"}
        state_machine.to_dict = mock.Mock(return_value=sm_definition)
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        session_mock.sfn.create_state_machine.return_value = {
            "stateMachineArn": "spam:arn",
            "creationDate": now - datetime.timedelta(seconds=1)}

        # Build input
        role_arn = "role/bla:arn"

        # Run function
        state_machine._sfn_create(role_arn)

        # Check result
        session_mock.sfn.create_state_machine.assert_called_once_with(
            name="spam",
            definition=mock.ANY,
            roleArn="role/bla:arn")
        res_call = session_mock.sfn.create_state_machine.call_args_list[0]
        res_definition = json.loads(res_call[1]["definition"])
        assert res_definition == sm_definition
        state_machine.to_dict.assert_called_once_with()

    @pytest.mark.parametrize(
        ("role_arn", "kw"),
        [("role/bla:arn", {"roleArn": "role/bla:arn"}), (None, {})])
    def test_sfn_update(self, state_machine, session_mock, role_arn, kw):
        """State-machine updating."""
        # Setup environment
        state_machine.arn = "spam:arn"
        sm_definition = {
            "States": {"a": {"j": 1}, "b": {"j": 2}},
            "StartAt": "a",
            "Comment": "a state-machine"}
        state_machine.to_dict = mock.Mock(return_value=sm_definition)
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        session_mock.sfn.update_state_machine.return_value = {
            "updateDate": now - datetime.timedelta(seconds=1)}

        # Run function
        state_machine._sfn_update(role_arn)

        # Check result
        session_mock.sfn.update_state_machine.assert_called_once_with(
            stateMachineArn="spam:arn",
            definition=mock.ANY,
            **kw)
        res_call = session_mock.sfn.update_state_machine.call_args_list[0]
        res_definition = json.loads(res_call[1]["definition"])
        assert res_definition == sm_definition
        state_machine.to_dict.assert_called_once_with()

    class TestRegister:
        """State-machine registration/updating."""
        @pytest.mark.parametrize(
            ("allow_update", "is_registered"),
            [(True, False), (False, True), (False, False)])
        @pytest.mark.parametrize(
            ("role_arn", "exp_role_arn"),
            [("role/bla:arn", "role/bla:arn"), (None, "role/default:arn")])
        def test_create(
                self,
                state_machine,
                role_arn,
                exp_role_arn,
                allow_update,
                is_registered):
            """State-machine creation."""
            state_machine.default_role_arn = "role/default:arn"
            state_machine.is_registered = mock.Mock(return_value=is_registered)
            state_machine._sfn_create = mock.Mock()
            state_machine._sfn_update = mock.Mock()
            exp_ir_calls = [mock.call()] * allow_update
            state_machine.register(
                role_arn=role_arn,
                allow_update=allow_update)
            state_machine._sfn_create.assert_called_once_with(exp_role_arn)
            state_machine._sfn_update.assert_not_called()
            assert state_machine.is_registered.call_args_list == exp_ir_calls

        def test_update(self, state_machine):
            """State-machine updating."""
            state_machine.is_registered = mock.Mock(return_value=True)
            state_machine._sfn_create = mock.Mock()
            state_machine._sfn_update = mock.Mock()
            role_arn = "role/bla:arn"
            state_machine.register(role_arn=role_arn, allow_update=True)
            state_machine._sfn_create.assert_not_called()
            state_machine._sfn_update.assert_called_once_with(role_arn)
            state_machine.is_registered.assert_called_once_with()

    def test_deregister(self, state_machine, session_mock):
        """State-machine de-registration."""
        state_machine.arn = "spam:arn"
        state_machine.deregister()
        session_mock.sfn.delete_state_machine.assert_called_once_with(
            stateMachineArn=state_machine.arn)

    def test_start_execution(self, state_machine, session_mock):
        """Execution starting."""
        # Setup environment
        state_machine.arn = "spam:arn"
        exec_mock = mock.Mock(spec=sfini.execution.Execution)
        state_machine._execution_class = mock.Mock(return_value=exec_mock)
        execution_input = {
            "a": 42,
            "b": "bla",
            "c": {"foo": [1, 2], "bar": None}}

        # Run function
        res = state_machine.start_execution(execution_input)

        # Check result
        assert res is exec_mock
        state_machine._execution_class.assert_called_once_with(
            mock.ANY,
            "spam:arn",
            execution_input,
            session=session_mock)
        res_name = state_machine._execution_class.call_args_list[0][0][0]
        assert "spam" in res_name
        now = datetime.datetime.now()
        assert now.strftime("%Y-%m-%dT%H-%M") in res_name
        exec_mock.start.assert_called_once_with()

    def test_build_executions(self, state_machine, session_mock):
        """Execution instantiation from list-items."""
        # Setup environment
        state_machine.arn = "spam:arn"
        exec_mocks = [
            mock.Mock(spec=sfini.execution.Execution)
            for _ in range(4)]
        exec_init_mock = mock.Mock(side_effect=exec_mocks)

        # Build input
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        items = [
            {
                "executionArn": "exec1:arn",
                "name": "exec1",
                "startDate": now - datetime.timedelta(minutes=50),
                "stateMachineArn": "spam:arn",
                "status": "SUCCEEDED",
                "stopDate": now - datetime.timedelta(minutes=45)},
            {
                "executionArn": "exec2:arn",
                "name": "exec2",
                "startDate": now - datetime.timedelta(minutes=40),
                "stateMachineArn": "spam:arn",
                "status": "FAILED",
                "stopDate": now - datetime.timedelta(minutes=36)},
            {
                "executionArn": "exec3:arn",
                "name": "exec3",
                "startDate": now - datetime.timedelta(minutes=30),
                "stateMachineArn": "spam:arn",
                "status": "ABORTED",
                "stopDate": now - datetime.timedelta(minutes=29)},
            {
                "executionArn": "exec4:arn",
                "name": "exec4",
                "startDate": now - datetime.timedelta(minutes=20),
                "stateMachineArn": "spam:arn",
                "status": "RUNNING"}]

        # Build expectation
        exp_feli_calls = [
            mock.call(item, session=session_mock)
            for item in items]

        # Run function
        with mock.patch.object(
                state_machine._execution_class,
                "from_list_item",
                exec_init_mock):
            res = state_machine._build_executions(items)

        # Check result
        assert res == exec_mocks
        for exec_mock in exec_mocks:
            assert exec_mock.state_machine is state_machine
        assert exec_init_mock.call_args_list == exp_feli_calls

    @pytest.mark.parametrize(
        ("status", "kw"),
        [
            (None, {}),
            ("RUNNING", {"statusFilter": "RUNNING"}),
            ("SUCCEEDED", {"statusFilter": "SUCCEEDED"}),
            ("FAILED", {"statusFilter": "FAILED"}),
            ("TIMED_OUT", {"statusFilter": "TIMED_OUT"}),
            ("ABORTED", {"statusFilter": "ABORTED"})])
    def test_list_executions(self, state_machine, session_mock, status, kw):
        """Execution listing."""
        # Setup environment
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        items = [
            {
                "executionArn": "exec1:arn",
                "name": "exec1",
                "startDate": now - datetime.timedelta(minutes=50),
                "stateMachineArn": "spam:arn",
                "status": "SUCCEEDED",
                "stopDate": now - datetime.timedelta(minutes=45)},
            {
                "executionArn": "exec2:arn",
                "name": "exec2",
                "startDate": now - datetime.timedelta(minutes=40),
                "stateMachineArn": "spam:arn",
                "status": "FAILED",
                "stopDate": now - datetime.timedelta(minutes=36)},
            {
                "executionArn": "exec3:arn",
                "name": "exec3",
                "startDate": now - datetime.timedelta(minutes=30),
                "stateMachineArn": "spam:arn",
                "status": "ABORTED",
                "stopDate": now - datetime.timedelta(minutes=29)},
            {
                "executionArn": "exec4:arn",
                "name": "exec4",
                "startDate": now - datetime.timedelta(minutes=20),
                "stateMachineArn": "spam:arn",
                "status": "RUNNING"}]
        items = [l for l in items if not status or l["status"] == status]
        resp = {"executions": items}
        session_mock.sfn.list_executions.return_value = resp

        exec_mocks = [mock.Mock(spec=sfini.execution.Execution) for _ in items]
        state_machine._build_executions = mock.Mock(return_value=exec_mocks)

        state_machine.arn = "spam:arn"

        # Build expectation
        kw["stateMachineArn"] = "spam:arn"

        # Run function
        res = state_machine.list_executions(status=status)

        # Check result
        assert res == exec_mocks
        session_mock.sfn.list_executions.assert_called_once_with(**kw)
        state_machine._build_executions.assert_called_once_with(items)


def test_construct_state_machine(session_mock):
    """State-machine building."""
    # Setup environment
    sm_mock = mock.Mock(spec=tscr.StateMachine)
    sm_class_mock = mock.Mock(return_value=sm_mock)

    state_b = mock.Mock(spec=sfini.state.State)
    state_c = mock.Mock(spec=sfini.state.State)

    def add_to(states):
        states["a"] = start_state
        states["b"] = state_b
        states["c"] = state_c

    # Build input
    start_state = mock.Mock(spec=sfini.state.State)
    start_state.add_to.side_effect = add_to
    start_state.name = "a"
    name = "spam"
    comment = "a state-machine"
    timeout = 42

    # Build expectation
    exp_states = {"a": start_state, "b": state_b, "c": state_c}

    # Run function
    with mock.patch.object(tscr, "StateMachine", sm_class_mock):
        res = tscr.construct_state_machine(
            name,
            start_state,
            comment=comment,
            timeout=timeout,
            session=session_mock)

    # Check result
    assert res is sm_mock
    sm_class_mock.assert_called_once_with(
        name,
        exp_states,
        "a",
        comment=comment,
        timeout=timeout,
        session=session_mock)
