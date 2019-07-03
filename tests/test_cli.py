"""Test ``sfini._cli``."""

from sfini import _cli as tscr
import pytest
from unittest import mock
import sfini
import argparse
import io
import json
import sys
import pathlib
from sfini import _util as sfini_util
import logging as lg


@pytest.fixture
def state_machine():
    """State-machine mock."""
    return mock.Mock(spec=sfini.state_machine.StateMachine)


@pytest.fixture
def activities():
    """Activity registration mock."""
    return mock.Mock(autospec=sfini.ActivityRegistration)


class TestCLI:
    """Test ``sfini._cli.CLI``."""
    @pytest.fixture
    def cli(self, state_machine, activities):
        """An example CLI instance."""
        return tscr.CLI(
            state_machine,
            activities,
            role_arn="spam:arn",
            version="0.42",
            prog="spam-prog")

    def test_init(self, cli, state_machine, activities):
        """CLI initialisation."""
        assert cli.state_machine is state_machine
        assert cli.activities is activities
        assert cli.role_arn == "spam:arn"
        assert cli.version == "0.42"
        assert cli.prog == "spam-prog"

    def test__build_parser(self, cli):
        """Argument parser construction."""
        res = cli._build_parser()
        assert isinstance(res, argparse.ArgumentParser)

    class TestRegister:
        """Activity/state-machine (de)registration."""
        @pytest.fixture(params=[
            pytest.param(True, id="update"),
            pytest.param(False, id="no_update")])
        def allow_update(self, request):
            """Allow state-machine updating."""
            return request.param

        @pytest.fixture()
        def toggle_state_machine(self, request, state_machine, cli):
            """Provide state-machine to CLI."""
            cli.state_machine = state_machine if request.param else None
            return request.param

        @pytest.fixture()
        def toggle_activities(self, request, activities, cli):
            """Provide activities to CLI."""
            cli.activities = activities if request.param else None
            return request.param

        @pytest.fixture()
        def state_machine_only(self, request):
            """Only modify state-machine."""
            return request.param

        @pytest.fixture()
        def activities_only(self, request):
            """Only modify activities."""
            return request.param

        @pytest.fixture()
        def args_reg(
                self,
                state_machine_only,
                activities_only,
                allow_update,
                toggle_state_machine):
            """Registration command-line arguments."""
            args_ = argparse.Namespace(
                state_machine_only=state_machine_only,
                activities_only=activities_only,
                command="register")
            if toggle_state_machine:
                args_.allow_update = allow_update
            return args_

        @pytest.fixture()
        def args_dereg(self, state_machine_only, activities_only):
            """Deregistration command-line arguments."""
            return argparse.Namespace(
                state_machine_only=state_machine_only,
                activities_only=activities_only,
                command="deregister")

        @pytest.fixture()
        def exp_sm_reg_calls(
                self,
                toggle_state_machine,
                activities_only,
                allow_update):
            """Expected state-machine method calls in registration."""
            if activities_only or not toggle_state_machine:
                return []
            return [mock.call.register("spam:arn", allow_update=allow_update)]

        @pytest.fixture()
        def exp_acts_reg_calls(self, toggle_activities, state_machine_only):
            """Expected activity registration method calls in registration."""
            if state_machine_only or not toggle_activities:
                return []
            return [mock.call.register()]

        @pytest.fixture()
        def exp_sm_dereg_calls(self, toggle_state_machine, activities_only):
            """Expected state-machine method calls in deregistration."""
            if activities_only or not toggle_state_machine:
                return []
            return [mock.call.deregister()]

        @pytest.fixture()
        def exp_acts_dereg_calls(self, toggle_activities, state_machine_only):
            """Expected activity registration method calls in dereg."""
            if state_machine_only or not toggle_activities:
                return []
            return [mock.call.deregister()]

        @pytest.mark.parametrize(
            ("toggle_state_machine", "toggle_activities"),
            [(True, True), (True, False), (False, True)],
            indirect=True)
        @pytest.mark.parametrize(
            ("state_machine_only", "activities_only"),
            [(False, False), (False, True), (True, False)],
            indirect=True)
        def test_register(
                self,
                cli,
                state_machine,
                activities,
                args_reg,
                exp_sm_reg_calls,
                exp_acts_reg_calls):
            """State-machine and activity registration."""
            cli._register(args_reg)
            assert state_machine.method_calls == exp_sm_reg_calls
            assert activities.method_calls == exp_acts_reg_calls

        @pytest.mark.parametrize(
            ("toggle_state_machine", "toggle_activities"),
            [(True, True), (True, False), (False, True)],
            indirect=True)
        @pytest.mark.parametrize(
            ("state_machine_only", "activities_only"),
            [(False, False), (False, True), (True, False)],
            indirect=True)
        def test_deregister(
                self,
                cli,
                state_machine,
                activities,
                args_dereg,
                exp_sm_dereg_calls,
                exp_acts_dereg_calls):
            """State-machine and activity deregistration."""
            cli._deregister(args_dereg)
            assert state_machine.method_calls == exp_sm_dereg_calls
            assert activities.method_calls == exp_acts_dereg_calls

    class TestStart:
        def test_stdin(self, cli, state_machine):
            """Start execution with input from stdin."""
            # Setup environment
            input_stream = io.StringIO()
            exec_input = {"a": 42, "b": {"foo": "spam", "bar": None}}
            input_stream.write(json.dumps(exec_input))
            input_stream.seek(0)

            execution_mock = state_machine.start_execution.return_value

            # Build input
            args = argparse.Namespace(
                input_json="-",
                wait=False,
                command="start")

            # Run function
            with mock.patch.object(sys, "stdin", input_stream):
                cli._start(args)

            # Check result
            state_machine.start_execution.assert_called_once_with(exec_input)
            execution_mock.wait.assert_not_called()

        def test_file(self, cli, state_machine, tmpdir):
            """Start execution with input from file."""
            # Setup environment
            exec_input = {"a": 42, "b": {"foo": "spam", "bar": None}}
            input_json = pathlib.Path(str(tmpdir.join("spam.json")))
            input_json.write_text(json.dumps(exec_input))

            execution_mock = state_machine.start_execution.return_value

            # Build input
            args = argparse.Namespace(
                input_json=str(input_json),
                wait=False,
                command="start")

            # Run function
            cli._start(args)

            # Check result
            state_machine.start_execution.assert_called_once_with(exec_input)
            execution_mock.wait.assert_not_called()

        def test_wait(self, cli, state_machine, tmpdir):
            """Start execution with input from stdin."""
            # Setup environment
            exec_input = {"a": 42, "b": {"foo": "spam", "bar": None}}
            input_json = pathlib.Path(str(tmpdir.join("spam.json")))
            input_json.write_text(json.dumps(exec_input))

            output_stream = io.StringIO()
            execution_mock = state_machine.start_execution.return_value
            execution_mock.output = "spam-output"

            # Build input
            args = argparse.Namespace(
                input_json=str(input_json),
                wait=True,
                command="start")

            # Run function
            with mock.patch.object(sys, "stdout", output_stream):
                cli._start(args)

            # Check result
            execution_mock.wait.assert_called_once_with()
            assert output_stream.getvalue() == "spam-output\n"

    def test_worker(self, cli, activities):
        """Worker running."""
        # Setup environment
        activities.activities = {
            "spam-act": mock.Mock(spec=sfini.activity.CallableActivity),
            "bla-act": mock.Mock(spec=sfini.activity.CallableActivity)}

        worker_mock = mock.Mock(spec=sfini.Worker)
        worker_class_mock = mock.Mock(return_value=worker_mock)
        cli._worker_class = worker_class_mock

        # Build input
        args = argparse.Namespace(activity_name="spam-act", command="worker")

        # Run function
        cli._worker(args)

        # Check result
        worker_class_mock.assert_called_once_with(
            activities.activities["spam-act"])
        worker_mock.run.assert_called_once_with()

    def test_executions(self, cli, state_machine):
        """Execution listing."""
        # Setup environment
        output_stream = io.StringIO()
        execs = [mock.Mock(spec=sfini.execution.Execution) for _ in range(4)]
        state_machine.list_executions.return_value = execs
        for j, execution in enumerate(execs):
            execution.format_history.return_value = "spam\n  %d" % j
            type(execution).__str__ = mock.Mock(return_value="exec%s" % j)

        # Build input
        args = argparse.Namespace(status="spam-status", command="executions")

        # Build expectation
        exp_output = (
            "\nExecution 'exec0':\nspam\n  0\n"
            "\nExecution 'exec1':\nspam\n  1\n"
            "\nExecution 'exec2':\nspam\n  2\n"
            "\nExecution 'exec3':\nspam\n  3\n")

        # Run function
        with mock.patch.object(sys, "stdout", output_stream):
            cli._executions(args)

        # Check result
        assert output_stream.getvalue() == exp_output

    class TestDelegate:
        @pytest.mark.parametrize(
            ("command", "mock_call_method"),
            [
                ("register", mock.call._register),
                ("deregister", mock.call._deregister),
                ("start", mock.call._start),
                ("worker", mock.call._worker),
                ("executions", mock.call._executions)])
        def test_command(self, cli, command, mock_call_method):
            """Which command is executed."""
            # Setup environment
            cli._register = mock.Mock()
            cli._deregister = mock.Mock()
            cli._start = mock.Mock()
            cli._worker = mock.Mock()
            cli._executions = mock.Mock()

            manager = mock.Mock()
            manager.attach_mock(cli._register, "_register")
            manager.attach_mock(cli._deregister, "_deregister")
            manager.attach_mock(cli._start, "_start")
            manager.attach_mock(cli._worker, "_worker")
            manager.attach_mock(cli._executions, "_executions")

            # Build input
            args = argparse.Namespace(
                spam="forty-two",
                bla=None,
                verbose=3,
                quiet=2,
                command=command)

            # Build expectation
            exp_calls = [mock_call_method(args)]

            # Run function
            with mock.patch.object(sfini_util, "setup_logging", mock.Mock()):
                cli._delegate(args)

            # Check result
            assert manager.method_calls == exp_calls

        @pytest.mark.parametrize(
            ("verbose", "quiet", "level"),
            [
                (0, 0, lg.WARNING),
                (3, 2, lg.INFO),
                (2, 0, lg.DEBUG),
                (6, 1, lg.DEBUG),
                (0, 1, lg.ERROR),
                (2, 4, lg.CRITICAL)])
        def test_logging_setup(self, cli, verbose, quiet, level):
            """Logging setup with provided verbosity."""
            # Setup environment
            cli._register = mock.Mock()
            cli._deregister = mock.Mock()
            cli._start = mock.Mock()
            cli._worker = mock.Mock()
            cli._executions = mock.Mock()
            sl_mock = mock.Mock()

            # Build input
            args = argparse.Namespace(
                spam="forty-two",
                bla=None,
                verbose=verbose,
                quiet=quiet,
                command="register")

            # Run function
            with mock.patch.object(sfini_util, "setup_logging", sl_mock):
                cli._delegate(args)

            # Check result
            sl_mock.assert_called_once_with(level=level)

    def test_parse_args(self, cli):
        """Command-line argument parsing and command execution."""
        # Setup environment
        parser_mock = mock.Mock(spec=argparse.ArgumentParser)
        cli._build_parser = mock.Mock(return_value=parser_mock)
        args = mock.Mock(spec=argparse.Namespace)
        parser_mock.parse_args.return_value = args
        cli._delegate = mock.Mock()

        # Run function
        cli.parse_args()

        # Check result
        cli._build_parser.assert_called_once_with()
        parser_mock.parse_args.assert_called_once_with()
        cli._delegate.assert_called_once_with(args)
