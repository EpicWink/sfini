# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/08/06

"""SFN service CLI helper.

Use in your ``__main__`` module to provide a CLI to your service.
"""

import json
import pathlib
import argparse
import logging as lg

from . import _util
from . import worker as sfini_worker


class CLI:  # TODO: unit-test
    """``sfini`` command-line interface.

    Args:
        state_machine (sfini.StateMachine): state-machine interact with
        activities (sfini.ActivityRegistration): activities to poll for
        role_arn: AWS ARN for state-machine IAM role
        version: version to display, default: no version display
        prog : program name displayed in program help,
            default: ``sys.argv[0]``
    """

    _worker_class = sfini_worker.Worker
    _parser_class = argparse.ArgumentParser

    def __init__(
            self,
            state_machine=None,
            activities=None,
            role_arn: str = None,
            version: str = None,
            prog: str = None):
        self.state_machine = state_machine
        self.activities = activities
        self.role_arn = role_arn
        self.version = version
        self.prog = prog
        assert state_machine or activities

    def _build_parser(self) -> argparse.ArgumentParser:
        """Build argument parser.

        Returns:
            configured command-line argument parser
        """

        d = None
        if self.state_machine and self.activities:
            _s = "Control %s and '%s' activities"
            d = _s % (self.state_machine, self.activities.prefix)
        elif self.state_machine:
            d = "Control '%s'" % self.state_machine
        elif self.activities:
            d = "Control '%s' activities" % self.activities.prefix
        parser = self._parser_class(description=d, prog=self.prog)
        if self.version:
            parser.add_argument(
                "-V",
                "--version",
                action="version",
                version=self.version)
        parser.add_argument(
            "-v",
            "--verbose",
            default=0,
            action="count",
            help="increase verbosity")
        parser.add_argument(
            "-q",
            "--quiet",
            default=0,
            action="count",
            help="decrease verbosity")
        subparsers = parser.add_subparsers(
            metavar="COMMAND",
            # help="description",
            dest="command")
        subparsers.required = True  # Python 3.6 compatibility

        sma_str = {
            (True, True): "state-machine and/or activities",
            (True, False): "state-machine",
            (False, True): "activities"}
        sma_str = sma_str[bool(self.state_machine), bool(self.activities)]

        register_parser = subparsers.add_parser(
            "register",
            help="register %s with SFN" % sma_str,
            description="register %s with SFN" % sma_str)
        if self.state_machine:
            register_parser.add_argument(
                "-u",
                "--allow-update",
                action="store_true",
                help="allow updating of existing state-machine")
        if self.state_machine and self.activities:
            _g = register_parser.add_mutually_exclusive_group()
            _g.add_argument(
                "-s",
                "--state-machine-only",
                action="store_true",
                help="only register (or update) state-machine")
            _g.add_argument(
                "-a",
                "--activities-only",
                action="store_true",
                help="only register activities")

        deregister_parser = subparsers.add_parser(
            "deregister",
            help="deregister %s from SFN" % sma_str,
            description="deregister %s from SFN" % sma_str)
        if self.state_machine and self.activities:
            _g = deregister_parser.add_mutually_exclusive_group()
            _g.add_argument(
                "-s",
                "--state-machine-only",
                action="store_true",
                help="only deregister state-machine")
            _g.add_argument(
                "-a",
                "--activities-only",
                action="store_true",
                help="only deregister activities")

        if self.state_machine:
            start_parser = subparsers.add_parser(
                "start",
                help="start state-machine execution")
            start_parser.add_argument(
                "input_json",
                metavar="PATH",
                help="execution input JSON, use '-' for STDIN")
            start_parser.add_argument(
                "-w",
                "--wait",
                action="store_true",
                help="wait for execution to finish, and print output")

        if self.activities:
            worker_parser = subparsers.add_parser(
                "worker",
                help="run an activity worker",
                description="run an activity worker")
            worker_parser.add_argument(
                "activity_name",
                choices=self.activities.activities,
                metavar="NAME",
                help="name of activity to poll, choose from: %(choices)s)")

        if self.state_machine:
            executions_parser = subparsers.add_parser(
                "executions",
                help="list executions",
                description="list executions")
            choices = "RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"
            executions_parser.add_argument(
                "-s",
                "--status",
                default=None,
                metavar="STATUS",
                choices=choices,
                help="only list executions with this status")

        return parser

    def _register(self, args: argparse.Namespace):
        """Register state-machine and/or activities.

        Args:
            args: parsed command-line arguments
        """

        if self.state_machine and self.activities:
            if args.state_machine_only:
                self.state_machine.register(
                    self.role_arn,
                    allow_update=args.allow_update)
            elif args.activities_only:
                self.activities.register()
            else:
                self.state_machine.register(
                    self.role_arn,
                    allow_update=args.allow_update)
                self.activities.register()
        elif self.state_machine:
            if not args.activities_only:
                self.state_machine.register(
                    self.role_arn,
                    allow_update=args.allow_update)
        elif self.activities:
            if not args.state_machine_only:
                self.activities.register()

    def _deregister(self, args: argparse.Namespace):
        """Deregister state-machine and/or activities.

        Args:
            args: parsed command-line arguments
        """

        if self.state_machine and self.activities:
            if args.state_machine_only:
                self.state_machine.deregister()
            elif args.activities_only:
                self.activities.deregister()
            else:
                self.state_machine.deregister()
                self.activities.deregister()
        elif self.state_machine:
            if not args.activities_only:
                self.state_machine.deregister()
        elif self.activities:
            if not args.state_machine_only:
                self.activities.deregister()

    def _start(self, args: argparse.Namespace):
        """Start a state-machine execution.

        Args:
            args: parsed command-line arguments
        """

        _ijp = args.input_json
        _eis = input() if _ijp == "-" else pathlib.Path(_ijp).read_text()
        execution_input = json.loads(_eis)
        execution = self.state_machine.start_execution(execution_input)
        if args.wait:
            execution.wait()
            print(execution.output)

    def _worker(self, args: argparse.Namespace):
        """Run an activity worker.

        Args:
            args: parsed command-line arguments
        """

        activity = self.activities.activities[args.activity_name]
        workers = self._worker_class(activity)
        workers.run()

    def _executions(self, args: argparse.Namespace):
        """List state-machine executions.

        Args:
            args: parsed command-line arguments
        """

        execs = self.state_machine.list_executions(status=args.status)
        for execution in execs:
            print(execution.format_history())

    def _parse_and_run(self, args: argparse.Namespace):
        """Parse and execute command-line arguments.

        Args:
            args: parsed command-line arguments
        """

        _lvl = max(lg.WARNING - 10 * (args.verbose - args.quiet), lg.DEBUG)
        _util.setup_logging(level=_lvl)

        command = {
            "register": self._register,
            "deregister": self._deregister,
            "start": self._start,
            "worker": self._worker,
            "executions": self._executions}
        command[args.command](args)

    def parse_args(self):
        """Parse command-line arguments and run CLI."""
        _util.setup_logging()
        parser = self._build_parser()
        args = parser.parse_args()
        self._parse_and_run(args)
