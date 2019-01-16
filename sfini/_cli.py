# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/08/06

"""SFN service helper.

Use in your ``__main__`` module to provide a CLI to your service.
"""

import json
import pathlib
import argparse
import logging as lg

from . import _util
from . import _worker


class CLI:
    """``sfini`` command-line interface.

    Args:
        state_machine (sfini.StateMachine): state-machine interact with
        activities (sfini.ActivityRegistration): activities to poll for
        version (str): version to display, default: no version display
        prog (str): program name displayed in program help,
            default: ``sys.argv[0]``
    """

    def __init__(
            self,
            state_machine=None,
            activities=None,
            version=None,
            prog=None):
        self.state_machine = state_machine
        self.activities = activities
        self.version = version
        self.prog = prog
        assert state_machine or activities

    def _build_parser(self):
        d = None
        if self.state_machine and self.activities:
            _s = "Control %s and '%s' activities"
            d = _s % (self.state_machine, self.activities.name)
        elif self.state_machine:
            d = "Control '%s'" % self.state_machine
        elif self.activities:
            d = "Control '%s' activities" % self.activities.name
        parser = argparse.ArgumentParser(description=d, prog=self.prog)
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
        subparsers = parser.add_subparsers(
            metavar="COMMAND",
            help="is this field used?",
            dest="command")

        sma_str = {
            (True, True): "state-machine and/or activities",
            (True, False): "state-machine",
            (False, True): "activities"}
        sma_str = sma_str[bool(self.state_machine), bool(self.activities)]

        register_parser = subparsers.add_parser(
            "register",
            help="register %s with SFN" % sma_str,
            description="register %s with SFN" % sma_str)
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
        if self.state_machine:
            deregister_parser.add_argument(
                "-u",
                "--allow-update",
                action="store_true",
                help="allow updating of existing state-machine")
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
                nargs="+",
                choices=self.activities.activities,
                metavar="NAME",
                help="name of activity to poll (can specify multiple)")

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

    def _execute(self, args):
        lg.getLogger().setLevel(max(lg.WARNING - 10 * args.verbose, lg.DEBUG))

        if args.command == "register":
            if self.state_machine and self.activities:
                if args.state_machine_only:
                    self.state_machine.register(allow_update=args.allow_update)
                elif args.activities_only:
                    self.activities.register()
                else:
                    self.state_machine.register(allow_update=args.allow_update)
                    self.activities.register()
            elif self.state_machine:
                self.state_machine.register(allow_update=args.allow_update)
            elif self.activities:
                self.activities.register()
        elif args.command == "start":
            _ijp = args.input_json
            _eis = input() if _ijp == "-" else pathlib.Path(_ijp).read_text()
            execution_input = json.loads(_eis)
            execution = self.state_machine.start_execution(execution_input)
            if args.wait:
                execution.wait()
                print(execution.output)
        elif args.command == "worker":
            all_activities = self.activities.activities
            activities = [all_activities[n] for n in args.activity_name]
            workers = _worker.WorkersManager(activities)
            workers.run()
        elif args.command == "executions":
            execs = self.state_machine.list_executions(status=args.status)
            for execution in execs:
                print(execution)
                execution.print_history()
        elif args.command == "deregister":
            if self.state_machine and self.activities:
                if args.state_machine_only:
                    self.state_machine.deregister()
                elif args.activities_only:
                    self.activities.deregister()
                else:
                    self.state_machine.deregister()
                    self.activities.deregister()
            elif self.state_machine:
                self.state_machine.deregister()
            elif self.activities:
                self.activities.deregister()

    def parse_args(self):
        """Parse command-line arguments and run CLI."""
        _util.setup_logging()
        parser = self._build_parser()
        args = parser.parse_args()
        self._execute(args)
