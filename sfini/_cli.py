# --- 100 characters ------------------------------------------------------------------------------
# Created by: Laurie 2018/08/06

"""SFN service helper.

Use in your ``__main__`` module to provide a CLI to your service.
"""

import json
import argparse

from . import _util
from . import _worker


class CLI:
    """``sfini`` command-line interface.

    Arguments:
        state_machine (StateMachine): state-machine to run
        activities (Activities): activities to run
    """

    def __init__(self, state_machine, activities):
        self.state_machine = state_machine
        self.activities = activities

    def _build_parser(self):
        description = self.state_machine.comment or None
        parser = argparse.ArgumentParser(
            description=description)
        parser.add_argument(
            "-V",
            "--version",
            action="version",
            version=self.activities.version)
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="use verbose output")
        subparsers = parser.add_subparsers(
            metavar="COMMAND",
            help="bla",
            dest="command")

        register_parser = subparsers.add_parser(
            "register",
            help="register (or update) activities and state-machine",
            description="register (or update) activities and state-machine")
        register_parser.add_argument(
            "-o",
            "--state-machine-only",
            action="store_true",
            help="only register (or update) state-machine")

        start_parser = subparsers.add_parser(
            "start",
            help="start state-machine execution")
        start_parser.add_argument(
            "input_json",
            help="execution input JSON, use '-' for STDIN")
        start_parser.add_argument(
            "-w",
            "--wait",
            action="store_true",
            help="wait for execution to finish, and print output")

        worker_parser = subparsers.add_parser(
            "worker",
            help="run an acitivity worker",
            description="run an activity worker")
        worker_parser.add_argument(
            "activity_name",
            nargs="*",
            help="name of activity to run")

        executions_parser = subparsers.add_parser(
            "executions",
            help="list executions",
            description="list executions")
        executions_parser.add_argument(
            "-s",
            "--status",
            default=None,
            help="only list executions with this status")

        return parser

    def _execute(self, args, parser):
        if args.command == "register":
            self.state_machine.register(allow_update=args.state_machine_only)
            if not args.state_machine_only:
                self.activities.register()
        elif args.command == "start":
            if args.input_json == "-":
                execution_input = input()
            else:
                with open(args.input_json, "r") as fl:
                    execution_input = json.load(fl)
            execution = self.state_machine.start_execution(execution_input)
            if args.wait:
                execution.wait()
                print(execution.output)
        elif args.command == "worker":
            workers = []
            for activity_name in args.activity_name:
                activity = self.activities.activities[activity_name]
                worker = _worker.Worker(activity)
                worker.start()
                workers.append(worker)
            try:
                for worker in workers:
                    worker.join()
            except KeyboardInterrupt:
                for worker in workers:
                    worker.end()
                    worker.join()
        elif args.command == "executions":
            execs = self.state_machine.list_executions(status=args.status)
            print("Executions:\n" + "\n".join(map(str, execs)))
        else:
            parser.error("Invalid command: %s" % repr(args.command))

    def parse_args(self):
        """Parse command-line arguments and run CLI."""
        _util.setup_logging()
        parser = self._build_parser()
        args = parser.parse_args()
        self._execute(args, parser)
