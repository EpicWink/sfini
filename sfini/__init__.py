# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""AWS Step Functions service."""

import pkg_resources

try:
    __version__ = pkg_resources.get_distribution(__name__).version
except pkg_resources.DistributionNotFound:
    __version__ = None

__all__ = [
    "AWSSession",
    "Activity",
    "ActivityRegistration",
    "CLI",
    "Execution",
    "Lambda",
    "StateMachine",
    "Worker",
    "WorkerCancel"]

from ._util import AWSSession
from .activity import Activity
from .activity import ActivityRegistration
from ._cli import CLI
from .execution import Execution
from .task_resource import Lambda
from .state_machine import StateMachine
from .worker import Worker
from .state.error import WorkerCancel
from .state.choice import And
from .state.choice import Or
from .state.choice import Not
from .state.choice import BooleanEquals
from .state.choice import NumericEquals
from .state.choice import NumericGreaterThan
from .state.choice import NumericGreaterThanEquals
from .state.choice import NumericLessThan
from .state.choice import NumericLessThanEquals
from .state.choice import StringEquals
from .state.choice import StringGreaterThan
from .state.choice import StringGreaterThanEquals
from .state.choice import StringLessThan
from .state.choice import StringLessThanEquals
from .state.choice import TimestampEquals
from .state.choice import TimestampGreaterThan
from .state.choice import TimestampGreaterThanEquals
from .state.choice import TimestampLessThan
from .state.choice import TimestampLessThanEquals

from . import activity
from . import execution
from . import state
from . import state_machine
from . import task_resource
from . import worker

import logging as lg
lg.getLogger(__name__).addHandler(lg.NullHandler())
