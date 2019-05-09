# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""AWS Step Functions service."""

import pkg_resources

try:
    __version__ = pkg_resources.get_distribution(__name__).version
except pkg_resources.DistributionNotFound:
    __version__ = None

__all__ = [
    "StateMachine",
    "Worker",
    "AWSSession",
    "ActivityRegistration",
    "Execution",
    "WorkerCancel",
    "Lambda",
    "And",
    "Or",
    "Not",
    "BooleanEquals",
    "NumericEquals",
    "NumericGreaterThan",
    "NumericGreaterThanEquals",
    "NumericLessThan",
    "NumericLessThanEquals",
    "StringEquals",
    "StringGreaterThan",
    "StringGreaterThanEquals",
    "StringLessThan",
    "StringLessThanEquals",
    "TimestampEquals",
    "TimestampGreaterThan",
    "TimestampGreaterThanEquals",
    "TimestampLessThan",
    "TimestampLessThanEquals",
    "CLI"]

import logging as lg

from ._state_machine import StateMachine
from ._worker import Worker
from ._util import AWSSession
from ._activity import ActivityRegistration
from ._execution import Execution
from ._state_error import WorkerCancel
from ._task_resource import Lambda
from ._choice_ops import And
from ._choice_ops import Or
from ._choice_ops import Not
from ._choice_ops import BooleanEquals
from ._choice_ops import NumericEquals
from ._choice_ops import NumericGreaterThan
from ._choice_ops import NumericGreaterThanEquals
from ._choice_ops import NumericLessThan
from ._choice_ops import NumericLessThanEquals
from ._choice_ops import StringEquals
from ._choice_ops import StringGreaterThan
from ._choice_ops import StringGreaterThanEquals
from ._choice_ops import StringLessThan
from ._choice_ops import StringLessThanEquals
from ._choice_ops import TimestampEquals
from ._choice_ops import TimestampGreaterThan
from ._choice_ops import TimestampGreaterThanEquals
from ._choice_ops import TimestampLessThan
from ._choice_ops import TimestampLessThanEquals
from ._cli import CLI

lg.getLogger(__name__).addHandler(lg.NullHandler())
