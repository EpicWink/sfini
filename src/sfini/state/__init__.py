# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2019/05/13

"""State definitions.

States comprise a state-machine, defining its logic and which
activities to run, and direct data.
"""

__all__ = [
    "State",
    "HasNext",
    "HasResultPath",
    "CanRetry",
    "CanCatch",
    "Succeed",
    "Fail",
    "Pass",
    "Wait",
    "Parallel",
    "Choice",
    "Task",
    "choice"]

from ._base import State
from ._base import HasNext
from ._base import HasResultPath
from ._base import CanRetry
from ._base import CanCatch
from ._state import Succeed
from ._state import Fail
from ._state import Pass
from ._state import Wait
from ._state import Parallel
from ._state import Choice
from ._state import Task
from . import choice
