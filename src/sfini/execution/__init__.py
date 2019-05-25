# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2019/05/13

"""State-machine execution interfacing.

Executions track state-machine execution history, input, status and (if
available) output. You can wait on it to finish, and iterate over its
history.
"""

__all__ = ["Execution", "history"]

from ._execution import Execution
from . import history
