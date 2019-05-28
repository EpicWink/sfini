"""State-machine execution interfacing.

Executions track state-machine execution history, input, status and (if
available) output. You can wait on it to finish, and iterate over its
history.
"""

__all__ = ["Execution", "history", "list_executions"]

from ._execution import Execution
from . import history
from ._execution import list_executions
