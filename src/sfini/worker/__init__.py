"""Activity task polling and execution.

You can provide you're own workers: the interface to the activities is
public. This module's worker implementation uses threading, and is
designed to be resource-managed outside of Python.
"""

__all__ = ["Worker", "WorkerCancel", "WorkersManager"]

from ._worker import Worker
from ._worker import WorkerCancel
from ._worker import WorkersManager
