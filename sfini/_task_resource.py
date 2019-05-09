# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2019/05/09

"""Task executors."""

import logging as lg

from . import _util

_logger = lg.getLogger(__name__)


class TaskResource:  # TODO: unit-test
    """Task execution.

    An instance of this represents a service which can run tasks defined in
    a state-machine.

    Args:
        name: name of resource
        session: session to use for AWS communication
    """

    _service = None

    def __init__(self, name: str, *, session: _util.AWSSession = None):
        self.name = name
        self.session = session

    def __str__(self):
        return "%s %s '%s'" % (type(self).__name__, self._service, self.name)

    def __repr__(self):
        return type(self).__name__ + "(%s, session=%s)" % (
            repr(self.name),
            repr(self.session))

    @_util.cached_property
    def arn(self) -> str:
        """Task resource generated ARN."""
        region = self.session.region
        account = self.session.account_id
        _s = "arn:aws:states:%s:%s:%s:%s"
        return _s % (region, account, self._service, self.name)


class Lambda(TaskResource):  # TODO: unit-test
    """AWS Lambda function executor for a task.

    Args:
        name: name of Lambda function
        session: session to use for AWS communication
    """

    _service = "function"

    @_util.cached_property
    def arn(self):
        arn_split = super().arn.split(":")
        arn_split[2] = "lambda"
        return ":".join(arn_split)
