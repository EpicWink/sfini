# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2019/05/09

"""Task resource interfacing.

'Task' states require some executor to implement the task, which
different AWS services can provide, including Step Functions activities
and Lambda functions.
"""

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

    Attributes:
        service: name of service of which the resource belongs to
    """

    service: str = None

    def __init__(self, name: str, *, session: _util.AWSSession = None):
        self.name = name
        self.session = session

    def __str__(self):
        return "%s [%s]" % (self.name, self.service)

    __repr__ = _util.easy_repr

    @_util.cached_property
    def arn(self) -> str:
        """Task resource generated ARN."""
        region = self.session.region
        account = self.session.account_id
        fmt = "arn:aws:states:%s:%s:%s:%s"
        return fmt % (region, account, self.service, self.name)


class Lambda(TaskResource):  # TODO: unit-test
    """AWS Lambda function executor for a task.

    Args:
        name: name of Lambda function
        session: session to use for AWS communication
    """

    service = "function"

    @_util.cached_property
    def arn(self):
        arn_split = super().arn.split(":")
        arn_split[2] = "lambda"
        return ":".join(arn_split)
