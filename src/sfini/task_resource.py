"""Task resource interfacing.

'Task' states require some executor to implement the task, which
different AWS services can provide, including Step Functions activities
and Lambda functions.
"""

import logging as lg

from . import _util

_logger = lg.getLogger(__name__)


class TaskResource:
    """Task executor.

    An instance of this represents a service which can run tasks defined in
    a state-machine.

    Args:
        name: name of resource
        session: session to use for AWS communication

    Attributes:
        service: resource type
    """

    service: str = None

    def __init__(self, name: str, *, session: _util.AWSSession = None):
        self.name = name
        self.session = session

    def __str__(self):
        return "%s [%s]" % (self.name, self.service)

    __repr__ = _util.easy_repr

    @classmethod
    def from_arn(cls, arn: str, *, session: _util.AWSSession = None):  # TODO: unit-test
        """Task executor from ARN.

        Args:
            arn: task executor resource ARN
            session: session to use for AWS communication
        """

        name = arn.split(":", 6)[6]
        resource = cls(name, session=session)
        assert resource.arn == arn
        return resource

    @property
    def _region(self) -> str:
        """Resource region."""
        return self.session.region

    @property
    def _account_id(self) -> str:
        """Resource account ID."""
        return self.session.account_id

    @_util.cached_property
    def arn(self) -> str:
        """Task resource generated ARN."""
        fmt = "arn:aws:states:%s:%s:%s:%s"
        return fmt % (self._region, self._account_id, self.service, self.name)


class Lambda(TaskResource):
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
