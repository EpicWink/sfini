# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""Common utilities for ``sfini``."""

import typing as T
import logging as lg
import functools as ft

import boto3
from botocore import credentials

_logger = lg.getLogger(__name__)

MAX_NAME_LENGTH = 79
INVALID_NAME_CHARACTERS = " \n\t<>{}[]?*$%\\^|~`$,;:/"
DEBUG = False
JSONable = T.Union[None, bool, str, int, float, list, T.Dict[str, T.Any]]


class DefaultParameter:  # TODO: unit-test
    """Default parameter for step-functions definition."""
    def __bool__(self):
        return False

    def __eq__(self, other):
        return isinstance(self, type(other))

    def __str__(self):
        return "<Unspecified>"

    def __repr__(self):
        return "%s()" % type(self).__name__


def setup_logging(level: int = None):  # TODO: unit-test
    """Setup logging for ``sfini``, if logs would otherwise be ignored.

    Args:
        level: logging level (see ``logging``), default: leave unchanged
    """

    lg.basicConfig(
        format="%(asctime)s [%(levelname)8s] %(name)s: %(message)s",
        level=level)
    if level is not None:
        lg.getLogger().setLevel(level)
        [h.setLevel(level) for h in lg.getLogger().handlers]


def cached_property(fn: T.Callable) -> property:  # TODO: unit-test
    """Decorate a method as a cached property.

    The wrapped method's result is stored in the instance's ``__cache__``
    dictionary, with the method's name as key.

    Args:
        fn: method to decorate

    Returns:
        cached property
    """

    name = fn.__name__

    @ft.wraps(fn)
    def wrapped(self):
        if not hasattr(self, "__cache__"):
            self.__cache__ = {}
        if name not in self.__cache__:
            self.__cache__[name] = fn(self)
        return self.__cache__[name]

    if DEBUG:  # for testing
        def fset(self, value):
            if not hasattr(self, "__cache__"):
                self.__cache__ = {}
            self.__cache__[name] = value

        def fdel(self):
            if not hasattr(self, "__cache__"):
                self.__cache__ = {}
            del self.__cache__[name]

        return property(wrapped, fset=fset, fdel=fdel)

    return property(wrapped)


def assert_valid_name(name: str):  # TODO: unit-test
    """Ensure a valid name of activity, state-machine or state.

    Args:
        name: name to analyse

    Raises:
        ValueError: name is invalid
    """

    if len(name) > MAX_NAME_LENGTH:
        raise ValueError("Name is too long: '%s'" % name)
    if any(c in name for c in INVALID_NAME_CHARACTERS):
        raise ValueError("Name contains invalid characters: '%s'" % name)


def collect_paginated(
        fn: T.Callable[..., T.Dict[str, JSONable]],
        **kwargs
) -> T.Dict[str, JSONable]:  # TODO: unit-test
    """Call SFN API paginated endpoint.

    Calls ``fn`` until "nextToken" isn't in the return value, collating
    results. Uses recursion: if recursion limit is reached, increase
    ``maxResults`` if available, otherwise increase the maximum recursion
    limit using the ``sys`` package.

    Args:
        fn: SFN API function
        kwargs: arguments to ``fn``

    Returns:
        combined results of paginated API calls
    """

    result = fn(**kwargs)
    if "nextToken" in result:
        kwargs["nextToken"] = result.pop("nextToken")
        r2 = collect_paginated(fn, **kwargs)
        [result[k].extend(v) for k, v in r2.items() if isinstance(v, list)]
    return result


class AWSSession:  # TODO: unit-test
    """AWS session, for preconfigure communication with AWS.

    Args:
        session: session to use
    """

    def __init__(self, session: boto3.Session = None):
        self.session = session or boto3.Session()

    def __str__(self):
        _k = self.credentials.access_key
        return "Session[access key: %s, region: %s]" % (_k, self.region)

    def __repr__(self):
        return "%s(%s)" % (type(self).__name__, repr(self.session))

    @cached_property
    def credentials(self) -> credentials.Credentials:
        """AWS session credentials."""
        return self.session.get_credentials()

    @cached_property
    def sfn(self) -> boto3.session.botocore.session.botocore.client.BaseClient:
        """Step Functions client."""
        setup_logging()
        return self.session.client("stepfunctions")

    @cached_property
    def region(self) -> str:
        """Session AWS region."""
        return self.session.region_name

    @cached_property
    def account_id(self) -> str:
        """Session's account's account ID."""
        return self.session.client("sts").get_caller_identity()["Account"]
