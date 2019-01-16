# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""Package utilities."""

import logging as lg
import functools as ft

import boto3
from botocore import credentials

_logger = lg.getLogger(__name__)

MAX_NAME_LENGTH = 79
INVALID_NAME_CHARACTERS = " \n\t<>{}[]?*$%\\^|~`$,;:/"
DEBUG = False


def setup_logging():  # TODO: unit-test
    """Setup logging for ``sfini``, if logs would otherwise be ignored."""
    lg.basicConfig(
        format="%(asctime)s [%(levelname)8s] %(name)s: %(message)s",
        level=lg.INFO)


def cached_property(fn):  # TODO: unit-test
    """Decorate a method as a cached property.

    The wrapped method's result is stored in the instance's ``__cache__``
    dictionary, with the method's name as key.

    Args:
        fn (callable): method to decorate

    Returns:
        property: cached property
    """

    name = fn.__name__

    @ft.wraps(fn)
    def wrapped(self):
        if not hasattr(self, "__cache__"):
            self.__cache__ = {}
        if name not in self.__cache__:
            self.__cache__[name] = fn(self)
        return self.__cache__[name]

    if DEBUG:
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


def assert_valid_name(name):  # TODO: unit-test
    """Ensure a valid name of activity, state-machine or state.

    Args:
        name (str): name to analyse

    Raises:
        ValueError: name is invalid
    """

    if len(name) > MAX_NAME_LENGTH:
        raise ValueError("Name is too long: '%s'" % name)
    if any(c in name for c in INVALID_NAME_CHARACTERS):
        raise ValueError("Name contains invalid characters: '%s'" % name)


def collect_paginated(fn, **kwargs):  # TODO: unit-test
    """Call SFN API paginated endpoint.

    Calls ``fn`` until "nextToken" isn't in the return value, collating
    results. Uses recursion: if recursion limit is reached, increase
    ``maxResults`` if available, otherwise increase the maximum recursion
    limit using the ``sys`` package.

    Arguments:
        fn (callable): SFN API function
        kwargs: arguments to ``fn``

    Returns:
        combined results of paginated API calls
    """

    result = fn(**kwargs)
    if "nextToken" in result:
        r2 = collect_paginated(fn, nextToken=result.pop("nextToken"), **kwargs)
        [result[k].extend(v) for k, v in r2.items() if isinstance(v, list)]
    return result


class AWSSession:  # TODO: unit-test
    """AWS session, for preconfigure communication with AWS.

    Args:
        session (boto3.Session): session to use
    """

    def __init__(self, session=None):
        self.session = session or boto3.Session()

    def __str__(self):
        _k = self.credentials.access_key
        return "Session[access key: %s, region: %s]" % (_k, self.region)

    def __repr__(self):
        return "%s(%s)" % (type(self).__name__, repr(self.session))

    @cached_property
    def credentials(self) -> credentials.Credentials:
        return self.session.get_credentials()

    @cached_property
    def sfn(self):
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
