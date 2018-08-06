# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/08/11

"""Package utilities."""

import boto3

import logging as lg
import functools as ft

from . import __name__ as package_name

_logger = lg.getLogger(__name__)

MAX_NAME_LENGTH = 79
INVALID_NAME_CHARACTERS = " \n\t<>{}[]?*$%\\^|~`$,;:/"


def setup_logging():
    """Setup logging for ``sfini``, if logs would otherwise be ignored."""
    package_logger = lg.getLogger(package_name)
    if not package_logger.propagate or package_logger.disabled:
        return
    if not package_logger.hasHandlers():
        handler = lg.StreamHandler()
        fmt = "%(asctime)s \t%(levelname)s \t%(name)s \t%(message)s"
        formatter = lg.Formatter(fmt)
        handler.setFormatter(formatter)
        package_logger.addHandler(handler)
    if not package_logger.isEnabledFor(lg.INFO):
        package_logger.setLevel(lg.INFO)


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


def collect_paginated(fn, kwargs=None):  # TODO: unit-test
    """Call SFN API paginated endpoint.

    Arguments:
        fn (callable): SFN API function
        kwargs (dict): arguments to ``fn``, default=``{}``

    Returns:
        combined results of paginated API calls
    """

    if kwargs is None:
        kwargs = {}

    if "nextToken" in kwargs:
        raise ValueError("Can't start pagination with 'nextToken'")

    result = fn(**kwargs)
    while "nextToken" in result:
        next_token = result.pop("nextToken")
        next_result = fn(nextToken=next_token, **kwargs)
        for key, value in next_result.items():
            if key == "nextToken":
                result["nextToken"] = value
            else:
                result[key].extend(value)
    return result


class AWSSession:  # TODO: unit-test
    """AWS session, for preconfigure communication with AWS.

    Args:
        session (boto3.Session): session to use
    """

    def __init__(self, session=None):
        self.session = session or boto3.Session()

    def __str__(self):
        _k = self.session.get_credentials().access_key
        return "Session[access key: %s, region: %s]" % (_k, self.region)

    def __repr__(self):
        return "%s(%s)" % (type(self).__name__, repr(self.session))

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
        _sts = self.session.client("sts")
        return _sts.get_caller_identity()["Account"]
