# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""Common utilities for ``sfini``."""

import inspect
import typing as T
import logging as lg
import functools as ft
from collections import abc

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
        return "<unspecified>"

    def __repr__(self):
        return call_repr(type(self))


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


def call_repr(
        fn: T.Callable,
        args: tuple = (),
        kwargs: T.Dict[str, T.Any] = None,
        shorten: bool = True
) -> str:
    """Produce a representation of a function call.

    Args:
        fn: function (or other callable) being called
        args: call positional arguments
        kwargs: call keyword arguments
        shorten: attempt to shorten long argument representations

    Returns:
        call representation
    """

    kwargs = kwargs or {}
    sig = inspect.signature(fn)

    # Raise on incompatible function call
    if not any(p.kind == p.VAR_POSITIONAL for p in sig.parameters.values()):
        pos_kinds = (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD)
        pos = [p for p in sig.parameters.values() if p.kind in pos_kinds]
        if len(pos) < len(args):
            sig.bind(*args, **kwargs)
    if not any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values()):
        if any(n not in sig.parameters for n in kwargs):
            sig.bind(*args, **kwargs)

    # Build positional arguments substrings
    arg_strs = []
    for j, arg_val in enumerate(args):
        arg_str = repr(arg_val)
        if shorten and len(arg_str) > 80 and isinstance(arg_val, abc.Sized):
            arg_str = "len %d" % len(arg_val)
        arg_strs.append(arg_str)

    # Build keyword arguments substrings
    for name, arg_val in kwargs.items():
        if name in sig.parameters:
            param = sig.parameters[name]
            if param.default != param.empty and arg_val == param.default:
                continue
        val_str = repr(arg_val)
        if shorten and len(val_str) > 80 and isinstance(arg_val, abc.Sized):
            arg_str = "len(%s)=%d" % (name, len(arg_val))
        else:
            arg_str = "%s=%s" % (name, val_str)
        arg_strs.append(arg_str)

    # Combine substrings
    args_str = ", ".join(arg_strs)
    fn_name = fn.__name__
    return "%s(%s)" % (fn_name, args_str)


class AWSSession:  # TODO: unit-test
    """AWS session, for preconfigure communication with AWS.

    Args:
        session: session to use
    """

    def __init__(self, session: boto3.Session = None):
        self.session = session or boto3.Session()

    def __str__(self):
        fmt = "<access key: %s, region: %s>"
        return fmt % (self.credentials.access_key, self.region)

    def __repr__(self):
        return call_repr(type(self), args=(self.session,))

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
