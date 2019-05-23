# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""Common utilities for ``sfini``."""

import inspect
import sys
import typing as T
import logging as lg
import functools as ft
from collections import abc

import boto3
from botocore import credentials
from botocore import client as botocore_client

_logger = lg.getLogger(__name__)
lg.getLogger("botocore").setLevel(lg.WARNING)
MAX_NAME_LENGTH = 79
INVALID_NAME_CHARACTERS = " \n\t<>{}[]?*\"#%\\^|~`$&,;:/"
DEBUG = "pytest" in sys.modules
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
        return type(self).__name__ + "()"


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

    def _ensure_cache(self):
        if not hasattr(self, "__cache__"):
            self.__cache__ = {}

    @ft.wraps(fn)
    def wrapped(self):
        _ensure_cache(self)
        if name not in self.__cache__:
            self.__cache__[name] = fn(self)
        return self.__cache__[name]

    if DEBUG:  # for testing
        def fset(self, value):
            _ensure_cache(self)
            self.__cache__[name] = value

        def fdel(self):
            _ensure_cache(self)
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
        **kwargs: arguments to ``fn``

    Returns:
        combined results of paginated API calls
    """

    result = fn(**kwargs)
    if "nextToken" in result:
        kwargs["nextToken"] = result.pop("nextToken")
        r2 = collect_paginated(fn, **kwargs)
        [result[k].extend(v) for k, v in r2.items() if isinstance(v, list)]
    return result


def easy_repr(instance) -> str:
    """Use attributes to generate a string representation.

    Set class ``__repr__ = easy_repr``.

    Args:
        instance: object to get representation of

    Returns:
        object representation
    """

    sig = inspect.signature(type(instance))
    params = sig.parameters.values()

    # Can't yet process var-args
    has_var_pos = any(p.kind == p.VAR_POSITIONAL for p in params)
    has_var_kw = any(p.kind == p.VAR_KEYWORD for p in params)
    if has_var_pos or has_var_kw:
        raise RuntimeError("Can't use `easy_repr` with var-args yet")

    # Separate difference kinds of parameters
    params_pos = [p for p in params if p.kind == p.POSITIONAL_ONLY]
    params_any = [p for p in params if p.kind == p.POSITIONAL_OR_KEYWORD]
    params_kw = [p for p in params if p.kind == p.KEYWORD_ONLY]

    params_any_required = [p for p in params_any if p.default == p.empty]
    params_any_optional = [p for p in params_any if p.default != p.empty]
    params_unnamed = params_pos + params_any_required
    params_named = params_any_optional + params_kw

    arg_strs = []
    for param in params_unnamed:
        attr_val = getattr(instance, param.name)
        arg_str = repr(attr_val)
        if len(arg_str) > 80 and isinstance(attr_val, abc.Sized):
            arg_str = "len %d" % len(attr_val)
        arg_strs.append(arg_str)
    for param in params_named:
        attr_val = getattr(instance, param.name)
        if param.default != param.empty and attr_val == param.default:
            continue
        arg_str = repr(attr_val)
        if len(arg_str) > 80 and isinstance(attr_val, abc.Sized):
            arg_str = "len(%s)=%d" % (param.name, len(attr_val))
        else:
            arg_str = "%s=%s" % (param.name, arg_str)
        arg_strs.append(arg_str)

    args_str = ", ".join(arg_strs)
    type_name = type(instance).__name__
    return "%s(%s)" % (type_name, args_str)


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

    __repr__ = easy_repr

    @cached_property
    def credentials(self) -> credentials.Credentials:
        """AWS session credentials."""
        return self.session.get_credentials()

    @cached_property
    def sfn(self) -> botocore_client.BaseClient:
        """Step Functions client."""
        return self.session.client("stepfunctions")

    @cached_property
    def region(self) -> str:
        """Session AWS region."""
        return self.session.region_name

    @cached_property
    def account_id(self) -> str:
        """Session's account's account ID."""
        return self.session.client("sts").get_caller_identity()["Account"]
