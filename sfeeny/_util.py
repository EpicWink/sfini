# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/11

"""Package utilities."""

import logging as lg
import functools as ft

_logger = lg.getLogger(__name__)


def cached_property(fn):  # TODO: unit-test
    """Decorate a method as a cached property.

    The wrapped method's result is stored in the instance's ``__cache__``
    dictionary, with the method's name as key

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
