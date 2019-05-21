# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""Test ``sfini._util``."""

from sfini import _util as tscr
import pytest
from unittest import mock


class TestEasyRepr:
    """Test ``sfini._util.easy_repr``"""
    def test_no_params(self):
        """Function has no paramaters."""
        class Class:
            pass

        instance = Class()
        exp = "Class()"
        res = tscr.easy_repr(instance)
        assert res == exp

    def test_positional(self):
        """Called with only positional arguments."""
        class Class:
            def __init__(self, a, b):
                self.a = a
                self.b = b

        instance = Class(42, "spam")
        exp = "Class(42, 'spam')"
        res = tscr.easy_repr(instance)
        assert res == exp

    def test_positional_with_optional(self):
        """Called with only some positional arguments."""
        class Class:
            def __init__(self, a, b, c=None):
                self.a = a
                self.b = b
                self.c = c

        instance = Class(42, "spam")
        exp = "Class(42, 'spam')"
        res = tscr.easy_repr(instance)
        assert res == exp

    def test_positional_with_optional_provided(self):
        """Called with some arguments keyword."""
        class Class:
            def __init__(self, a, b, c=None):
                self.a = a
                self.b = b
                self.c = c

        instance = Class(42, "spam", c=[1, 2])
        exp = "Class(42, 'spam', c=[1, 2])"
        res = tscr.easy_repr(instance)
        assert res == exp

    def test_keyword_only_required(self):
        """Function has required keyword-only."""
        class Class:
            def __init__(self, a, b, c=None, *, d):
                self.a = a
                self.b = b
                self.c = c
                self.d = d

        instance = Class(42, "spam", d="bla")
        exp = "Class(42, 'spam', d='bla')"
        res = tscr.easy_repr(instance)
        assert res == exp

    def test_keyword_only_optional(self):
        """Function has optional keyword-only."""
        class Class:
            def __init__(self, a, b, c=None, *, d=None):
                self.a = a
                self.b = b
                self.c = c
                self.d = d

        instance = Class(42, "spam")
        exp = "Class(42, 'spam')"
        res = tscr.easy_repr(instance)
        assert res == exp

    def test_long_positional(self):
        """Passed long positional."""
        class Class:
            def __init__(self, a, b, c=None):
                self.a = a
                self.b = b
                self.c = c

        instance = Class(42, "spam" * 42)
        exp = "Class(42, len 168)"
        res = tscr.easy_repr(instance)
        assert res == exp

    def test_long_keyword(self):
        """Passed long keyword."""
        class Class:
            def __init__(self, a, b, c=None):
                self.a = a
                self.b = b
                self.c = c

        instance = Class(42, "spam", c=[1, 2] * 42)
        exp = "Class(42, 'spam', len(c)=84)"
        res = tscr.easy_repr(instance)
        assert res == exp

    def test_combined(self):
        """A combined test."""
        class Class:
            def __init__(self, a, b, c=None, d="foo", *, e, f=None, g=""):
                self.a = a
                self.b = b
                self.c = c
                self.d = d
                self.e = e
                self.f = f
                self.g = g

        instance = Class(42, "spam", d="bar", e=3, g="1")
        exp = "Class(42, 'spam', d='bar', e=3, g='1')"
        res = tscr.easy_repr(instance)
        assert res == exp

    def test_var_positional(self):
        """Error when initialiser has a positional var-arg."""
        class Class:
            def __init__(self, a, b, c=None, *args):
                self.a = a
                self.b = b
                self.c = c
                self.args = args

        instance = Class(42, "spam")
        with pytest.raises(RuntimeError):
            _ = tscr.easy_repr(instance)

    def test_var_keyword(self):
        """Error when initialiser has a keyword var-arg."""
        class Class:
            def __init__(self, a, b, c=None, **kwargs):
                self.a = a
                self.b = b
                self.c = c
                self.kwargs = kwargs

        instance = Class(42, "spam")
        with pytest.raises(RuntimeError):
            _ = tscr.easy_repr(instance)

    def test_repr(self):
        """Usage as ``__repr__``."""
        class Class:
            def __init__(self, a, b, c=None):
                self.a = a
                self.b = b
                self.c = c

            __repr__ = tscr.easy_repr

        instance = Class(42, "spam")
        exp = "Class(42, 'spam')"
        res = repr(instance)
        assert res == exp

    def test_repr_combined(self):
        """A combined test."""
        class Class:
            def __init__(self, a, b, c=None, d="foo", *, e, f=None, g=""):
                self.a = a
                self.b = b
                self.c = c
                self.d = d
                self.e = e
                self.f = f
                self.g = g

            __repr__ = tscr.easy_repr

        instance = Class(42, "spam", d="bar", e=3, g="1")
        exp = "Class(42, 'spam', d='bar', e=3, g='1')"
        res = repr(instance)
        assert res == exp
