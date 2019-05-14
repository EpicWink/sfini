# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/11

"""Test ``sfini._util``."""

from sfini import _util as tscr
import pytest
from unittest import mock


class TestCallRepr:
    """Test ``sfini._util.call_repr``"""
    def test_no_params(self):
        """Function has no paramaters."""
        def fn():
            pass

        exp = "fn()"
        res = tscr.call_repr(fn)
        assert res == exp

    def test_positional(self):
        """Called with only positional arguments."""
        def fn(a, b):
            pass

        args = (42, "spam")

        exp = "fn(42, 'spam')"
        res = tscr.call_repr(fn, args=args)
        assert res == exp

    def test_positional_with_optional(self):
        """Called with only some positional arguments."""
        def fn(a, b, c=None):
            pass

        args = (42, "spam")

        exp = "fn(42, 'spam')"
        res = tscr.call_repr(fn, args=args)
        assert res == exp

    def test_positional_and_keyword(self):
        """Called with some arguments keyword."""
        def fn(a, b, c):
            pass

        args = (42, "spam")
        kwargs = {"c": [1, 2]}

        exp = "fn(42, 'spam', c=[1, 2])"
        res = tscr.call_repr(fn, args=args, kwargs=kwargs)
        assert res == exp

    def test_positional_and_keyword_with_optional(self):
        """Called with some arguments keyword."""
        def fn(a, b, c=None):
            pass

        args = (42, "spam")
        kwargs = {"c": [1, 2]}

        exp = "fn(42, 'spam', c=[1, 2])"
        res = tscr.call_repr(fn, args=args, kwargs=kwargs)
        assert res == exp

    def test_keyword_only_required(self):
        """Function has required keyword-only."""
        def fn(a, b, c=None, *, d):
            pass

        args = (42, "spam")
        kwargs = {"d": "bla"}

        exp = "fn(42, 'spam', d='bla')"
        res = tscr.call_repr(fn, args=args, kwargs=kwargs)
        assert res == exp

    def test_keyword_only_optional(self):
        """Function has optional keyword-only."""
        def fn(a, b, c=None, *, d=None):
            pass

        args = (42, "spam")

        exp = "fn(42, 'spam')"
        res = tscr.call_repr(fn, args=args)
        assert res == exp

    def test_long_positional(self):
        """Passed long positional."""
        def fn(a, b, c=None):
            pass

        args = (42, "spam" * 42)

        exp = "fn(42, len 168)"
        res = tscr.call_repr(fn, args=args)
        assert res == exp

    def test_long_keyword(self):
        """Passed long keyword."""
        def fn(a, b, c=None):
            pass

        args = (42, "spam")
        kwargs = {"c": [1, 2] * 42}

        exp = "fn(42, 'spam', len(c)=84)"
        res = tscr.call_repr(fn, args=args, kwargs=kwargs)
        assert res == exp

    def test_long_positional_no_shorten(self):
        """Passed long positional disallowing shortening."""
        def fn(a, b, c=None):
            pass

        args = (42, "spam" * 42)

        exp = "fn(42, '" + "spam" * 42 + "')"
        res = tscr.call_repr(fn, args=args, shorten=False)
        assert res == exp

    def test_long_keyword_no_shorten(self):
        """Passed long keyword disallowing shortening."""
        def fn(a, b, c=None):
            pass

        args = (42, "spam")
        kwargs = {"c": [1, 2] * 42}

        exp = "fn(42, 'spam', c=" + repr([1, 2] * 42) + ")"
        res = tscr.call_repr(fn, args=args, kwargs=kwargs, shorten=False)
        assert res == exp

    def test_bad_positional(self):
        """Error on too many positionals."""
        def fn(a, b, c=None):
            pass

        args = (42, "spam", None, [1, 2])

        with pytest.raises(TypeError):
            print(tscr.call_repr(fn, args=args))

    def test_bad_keyword(self):
        """Error on invalid keywords."""
        def fn(a, b, c=None):
            pass

        args = (42, "spam")
        kwargs = {"d": [1, 2]}

        with pytest.raises(TypeError):
            tscr.call_repr(fn, args=args, kwargs=kwargs)
