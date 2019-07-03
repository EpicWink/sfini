"""Test ``sfini._util``."""

from sfini import _util as tscr
import pytest
from unittest import mock
import logging as lg
import boto3


class TestDefaultParameter:
    """Test `sfini._util.DefaultParameter``."""
    @pytest.fixture
    def default(self):
        """A DefaultParameter instance."""
        return tscr.DefaultParameter()

    def test_bool(self, default):
        """Conversion to boolean."""
        assert not default
        assert bool(default) is False

    def test_eq(self, default):
        """Default paramater equality."""
        other = tscr.DefaultParameter()
        assert default is not other
        assert default == other

    def test_str(self, default):
        """Default paramater stringification."""
        assert str(default) == "<unspecified>"

    def test_repr(self, default):
        """Default paramater string representation."""
        assert repr(default) == "DefaultParameter()"


@pytest.mark.parametrize(
    ("level", "exp_logger_level", "exp_handler_level"),
    [(None, lg.WARNING, lg.NOTSET), (lg.INFO, lg.INFO, lg.INFO)])
def test_setup_logging(level, exp_logger_level, exp_handler_level):
    """Standard-library logging set-up configuration."""
    # Setup environment
    root_logger = lg.getLogger()
    root_logger.setLevel(lg.WARNING)

    # Run function
    with mock.patch.object(root_logger, "handlers", []):
        tscr.setup_logging(level=level)
        handlers = root_logger.handlers

    # Check result
    assert root_logger.level == exp_logger_level
    handler, = handlers
    assert isinstance(handler, lg.StreamHandler)
    fmt = handler.formatter._fmt
    assert "message" in fmt
    assert "asctime" in fmt[:fmt.index("message")]
    assert "levelname" in fmt[:fmt.index("message")]
    assert "name" in fmt[:fmt.index("message")]
    assert handler.level == exp_handler_level


class TestCachedProperty:
    """Test `sfini._util.cached_property``."""
    def test(self):
        """Standard use."""
        with mock.patch.object(tscr, "DEBUG", False):
            class C:
                def __init__(self):
                    self.a = 42

                @tscr.cached_property
                def b(self):
                    return self.a * 2

        # Getting
        c = C()
        assert c.b == 84
        c.a = 3
        assert c.b == 84

        # Setting
        with pytest.raises(AttributeError):
            c.b = 4

        # Deleting
        with pytest.raises(AttributeError):
            del c.b

    def test_debug(self):
        """Debug-model allowing property setting/deleting."""
        with mock.patch.object(tscr, "DEBUG", True):
            class C:
                def __init__(self):
                    self.a = 42

                @tscr.cached_property
                def b(self):
                    return self.a * 2

        # Getting
        c = C()
        assert c.b == 84
        c.a = 3
        assert c.b == 84

        # Setting
        c.b = 4
        assert c.b == 4

        # Deleting
        del c.b
        assert c.b == 6


class TestAssertValidName:
    """AWS-given name validation."""
    @pytest.mark.parametrize("name", ["spam", "a.!@-_+='"])
    def test_valid(self, name):
        """Passes for valid names."""
        tscr.assert_valid_name(name)

    @pytest.mark.parametrize(
        "name",
        [
            "Lorem ipsum dolor sit amet, consectetur adipiscing "
            "elit, sed do eiusmod tempor incididunt",
            "foo bar",
            "spam\nbla",
            "<xml />",
            "spam [type]",
            "{name}",
            "\"name\"",
            "name:spam",
            "#names",
            "eggs?",
            ".*",
            "50%",
            "\\spam",
            "foo^bar",
            "spam|bla",
            "~name",
            "/path/to/name",
            "`name`",
            "$name"
            "foo&bar",
            "foo,bar",
            "spam;bar",
            tscr.INVALID_NAME_CHARACTERS])
    def test_invalid(self, name):
        """Raises for invalid names."""
        with pytest.raises(ValueError) as e:
            tscr.assert_valid_name(name)
        assert name in str(e.value)


def test_collect_paginated():
    """Paginated AWS API endpoint request collection."""
    # Build input
    fn_rvs = [
        {"items": [1, 5, 4], "nextToken": 42},
        {"items": [9, 3, 0], "nextToken": 17},
        {"items": [8]}]
    fn = mock.Mock(side_effect=fn_rvs)
    kwargs = {"a": 128, "b": [{"c": None, "d": "spam"}]}

    # Build expectation
    exp = {"items": [1, 5, 4, 9, 3, 0, 8]}
    exp_calls = [
        mock.call(**kwargs),
        mock.call(nextToken=42, **kwargs),
        mock.call(nextToken=17, **kwargs)]

    # Run function
    res = tscr.collect_paginated(fn, **kwargs)

    # Check result
    assert res == exp
    assert fn.call_args_list == exp_calls


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


class TestAWSSession:
    """Test ``sfini._util.AWSSession``."""
    @pytest.fixture
    def session(self):
        """AWS ``boto3`` session mock."""
        return mock.Mock(spec=boto3.Session)

    @pytest.fixture
    def sfini_session(self, session):
        """An example AWSSession instance."""
        return tscr.AWSSession(session=session)

    def test_init(self, sfini_session, session):
        """AWSSession instantiation."""
        assert sfini_session.session is session

    def test_str(self, sfini_session):
        """AWSSession stringification."""
        sfini_session.credentials = mock.Mock()
        sfini_session.credentials.access_key = "spamkey"
        sfini_session.region = "spamregion"
        res = str(sfini_session)
        assert "spamkey" in res
        assert "spamregion" in res

    def test_repr(self, sfini_session, session):
        """AWSSession string representation."""
        exp = f"AWSSession(session={session!r})"
        res = repr(sfini_session)
        assert res == exp

    def test_credentials(self, sfini_session, session):
        """AWS IAM credentials."""
        res = sfini_session.credentials
        assert res is session.get_credentials.return_value
        session.get_credentials.assert_called_once_with()

    def test_sfn(self, sfini_session, session):
        """AWS Step Functions client."""
        res = sfini_session.sfn
        assert res is session.client.return_value
        session.client.assert_called_once_with("stepfunctions")

    def test_region(self, sfini_session, session):
        """AWS session API region."""
        session.region_name = "spamregion"
        assert sfini_session.region == "spamregion"

    def test_account_id(self, sfini_session, session):
        """AWS account ID."""
        client_mock = mock.Mock()
        session.client.return_value = client_mock
        client_mock.get_caller_identity.return_value = {"Account": "spamacc"}
        assert sfini_session.account_id == "spamacc"
        session.client.assert_called_once_with("sts")
        client_mock.get_caller_identity.assert_called_once_with()
