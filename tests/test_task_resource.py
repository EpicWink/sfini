"""Test ``sfini.task_resource``."""

from sfini import task_resource as tscr
import pytest
from unittest import mock
import sfini


@pytest.fixture
def session():
    """AWS session mock."""
    return mock.MagicMock(autospec=sfini.AWSSession)


class TestTaskResource:
    """Test ``sfini.task_resource.TaskResource``."""
    @pytest.fixture
    def task_resource(self, session):
        """An example TaskResource instance."""
        return tscr.TaskResource("spam", session=session)

    def test_init(self, task_resource, session):
        """Correct attributes after instantiation."""
        assert task_resource.name == "spam"
        assert task_resource.session is session

    def test_str(self, task_resource):
        """TaskResource stringification."""
        task_resource.service = "bla"
        res = str(task_resource)
        assert "bla" in res
        assert "spam" in res

    def test_repr(self, task_resource, session):
        """TaskResource representation."""
        exp = "TaskResource('spam', session=%s)" % repr(session)
        res = repr(task_resource)
        assert res == exp

    def test_arn(self, task_resource, session):
        """TaskResource instance ARN."""
        session.region = "space"
        session.account_id = "1234"
        task_resource.service = "bla"
        exp = "arn:aws:states:space:1234:bla:spam"
        res = task_resource.arn
        assert res == exp


class TestLambda:
    """Test ``sfini.task_resource.Lambda``."""
    @pytest.fixture
    def task_resource(self, session):
        """An example TaskResource instance."""
        return tscr.Lambda("spam", session=session)

    def test_class(self):
        """Class structure, including inheritence"""
        assert issubclass(tscr.Lambda, tscr.TaskResource)
        assert tscr.Lambda.service == "function"

    def test_arn(self, task_resource, session):
        """TaskResource instance ARN."""
        session.region = "space"
        session.account_id = "1234"
        exp = "arn:aws:lambda:space:1234:function:spam"
        res = task_resource.arn
        assert res == exp

