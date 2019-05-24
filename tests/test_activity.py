# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/13

"""Test ``sfini.activity``."""

from sfini import activity as tscr
import pytest
from unittest import mock
import sfini
from sfini import _util as sfini_util
import datetime
import inspect


@pytest.fixture
def session_mock():
    """AWS session mock."""
    return mock.Mock(autospec=sfini.AWSSession)


class TestActivity:
    """Test ``sfini.activity.Activity``."""
    @pytest.fixture
    def activity(self, session_mock):
        """An Activity instance."""
        return tscr.Activity("spam", heartbeat=42, session=session_mock)

    def test_init(self, activity, session_mock):
        """Activity initialisation."""
        assert activity.name == "spam"
        assert activity.heartbeat == 42
        assert activity.session is session_mock

    def test_register(self, activity, session_mock):
        """Activity registration."""
        # Setup environment
        activity.arn = "spam:arn"
        session_mock.sfn.create_activity.return_value = {
            "activityArn": "spam:arn",
            "creationDate": datetime.datetime.now(tz=datetime.timezone.utc)}
        avn_mock = mock.Mock()

        # Run function
        with mock.patch.object(sfini_util, "assert_valid_name", avn_mock):
            activity.register()

        # Check result
        session_mock.sfn.create_activity.assert_called_once_with(name="spam")
        avn_mock.assert_called_once_with("spam")

    @pytest.mark.parametrize(
        ("names", "exp"),
        [(["foo", "bar"], False), (["foo", "spam", "bar"], True)])
    def test_is_registered(self, activity, session_mock, names, exp):
        """Checking for activity registration."""
        activity.arn = "spam:arn"
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        activities = [
            {"name": n, "activityArn": n + ":arn", "creationDate": now}
            for n in names]
        session_mock.sfn.list_activities.return_value = {
            "activities": activities}
        assert activity.is_registered() is exp

    def test_deregister(self, activity, session_mock):
        """Activity de-registration."""
        activity.arn = "spam:arn"
        activity.deregister()
        session_mock.sfn.delete_activity.assert_called_once_with(
            activityArn=activity.arn)


class TestCallableActivity:
    """Test ``sfini.activity.CallableActivity``."""
    @pytest.fixture
    def fn(self):
        return mock.Mock()

    @pytest.fixture
    def activity(self, session_mock, fn):
        """A CallableActivity instance."""
        return tscr.CallableActivity("spam", fn, heartbeat=42, session=session_mock)

    def test_init(self, activity, session_mock, fn):
        """CallableActivity initialisation."""
        assert activity.name == "spam"
        assert activity.fn is fn
        assert activity.heartbeat == 42
        assert activity.session is session_mock

    class TestCall:
        """CallableActivity calling."""
        def test_valid(self, activity, fn):
            """With valid parameters."""
            args = (42, "bla")
            kwargs = {"foo": [1, 2], "bar": None}
            res = activity(*args, **kwargs)
            assert res is fn.return_value
            fn.assert_called_once_with(*args, **kwargs)

        def test_no_positional(self, activity):
            """Without any positional arguments."""
            with pytest.raises(TypeError) as e:
                activity(foo=[1, 2], bar=None)
            assert "task_input" in str(e.value)

    def test_decorate(self, session_mock):
        """Callable decoration."""
        @tscr.CallableActivity.decorate(
            "bla",
            heartbeat=17,
            session=session_mock)
        def res(task_input):
            return task_input["c"]

        assert isinstance(res, tscr.CallableActivity)
        assert callable(res.fn)
        assert res.fn is res.__wrapped__
        assert res.name == "bla"
        assert res.heartbeat == 17
        assert res.session is session_mock

    def test_call_with(self, activity, fn):
        """Calling with data input."""
        task_input = {"a": 42, "b": "bla", "c": {"foo": [1, 2], "bar": None}}
        res = activity.call_with(task_input)
        assert res is fn.return_value
        fn.assert_called_once_with(task_input)


class TestSmartCallableActivity:
    """Test ``sfini.activity.SmartCallableActivity``."""
    @staticmethod
    def fn(a, b, c=None):
        return {"a": a, "b": b, "c": c}

    @pytest.fixture
    def activity(self, session_mock):
        """A SmartCallableActivity instance."""
        return tscr.SmartCallableActivity(
            "spam",
            TestSmartCallableActivity.fn,
            heartbeat=42,
            session=session_mock)

    def test_init(self, activity, session_mock):
        """SmartCallableActivity initialisation."""
        assert activity.name == "spam"
        assert activity.fn is self.fn
        assert activity.heartbeat == 42
        assert activity.session is session_mock
        assert activity.sig == inspect.signature(self.fn)

    class TestGetInputFrom:
        """Argument extract from input."""
        def test_all_provided(self, activity):
            """All and only parameters are provided."""
            task_input = {"a": 42, "b": "l", "c": {"foo": [1, 2], "bar": None}}
            exp = {"a": 42, "b": "l", "c": {"foo": [1, 2], "bar": None}}
            res = activity._get_input_from(task_input)
            assert res == exp

        def test_some_provided(self, activity):
            """Some and only parameters are provided."""
            task_input = {"a": 42, "b": "l"}
            exp = {"a": 42, "b": "l"}
            res = activity._get_input_from(task_input)
            assert res == exp

        def test_extra_provided(self, activity):
            """Extra input is provided."""
            task_input = {"a": 42, "b": "l", "d": [1, 4, 9, 16, 25]}
            exp = {"a": 42, "b": "l"}
            res = activity._get_input_from(task_input)
            assert res == exp

        def test_var_keyword(self, activity):
            """Callable has var-keyword parameter (ie ``**kwargs``)."""
            def fn(a, b, c=None, **kwargs):
                return {"a": a, "b": b, "c": c, "kwargs": kwargs}

            activity.sig = inspect.signature(fn)
            task_input = {"a": 42, "b": "l", "d": [1, 4, 9, 16, 25]}
            exp = {"a": 42, "b": "l", "d": [1, 4, 9, 16, 25]}
            res = activity._get_input_from(task_input)
            assert res == exp

        def test_var_positional(self, activity):
            """Callable has var-positional parameter (ie ``**args``)."""
            def fn(a, b, *args, c=None):
                return {"a": a, "b": b, "c": c, "args": args}

            activity.sig = inspect.signature(fn)
            task_input = {"a": 42, "b": "l", "args": [1, 4, 9, 16, 25]}
            exp = {"a": 42, "b": "l"}
            res = activity._get_input_from(task_input)
            assert res == exp

    class TestCallWith:
        """Calling with data input."""
        def test_valid(self, activity):
            """Provided valid function input."""
            kwargs = {"a": 42, "b": "bla"}
            activity._get_input_from = mock.Mock(return_value=kwargs)
            task_input = {"a": 42, "b": "bla", "d": [1, 4, 9, 16, 25]}
            exp = {"a": 42, "b": "bla", "c": None}
            res = activity.call_with(task_input)
            assert res == exp
            activity._get_input_from.assert_called_once_with(task_input)

        def test__missing_parameter(self, activity):
            """Not provided all required arguments."""
            kwargs = {"a": 42}
            activity._get_input_from = mock.Mock(return_value=kwargs)
            task_input = {"a": 42, "d": [1, 4, 9, 16, 25]}
            with pytest.raises(TypeError) as e:
                activity.call_with(task_input)
            assert "b" in str(e.value)
            activity._get_input_from.assert_called_once_with(task_input)


class TestActivityRegistration:
    """Test ``sfini.activity.ActivityRegistration``."""
    @pytest.fixture
    def activities(self, session_mock):
        """An ActivityRegistration instance."""
        return tscr.ActivityRegistration(prefix="spam", session=session_mock)

    def test_init(self, activities, session_mock):
        """ActivityRegistration initialisation."""
        assert activities.prefix == "spam"
        assert activities.session is session_mock
        assert not activities.activities

    def test_str(self, activities):
        """ActivityRegistration stringification."""
        assert "spam" in str(activities)

    def test_repr(self, activities, session_mock):
        """ActivityRegistration string representation."""
        fmt = "ActivityRegistration(prefix='spam', session=%s)"
        exp = fmt % repr(session_mock)
        assert repr(activities) == exp

    class TestAddActivity:
        """Adding activity to the group."""
        def test_not_in_use(self, activities):
            """Successful adding as name not already in group."""
            class Activity:
                name = "bla"
                heartbeat = 42

            activity = Activity()
            assert not activities.activities
            activities.add_activity(activity)
            assert activities.activities == {"bla": activity}

        def test_in_use(self, activities):
            """Name already in group."""
            class Activity:
                name = "bla"
                heartbeat = 42

            activity = Activity()
            activities.activities = {"bla": Activity()}
            with pytest.raises(ValueError) as e:
                activities.add_activity(activity)
            assert "bla" in str(e.value)

    class TestActivityP:
        """Test ``sfini.activity.ActivityRegistration._activity``."""
        def test_name(self, activities, session_mock):
            """Name provided."""
            # Setup environment
            activities.add_activity = mock.Mock()

            # Build input
            activity_mock = mock.Mock(spec=tscr.CallableActivity)
            activity_cls = mock.Mock()
            wrap = mock.Mock(return_value=activity_mock)
            activity_cls.decorate.return_value = wrap

            fn = mock.Mock()
            fn.__name__ = "foo"

            # Run function
            res = activities._activity(
                activity_cls,
                name="bla",
                heartbeat=42)(fn)

            # Check result
            assert res is activity_mock
            activity_cls.decorate.assert_called_once_with(
                "spambla",
                heartbeat=42,
                session=session_mock)
            wrap.assert_called_once_with(fn)
            activities.add_activity.assert_called_once_with(activity_mock)

        def test_no_name(self, activities, session_mock):
            """Name not provided."""
            # Setup environment
            activities.add_activity = mock.Mock()

            # Build input
            activity_mock = mock.Mock(spec=tscr.CallableActivity)
            activity_cls = mock.Mock()
            wrap = mock.Mock(return_value=activity_mock)
            activity_cls.decorate.return_value = wrap

            fn = mock.Mock()
            fn.__name__ = "foo"

            # Run function
            res = activities._activity(activity_cls, heartbeat=42)(fn)

            # Check result
            assert res is activity_mock
            activity_cls.decorate.assert_called_once_with(
                "spamfoo",
                heartbeat=42,
                session=session_mock)
            wrap.assert_called_once_with(fn)
            activities.add_activity.assert_called_once_with(activity_mock)

    def test_activity(self, activities):
        """CallableActivity construction decorator."""
        activities._activity = mock.Mock()
        res = activities.activity(name="bla", heartbeat=42)
        assert res is activities._activity.return_value
        activities._activity.assert_called_once_with(
            tscr.CallableActivity,
            name="bla",
            heartbeat=42)

    def test_smart_activity(self, activities):
        """SmartCallableActivity construction decorator."""
        activities._activity = mock.Mock()
        res = activities.smart_activity(name="bla", heartbeat=42)
        assert res is activities._activity.return_value
        activities._activity.assert_called_once_with(
            tscr.SmartCallableActivity,
            name="bla",
            heartbeat=42)

    def test_register(self, activities):
        """Activity group registration."""
        activities.activities = {
            "spambla": mock.Mock(spec=tscr.Activity),
            "spamfoo": mock.Mock(spec=tscr.Activity),
            "bar": mock.Mock(spec=tscr.Activity)}
        activities.register()
        for activity in activities.activities.values():
            activity.register.assert_called_once_with()

    def test_list_activities(self, activities, session_mock):
        """Activity group listing."""
        # Setup environment
        now = datetime.datetime.now()
        resp = {
            "activities": [
                {
                    "name": "spamfoo",
                    "activityArn": "spamfoo:arn",
                    "creationDate": now - datetime.timedelta(minutes=1)},
                {
                    "name": "bar",
                    "activityArn": "bar:arn",
                    "creationDate": now - datetime.timedelta(minutes=2)},
                {
                    "name": "another",
                    "activityArn": "another:arn",
                    "creationDate": now - datetime.timedelta(days=1)}]}
        session_mock.sfn.list_activities = mock.Mock(return_value=resp)
        activities.activities = {
            "spambla": mock.Mock(spec=tscr.Activity),
            "bar": mock.Mock(spec=tscr.Activity)}

        # Build expectation
        exp = [
            ("spamfoo", "spamfoo:arn", now - datetime.timedelta(minutes=1)),
            ("bar", "bar:arn", now - datetime.timedelta(minutes=2))]

        # Run function
        res = activities._list_activities()

        # Check result
        assert res == exp
        session_mock.sfn.list_activities.assert_called_once_with()

    def test_deregister_activities(self, activities, session_mock):
        """Activity de-registration."""
        now = datetime.datetime.now()
        activity_items = [
            ("spamfoo", "spamfoo:arn", now - datetime.timedelta(minutes=1)),
            ("bar", "bar:arn", now - datetime.timedelta(minutes=2))]
        exp_da_calls = [
            mock.call.delete_activity(activityArn="spamfoo:arn"),
            mock.call.delete_activity(activityArn="bar:arn")]
        activities._deregister_activities(activity_items)
        assert session_mock.sfn.method_calls == exp_da_calls

    def test_deregister(self, activities):
        """Activity group de-registration."""
        # Setup environment
        now = datetime.datetime.now()
        acts = [
            ("spamfoo", "spamfoo:arn", now - datetime.timedelta(minutes=1)),
            ("bar", "bar:arn", now - datetime.timedelta(minutes=2))]
        activities._list_activities = mock.Mock(return_value=acts)
        activities._deregister_activities = mock.Mock()

        # Run function
        activities.deregister()

        # Check result
        activities._list_activities.assert_called_once_with()
        activities._deregister_activities.assert_called_once_with(acts)
