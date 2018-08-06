# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/08/12

"""Activity wrapper."""

import inspect
import logging as lg
import functools as ft

from . import _util

_logger = lg.getLogger(__name__)


class Activity:  # TODO: unit-test
    """Activity execution.

    Note that activity names must be unique (within a region). It's
    recommended to put your code's title and version in the activity name.
    ``Activities`` makes this straight-forward.

    An activity is attached to state-machine tasks, and is called when that
    task is executed. A worker registers itself able to run some
    activities using their names.

    Args:
        name (str): name of activity
        fn (callable): function to run activity
        heartbeat (int): seconds between heartbeat during activity running
        session (_util.Session): session to use for AWS communication
    """

    def __init__(self, name, fn, heartbeat=20, *, session=None):
        self.name = name
        self.fn = fn
        self.heartbeat = heartbeat
        self.session = session or _util.AWSSession()
        self.sig = inspect.Signature.from_callable(fn)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def __str__(self):
        return "%s '%s'" % (type(self).__name__, self.name)

    def __repr__(self):
        return type(self).__name__ + "(%s, %s, session=%s)" % (
            repr(self.name),
            repr(self.fn),
            repr(self.session))

    @classmethod
    def from_callable(cls, fn, name, heartbeat=20, *, session=None):
        """Create an activity from the callable.

        Args:
            fn (callable): function to run activity
            name (str): name of activity
            heartbeat (int): seconds between heartbeat during activity running
            session (_util.Session): session to use for AWS communication
        """

        activity = cls(name, fn, heartbeat=heartbeat, session=session)
        ft.update_wrapper(activity, fn)
        return activity

    @_util.cached_property
    def arn(self) -> str:
        """Activity generated ARN."""
        region = self.session.region
        account = self.session.account_id
        _s = "arn:aws:states:%s:%s:activity:%s"
        return _s % (region, account, self.name)

    def register(self):
        """Register activity with AWS."""
        _util.assert_valid_name(self.name)
        resp = self.session.sfn.create_activity(name=self.name)
        assert resp["activityArn"] == self.arn
        _s = "Activity '%s' registered at %s"
        _logger.info(_s % (self, resp["creationDate"]))

    def _get_input_from(self, task_input):
        """Parse task input for execution input.

        Args:
            task_input (dict): task input

        Returns:
            dict: activity input
        """

        kwargs = {}
        for param_name, param in self.sig.parameters.items():
            val = task_input.get(param_name, param.default)
            if val is param.empty:
                _s = "Required parameter '%s' not in task input"
                raise KeyError(_s % param_name)
            kwargs[param_name] = val
        return kwargs

    def call_with(self, task_input):
        """Call with task-input context.

        Args:
            task_input (dict): task input

        Returns:
            function return-value
        """

        kwargs = self._get_input_from(task_input)
        return self.fn(**kwargs)


class Activities:  # TODO: unit-test
    """Activities registration.

    Provides convenience for grouping activities, generating activity
    names, and bulk-registering activities.

    An activity is attached to state-machine tasks, and is called when that
    task is executed. A worker registers itself able to run some activities
    using their names.

    Args:
        name (str): name of activities group, used in prefix of activity
            names
        version (str): version of activities group, used in prefix of
            activity names
        session (_util.Session): session to use for AWS communication

    Attributes:
        activities (dict[str, Activity]): registered activities

    Example:
        >>> activities = Activities("foo", "1.0")
        >>> @activities.activity("myActivity")
        >>> def fn():
        ...     print("hi")
        >>> print(fn.name)
        foo_1.0_myActivity
    """

    _activity_class = Activity

    def __init__(self, name, version="latest", *, session=None):
        self.name = name
        self.version = version
        self.activities = {}
        self.session = session or _util.AWSSession()

    def __str__(self):
        return "%s '%s' [%s]" % (type(self).__name__, self.name, self.version)

    def __repr__(self):
        return "%s(%s, %s, session=%s)" % (
            type(self).__name__,
            repr(self.name),
            repr(self.version),
            repr(self.session))

    @property
    def all_activities(self):
        """All registered activities."""
        return set(self.activities.values())

    def activity(self, name=None, heartbeat=20):
        """Activity function decorator.

        Args:
            name (str): name of activity, default: function name
            heartbeat (int): seconds between heartbeat during activity running
        """

        pref = "%s-%s-" % (self.name, self.version)

        def wrapper(fn):
            suff = fn.__name__ if name is None else name
            if suff in self.activities:
                raise ValueError("Activity '%s' already registered" % suff)
            activity = Activity.from_callable(
                fn,
                pref + suff,
                heartbeat=heartbeat,
                session=self.session)
            self.activities[suff] = activity
            return activity
        return wrapper

    def register(self):
        """Add registered activities to AWS SFN."""
        for activity in self.activities.values():
            activity.register()

    def deregister(self, version=None):
        """Remove activities in AWS SFN.

        Args:
            version (str): version of activities to remove, default: all other
                versions
        """

        # List activities in SFN
        # Determine available versions
        # Remove all activities with requested version
        raise NotImplementedError  # TODO: implement activity deletion
