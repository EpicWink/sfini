# --- 80 characters -------------------------------------------------------
# Created by: Laurie 2018/08/12

"""Activity wrapper."""

import inspect
import logging as lg
import functools as ft

from . import _util

_logger = lg.getLogger(__name__)


class Activities:  # TODO: unit-test
    """Activities registration.

    Arguments:
        name (str): name of activities group, used in prefix of activity
            names
        version (str): version of activities group, used in prefix of
            activity names
        session (_util.Session): session to use for AWS communication

    Attributes:
        activities (dict[str, Activity]): registered activities
    """

    def __init__(self, name="", version="", *, session=None):
        self.name = name
        self.version = version
        self.activities = {}
        self.session = session or _util.AWSSession()

    def activity(self, name):
        """Activity function decorator.

        Args:
            name (str): name of activity

        Example:
            >>> activities = Activities("foo", "1.0")
            >>> @activities.activity("myActivity")
            >>> def fn():
            ...     print("hi")
        """

        name_ = "%s-%s-%s" % (self.name, self.version, name)
        _util.assert_valid_name(name_)

        def wrapper(fn):
            activity = Activity(name_, fn, session=self.session)
            ft.update_wrapper(activity, fn)
            self.activities[name] = activity
            return activity
        return wrapper

    def register(self):
        """Register all activities with AWS."""
        for activity in self.activities:
            activity.register()

    def unregister(self, version=None):
        """Unregister activities in AWS.

        Arguments:
            version (str): version of activities to unregister,
                default: all versions
        """

        raise NotImplementedError


class Activity:  # TODO: unit-test
    """Activity execution.

    Note that activity names must be unique (within a region). It's
    recommended to put your code's title and version in the activity name.

    Args:
        name (str): name of activity
        fn (callable): function to run activity
        session (_util.Session): session to use for AWS communication
    """

    def __init__(self, name, fn, *, session=None):
        self.name = name
        self.fn = fn
        self.session = session or _util.AWSSession()
        self.sig = inspect.Signature.from_callable(fn)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def register(self):
        """Register activity with AWS."""
        self.session.sfn.create_activity(name=self.name)

    def get_input_from(self, task_input):
        """Parse task input for activity input.

        Arguments:
            task_input (dict): task input

        Returns:
            dict: activity input
        """

        kwargs = {}
        for param_name, param in self.sig.parameters.items():
            if param_name not in task_input:
                if param.default != inspect.Parameter.empty:
                    _s = "Required parameter '%s' not in task input"
                    raise KeyError(_s % param_name)
                kwargs[param_name] = param.default
            else:
                kwargs[param_name] = task_input[param_name]
        return kwargs
