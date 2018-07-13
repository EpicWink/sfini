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

    Provides convenience for grouping activities, generating activity
    names, and bulk-registering activities.

    An activity is attached to state-machine tasks, and is called when that
    task is executed. A worker registers itself able to run some
    activities using their names.

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

    def __init__(self, name, version="latest", *, session=None):
        self.name = name
        self.version = version
        self.activities = {}
        self.session = session or _util.AWSSession()

    def activity(self, name=None):
        """Activity function decorator.

        Args:
            name (str): name of activity, default: function name
        """

        pref = "%s-%s-" % (self.name, self.version)

        def wrapper(fn):
            suff = fn.__name__ if name is None else name
            if suff in self.activities:
                raise ValueError("Activity '%s' already registered" % suff)
            activity = Activity.from_callable(
                fn,
                pref + suff,
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
            version (str): version of activities to remove, default: all
                other versions
        """

        # List activities in SFN
        # Determine available versions
        # Remove all activities with requested version
        raise NotImplementedError  # TODO: implement activity deletion


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
        session (_util.Session): session to use for AWS communication
    """

    def __init__(self, name, fn, *, session=None):
        self.name = name
        self.fn = fn
        self.session = session or _util.AWSSession()
        self.sig = inspect.Signature.from_callable(fn)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    @classmethod
    def from_callable(cls, fn, name, *, session=None):
        """Create an activity from the callable.

        Args:
            fn (callable): function to run activity
            name (str): name of activity
            session (_util.Session): session to use for AWS communication
        """

        _util.assert_valid_name(name)
        activity = cls(name, fn, session=session)
        ft.update_wrapper(activity, fn)
        return activity

    def register(self):
        """Register activity with AWS."""
        self.session.sfn.create_activity(name=self.name)

    def get_input_from(self, task_input):
        """Parse task input for activity input.

        Args:
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
