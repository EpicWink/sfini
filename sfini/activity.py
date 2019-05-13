# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2018/07/12

"""Activity interfacing.

Activities are separate from state-machines, and are used as
implementations of 'Task' states. Activities are registered separately.
"""

import inspect
import typing as T
import logging as lg
import functools as ft

from . import _util
from . import task_resource as sfini_task_resource

_logger = lg.getLogger(__name__)


class Activity(sfini_task_resource.TaskResource):  # TODO: unit-test
    """Activity execution.

    Note that activity names must be unique (within a region). It's
    recommended to put your code's title and version in the activity name.
    ``Activities`` makes this straight-forward.

    An activity is attached to state-machine tasks, and is called when that
    task is executed. A worker registers itself able to run some
    activities using their names.

    Args:
        name: name of activity
        heartbeat: seconds between heartbeat during activity running
        session: session to use for AWS communication
    """

    service = "activity"

    def __init__(self, name, heartbeat: int = 20, *, session=None):
        super().__init__(name, session=session)
        self.name = name
        self.heartbeat = heartbeat

    def register(self):
        """Register activity with AWS."""
        _util.assert_valid_name(self.name)
        resp = self.session.sfn.create_activity(name=self.name)
        assert resp["activityArn"] == self.arn
        _s = "Activity '%s' registered at %s"
        _logger.info(_s % (self, resp["creationDate"]))


class CallableActivity(Activity):  # TODO: unit-test
    """Activity execution defined by a callable.

    Note that activity names must be unique (within a region). It's
    recommended to put your code's title and version in the activity name.
    ``Activities`` makes this straight-forward.

    An activity is attached to state-machine tasks, and is called when that
    task is executed. A worker registers itself able to run some
    activities using their names.

    Args:
        name: name of activity
        fn: function to run activity
        heartbeat: seconds between heartbeat during activity running
        session: session to use for AWS communication
    """

    def __init__(self, name, fn: T.Callable, heartbeat=20, *, session=None):
        super().__init__(name, heartbeat=heartbeat, session=session)
        self.fn = fn

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def __repr__(self):
        return type(self).__name__ + "(%s, %s, session=%s)" % (
            repr(self.name),
            repr(self.fn),
            repr(self.session))

    @classmethod
    def from_callable(
            cls,
            fn: T.Callable,
            name: str,
            heartbeat: int = 20,
            *,
            session: _util.AWSSession = None
    ) -> "CallableActivity":
        """Create an activity from the callable.

        Args:
            fn: function to run activity
            name: name of activity
            heartbeat: seconds between heartbeat during activity running
            session: session to use for AWS communication
        """

        activity = cls(name, fn, heartbeat=heartbeat, session=session)
        ft.update_wrapper(activity, fn)
        return activity

    def call_with(self, task_input: _util.JSONable) -> _util.JSONable:
        """Call with task-input context.

        Args:
            task_input: task input

        Returns:
            function return-value
        """

        return self.fn(task_input)


class SmartCallableActivity(CallableActivity):  # TODO: unit-test
    """Activity execution defined by a callable, processing input.

    Note that activity names must be unique (within a region). It's
    recommended to put your code's title and version in the activity name.
    ``Activities`` makes this straight-forward.

    An activity is attached to state-machine tasks, and is called when that
    task is executed. A worker registers itself able to run some
    activities using their names.

    Args:
        name: name of activity
        fn: function to run activity
        heartbeat: seconds between heartbeat during activity running
        session: session to use for AWS communication

    Attributes:
        sig: function signature
    """

    def __init__(self, name, fn: T.Callable, heartbeat=20, *, session=None):
        super().__init__(name, fn, heartbeat=heartbeat, session=session)
        self.sig: inspect.Signature = inspect.Signature.from_callable(fn)

    def _get_input_from(
            self,
            task_input: T.Dict[str, _util.JSONable]
    ) -> T.Dict[str, _util.JSONable]:
        """Parse task input for execution input.

        Args:
            task_input: task input

        Returns:
            activity input
        """

        _kws = inspect.Parameter.VAR_KEYWORD
        if any(p.kind is _kws for p in self.sig.parameters.values()):
            return task_input

        kwargs = {}
        for param_name, param in self.sig.parameters.items():
            val = task_input.get(param_name, param.default)
            if val is param.empty:
                _s = "Required parameter '%s' not in task input"
                raise KeyError(_s % param_name)
            kwargs[param_name] = val

        return kwargs

    def call_with(self, task_input: T.Dict[str, _util.JSONable]):
        kwargs = self._get_input_from(task_input)
        return self.fn(**kwargs)


class ActivityRegistration:  # TODO: unit-test
    """Activities registration.

    Provides convenience for grouping activities, generating activity
    names, bulk-registering activities, and activity function decoration.

    An activity is attached to state-machine tasks, and is called when that
    task is executed. A worker registers itself able to run some activities
    using their names.

    Args:
        prefix: prefix for activity names
        session: session to use for AWS communication

    Attributes:
        activities: registered activities

    Example:
        >>> activities = ActivityRegistration(prefix="foo")
        >>> @activities.activity(name="MyActivity")
        >>> def fn(data):
        ...     print("hi")
        >>> print(fn.name)
        fooMyActivity
    """

    _activity_class = CallableActivity
    _smart_activity_class = SmartCallableActivity

    def __init__(self, prefix: str = "", *, session: _util.AWSSession = None):
        self.prefix = prefix
        self.activities: T.Dict[str, Activity] = {}
        self.session = session or _util.AWSSession()

    def __str__(self):
        return "%s '%s'" % (type(self).__name__, self.prefix)

    def __repr__(self):
        return "%s(%s, session=%s)" % (
            type(self).__name__,
            repr(self.prefix),
            repr(self.session))

    @property
    def all_activities(self) -> T.Set[Activity]:
        """All registered activities."""
        return set(self.activities.values())

    def add_activity(self, activity: Activity):
        """Add an activity to the group.

        Args:
            activity: activity to add

        Raises:
            ValueError: if activity the same name as an existing activity
        """

        name = activity.name
        if name in self.activities:
            raise ValueError("Activity '%s' already registered" % name)
        self.activities[name] = activity

    def _activity(
            self,
            activity_cls: T.Type[CallableActivity],
            name: str = None,
            heartbeat: int = 20
    ) -> T.Callable:
        """Activity function decorator.

        Args:
            activity_cls: activity class
            name: name of activity, default: function name
            heartbeat: seconds between heartbeat during activity running
        """

        def wrapper(fn):
            suff = fn.__name__ if name is None else name
            activity = activity_cls.from_callable(
                fn,
                self.prefix + suff,
                heartbeat=heartbeat,
                session=self.session)
            self.add_activity(activity)
            return ft.update_wrapper(activity, fn)
        return wrapper

    def activity(
            self,
            name: str = None,
            heartbeat: int = 20
    ) -> T.Callable:
        """Activity function decorator.

        The decorated function will be passed one argument: the input to
        the task state that executes the activity.

        Args:
            name: name of activity, default: function name
            heartbeat: seconds between heartbeat during activity running
        """

        return self._activity(
            self._activity_class,
            name=name,
            heartbeat=heartbeat)

    def smart_activity(
            self,
            name: str = None,
            heartbeat: int = 20
    ) -> T.Callable:
        """Smart activity function decorator.

        The decorated function will be passed values to its parameters
        from the input to the task state that executes the activity.

        Args:
            name: name of activity, default: function name
            heartbeat: seconds between heartbeat during activity running
        """

        return self._activity(
            self._smart_activity_class,
            name=name,
            heartbeat=heartbeat)

    def register(self):
        """Add registered activities to AWS SFN."""
        for activity in self.activities.values():
            activity.register()

    def _list_activities(self) -> T.List[T.Tuple[str, str, str]]:
        """List activities in SFN."""
        resp = _util.collect_paginated(self.session.sfn.list_activities)
        acts = []
        for act in resp["activities"]:
            prefix = act["name"][:len(self.prefix)]
            if prefix != self.prefix or act["name"] not in self.activities:
                continue
            name = act["name"][len(self.prefix):]
            acts.append((name, act["arn"], act["creationDate"]))
        return acts

    def _deregister_activities(
            self,
            activity_items: T.Sequence[T.Tuple[str, str, str]]):
        """Deregister activities."""
        _logger.info("Deregistering %d activities" % len(activity_items))
        for act in activity_items:
            _logger.debug("Deregistering '%s'" % act[0])
            self.session.sfn.delete_activity(activityArn=act[1])

    def deregister(self):
        """Remove activities in AWS SFN."""
        acts = self._list_activities()
        self._deregister_activities(acts)
