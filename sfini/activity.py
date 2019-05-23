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
        self.heartbeat = heartbeat

    def register(self):
        """Register activity with AWS SFN."""
        _util.assert_valid_name(self.name)
        resp = self.session.sfn.create_activity(name=self.name)
        assert resp["activityArn"] == self.arn
        fmt = "Activity '%s' registered with ARN '%s' at %s"
        _logger.info(fmt % (self, self.arn, resp["creationDate"]))

    def is_registered(self) -> bool:
        """See if this activity is registered with AWS SFN.

        Returns:
            if this activity is registered
        """

        _logger.debug("Testing for registration of '%s' on SFN" % self)
        resp = _util.collect_paginated(self.session.sfn.list_activities)
        arns = {sm["activityArn"] for sm in resp["activities"]}
        return self.arn in arns

    def deregister(self):
        """Remove activity from AWS SFN."""
        _logger.info("Deleting activity '%s' from SFN" % self)
        self.session.sfn.delete_activity(activityArn=self.arn)


class CallableActivity(Activity):  # TODO: unit-test
    """Activity execution defined by a callable.

    Note that activity names must be unique (within a region). It's
    recommended to put your application's name and version in the activity
    name. ``ActivityRegistration`` makes this straight-forward.

    An activity is attached to state-machine tasks, and is called when that
    task is executed. A worker registers itself able to run an activity
    using the registered activity name.

    Args:
        name: name of activity
        fn: function to run activity
        heartbeat: seconds between heartbeat during activity running
        session: session to use for AWS communication
    """

    def __init__(self, name, fn: T.Callable, heartbeat=20, *, session=None):
        super().__init__(name, heartbeat=heartbeat, session=session)
        self.fn = fn

    def __call__(self, task_input: _util.JSONable, *args, **kwargs):
        return self.fn(task_input, *args, **kwargs)

    @classmethod
    def decorate(
            cls,
            name: str,
            heartbeat: int = 20,
            *,
            session: _util.AWSSession = None
    ) -> T.Callable[[T.Callable], "CallableActivity"]:
        """Decorate a callable as an activity implementation.

        Args:
            name: name of activity
            heartbeat: seconds between heartbeat during activity running
            session: session to use for AWS communication
        """

        def wrap(fn: T.Callable):
            activity = cls(name, fn, heartbeat=heartbeat, session=session)
            return ft.update_wrapper(activity, fn)
        return wrap

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

    The arguments to ``fn`` are extracted from the input provided by AWS
    Step Functions.

    Note that activity names must be unique (within a region). It's
    recommended to put your application's name and version in the activity
    name. ``ActivityRegistration`` makes this straight-forward.

    An activity is attached to state-machine tasks, and is called when that
    task is executed. A worker registers itself able to run an activity
    using the registered activity name.

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

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def _get_input_from(
            self,
            task_input: T.Dict[str, _util.JSONable]
    ) -> T.Dict[str, _util.JSONable]:
        """Parse task input for execution input.

        Does not perform input validation: ``fn(**kwargs)`` in
        ``call_with`` will do that anyway.

        Args:
            task_input: task input

        Returns:
            activity input
        """

        kinds = {n: p.kind for n, p in self.sig.parameters.items()}
        if any(k == inspect.Parameter.VAR_KEYWORD for k in kinds.values()):
            return task_input

        var_pos = inspect.Parameter.VAR_POSITIONAL
        kwargs = {}
        for name, arg_val in task_input.items():
            if kinds.get(name, var_pos) != var_pos:
                kwargs[name] = arg_val
        return kwargs

    def call_with(self, task_input: T.Dict[str, _util.JSONable]):
        kwargs = self._get_input_from(task_input)
        return self.fn(**kwargs)


class ActivityRegistration:  # TODO: unit-test
    """Activities registration.

    Provides convenience for grouping activities, generating activity
    names, bulk-registering activities, and activity function decoration.

    An activity is attached to state-machine tasks, and is called when that
    task is executed. A worker registers itself able to run an activity
    using the registered activity name.

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
        self.session = session or _util.AWSSession()
        self.activities: T.Dict[str, Activity] = {}

    def __str__(self):
        return "'%s' activities" % self.prefix

    __repr__ = _util.easy_repr

    def add_activity(self, activity: Activity):
        """Add an activity to the group.

        Args:
            activity: activity to add

        Raises:
            ValueError: if activity name already in-use in group
        """

        if activity.name in self.activities:
            raise ValueError("Activity '%s' already in group" % activity.name)
        self.activities[activity.name] = activity

    def _activity(
            self,
            activity_cls: T.Type[CallableActivity],
            name: str = None,
            heartbeat: int = 20
    ) -> T.Callable[[T.Callable], CallableActivity]:
        """Activity function decorator.

        Args:
            activity_cls: activity class
            name: name of activity, default: function name
            heartbeat: seconds between heartbeat during activity running
        """

        def wrap(fn):
            suff = fn.__name__ if name is None else name
            activity = activity_cls.decorate(
                self.prefix + suff,
                heartbeat=heartbeat,
                session=self.session)(fn)
            self.add_activity(activity)
            return activity
        return wrap

    def activity(
            self,
            name: str = None,
            heartbeat: int = 20
    ) -> T.Callable[[T.Callable], CallableActivity]:
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
    ) -> T.Callable[[T.Callable], SmartCallableActivity]:
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
            if prefix != self.prefix and act["name"] not in self.activities:
                continue
            acts.append((act["name"], act["activityArn"], act["creationDate"]))
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
