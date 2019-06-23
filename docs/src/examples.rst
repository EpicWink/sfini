Examples
========

More examples:

- `File-processing`_
- `Looping`_
- `Parallel`_
- `CLI`_
- `Error-handling`_

My first ``sfini``
------------------

First, a step-by-step example. We'll begin by defining activities::

    import sfini

    activities = sfini.ActivityRegistration(prefix="test")


    @activities.activity(name="addActivity")
    def add_activity(data):
        return data["a"] + data["b"]

We've created one activity, which when passed some data, will add two of the
values in that data and return the result. This activity is independent of any
state-machine, and will always do what we define it to do. We're using a prefix
in activities registeration to help with unregistering later.

Next, let's define a simple state-machine to utilise our adding activity::

    add = sfini.Task("add", add_activity)
    sm = sfini.construct_state_machine("testAdding", add)

We've added a 'task' as the initial (and in this example, only) state (ie
stage) of the workflow. This task will be implemented by our adding activity.
The workflow input always gets passed to its first state, and in this example
we are passing all of the state input into the activity (same for the output:
all activity output goes to the state, which becomes the workflow output).

To be able to use this activity and state-machine, we must register it with AWS
Step Functions::

    activities.register()
    sm.register()

You may need to pass a role ARN for an IAM account which has permissions to run
state-machine executions: call ``sm.register(role_arn="...")``.

Now, let's start an execution of the state-machine, with some input::

    execution = sm.start_execution(execution_input={"a": 3, "b": 42})
    print(execution.name)
    # testAdding_2019-05-13T19-07_0354d790

The execution is now started, however it's blocked on the 'add' task (which is
the only task). We've now declared, defined and registered our adding activity,
but we need a worker to be able to run executions of the activity. `sfini`'s
workers are implemented in threads, but you're welcome to bring your own

Start a worker to allow the workflow execution to progress through the 'add'
task::

    worker = sfini.Worker(add_activity)
    worker.start()

We can now block the local script's execution by waiting for the execution to
finish::

    execution.wait()
    print(execution.output)
    # 45

Executions track the progress of the running of the state-machine, and have
knowledge of the full history of the process. Once they're finished, we can get
the workflow's output, like above.

Clean-up: turn off our workers. Calling ``end`` on the worker prevents new
activity executions from occuring, but won't kill any current executions (use
CTRL+C or your favourite interrupt/kill signal sender for that). ``join``
simply waits for the thread to finish::

    worker.end()
    worker.join()

And more clean-up: unregister the adding activity and state-machine (unless
you're felling particularly attached)::

    activities.deregister()
    sm.deregister()

This will only unregister the adding activity.

More examples
-------------

Enabling log output for these examples may be helpful:

.. code-block:: python

    import logging as lg
    lg.basicConfig(
        level=lg.DEBUG,
        format="[%(levelname)8s] %(name)s: %(message)s")

File-processing
^^^^^^^^^^^^^^^

.. code-block:: python

    import sfini
    import pathlib
    from PIL import Image

    # Define activities
    activities = sfini.ActivityRegistration(prefix="sfiniActs")


    @activities.smart_activity("resizeActivity")
    def resize_activity(image_dir, resized_image_dir, new_size=(64, 64)):
        image_dir = pathlib.Path(image_dir)
        resized_image_dir = pathlib.Path(resized_image_dir)
        for path in image_dir.iterdir():
            resized_path = resized_image_dir / path.relative_to(image_dir)
            print("Resizing image '%s'" % path)
            Image.open(path).resize(new_size).save(resized_path)


    @activities.activity("getCentresActivity")
    def get_centres_activity(resized_image_dir):
        resized_image_dir = pathlib.Path(resized_image_dir)
        centres = []
        for path in resized_image_dir.iterdir():
            im = Image.open(path)
            centres.append(im.getpixel((im.size[0] // 2, im.size[1] // 2)))
        return centres


    # Define state-machine
    resize_images = sfini.Task(
        "resizeImages",
        resize_activity,
        result_path=None)

    get_centres = sfini.Task(
        "getCentre",
        get_centres_activity,
        comment="get pixel values of centres of images",
        input_path="$.resized_image_dir",
        result_path="$.res")
    resize_images.goes_to(get_centres)

    sm = sfini.construct_state_machine("sfiniSM", resize_images)

    # Register state-machine and activities
    activities.register()
    sm.register()

    # Start activity workers
    workers = [
        sfini.Worker(resize_activity),
        sfini.Worker(get_centres_activity)]
    [w.start() for w in workers]

    # Start execution
    execution = sm.start_execution(
        execution_input={
            "image_dir": "~/data/images/",
            "resized_image_dir": "~/data/images-small/"})
    print(execution.name)
    # sfiniSM-07-11T19-07_0354d790

    # Wait for execution and print output
    execution.wait()
    print(execution.output)
    # {
    #     "image_dir": "~/data/images/",
    #     "resized_image_dir": "~/data/images-small/"
    #     "res": [(128, 128, 128), (128, 255, 0), (0, 0, 0), (0, 0, 255)]}

    # Stop activity workers
    [w.end() for w in workers]
    [w.join() for w in workers]

    # Deregister state-machine and activities
    activities.deregister()
    sm.deregister()


Looping
^^^^^^^

.. code-block:: python

    import sfini

    # Define activities
    activities = sfini.ActivityRegistration(prefix="sfiniActs")


    @activities.activity("increment")
    def increment_activity(data):
        return data["counter"] + data["increment"]


    # Define state-machine
    initialise = sfini.Pass(
        "initialise",
        result=0,
        result_path="$.counter")

    increment = sfini.Task(
        "increment",
        increment_activity,
        result_path="$.counter")
    initialise.goes_to(increment)

    check_counter = sfini.Choice("checkCounter")
    increment.goes_to(check_counter)

    check_counter.add(sfini.NumericLessThan("$.counter", 10, increment))

    end = sfini.Succeed("end", output_path="$.counter")
    check_counter.set_default(end)

    sm = sfini.construct_state_machine("sfiniSM", initialise)

    # Register state-machine and activities
    activities.register()
    sm.register()

    # Start activity workers
    worker = sfini.Worker(increment_activity)
    worker.start()

    # Start execution
    execution = sm.start_execution(execution_input={"increment": 3})
    print(execution.name)
    # sfiniSM-07-11T19-07_0354d790

    # Wait for execution and print output
    execution.wait()
    print(execution.output)
    # 12

    # Stop activity workers
    worker.end()
    worker.join()

    # Deregister state-machine and activities
    activities.deregister()
    sm.deregister()


Parallel
^^^^^^^^

.. code-block:: python

    import sfini
    import datetime
    import logging as lg

    # Define activities
    activities = sfini.ActivityRegistration(prefix="sfiniActs")


    @activities.activity("logActivity")
    def log_message_activity(data):
        lg.log(data["level"], data["message"])


    @activities.activity("printActivity")
    def print_message_activity(message):
        print(message)
        diff = datetime.timedelta(seconds=len(message) * 5)
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        return (now + diff).isoformat()


    # Define state-machine
    print_and_log = sfini.Parallel(
        "printAndLog",
        result_path="$.parallel",
        output_path="$.parallel")

    log = sfini.Task("log", log_message_activity, result_path=None)
    log_sm = sfini.construct_state_machine("logSM", log)

    print_ = sfini.Task(
        "print",
        print_message_activity,
        result_path="$.until")
    wait = sfini.Wait("wait", "$.until")
    print_.goes_to(wait)
    print_sm = sfini.construct_state_machine("printSM", print_)

    print_and_log.add(log_sm)
    print_and_log.add(print_sm)

    sm = sfini.construct_state_machine("sfiniSM", print_and_log)

    # Register state-machine and activities
    activities.register()
    sm.register()

    # Start activity workers
    workers = [
        sfini.Worker(log_message_activity),
        sfini.Worker(print_message_activity)]
    [w.start() for w in workers]

    # Start execution
    execution = sm.start_execution(
        execution_input={"level": 20, "message": "foo"})
    print(execution.name)
    # sfiniSM-07-11T19-07-26.53_0354d790

    # Wait for execution and print output
    execution.wait()
    print(execution.output)
    # [
    #     {"level": 20, "message": "foo"},
    #     {"level": 20, "message": "foo", "until": "2018-07-11T19-07-42.53"}]

    # Stop activity workers
    [w.end() for w in workers]
    [w.join() for w in workers]

    # Deregister state-machine and activities
    activities.deregister()
    sm.deregister()


CLI
^^^

.. code-block:: python

    import sfini

    # Define activities
    activities = sfini.ActivityRegistration(prefix="sfiniActs")


    @activities.activity("printActivity")
    def print_activity(data):
        print(data)


    # Define state-machine
    print_ = sfini.Task("print", print_activity)
    sm = sfini.construct_state_machine("sfiniSM", print_)

    # Parse arguments
    sfini.CLI(sm, activities, role_arn="...", version="1.0").parse_args()


Error-handling
^^^^^^^^^^^^^^

.. code-block:: python

    import sfini
    import time

    # Define activities
    activities = sfini.ActivityRegistration(prefix="sfiniActs")

    sleep_time = 15


    class MyError(Exception):
        pass


    @activities.activity("raiseActivity")
    def raise_activity(data):
        global sleep_time
        time.sleep(sleep_time)
        sleep_time -= 10
        raise MyError("foobar")


    # Define state-machine
    raise_ = sfini.Task("raise", raise_activity, timeout=10)
    raise_.retry_for(["States.Timeout"], interval=3)

    fail = sfini.Fail(
        "fail",
        error="WorkerError",
        cause="MyError was raised")
    raise_.catch(["MyError"], fail, result_path="$.error-info")

    sm = sfini.construct_state_machine("sfiniSM", raise_)

    # Register state-machine and activities
    activities.register()
    sm.register()

    # Start activity workers
    worker = sfini.Worker(raise_activity)
    worker.start()

    # Start execution
    execution = sm.start_execution(execution_input={})
    print(execution.name)
    # sfiniSM-07-11T19-07_0354d790

    # Wait for execution and print output
    execution.wait()
    print(execution.format_history())
    # ExecutionStarted [1] @ 2019-06-23 19:27:34.026000+10:00
    # TaskStateEntered [2] @ 2019-06-23 19:27:34.052000+10:00:
    #   name: raise
    # ActivityScheduled [3] @ 2019-06-23 19:27:34.052000+10:00:
    #   resource: arn:...:sfiniActsraiseActivity
    # ActivityStarted [4] @ 2019-06-23 19:27:34.130000+10:00:
    #   worker: myWorker-81a5a3e4
    # ActivityTimedOut [5] @ 2019-06-23 19:27:44.131000+10:00:
    #   error: States.Timeout
    # ActivityScheduled [6] @ 2019-06-23 19:27:47.132000+10:00:
    #   resource: arn:...:sfiniActsraiseActivity
    # ActivityStarted [7] @ 2019-06-23 19:30:45.637000+10:00:
    #   worker: myWorker-4b6b9dfb
    # ActivityFailed [8] @ 2019-06-23 19:30:50.908000+10:00:
    #   error: MyError
    # TaskStateExited [9] @ 2019-06-23 19:30:50.908000+10:00:
    #   name: raise
    # FailStateEntered [10] @ 2019-06-23 19:30:50.916000+10:00:
    #   name: fail
    # ExecutionFailed [11] @ 2019-06-23 19:30:50.916000+10:00:
    #   error: WorkerError

    # Stop activity workers
    worker.end()
    worker.join()

    # Deregister state-machine and activities
    activities.deregister()
    sm.deregister()
