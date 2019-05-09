# sfini
Create AWS Step Functions easily. Pronounced "SFIN-ee".

This package aims to provide a user-friendly interface into defining and
running Step Functions. Things you can do in `sfini` to interact with AWS Step
Functions:
* Implement and register activities
* Define and register state machines
* Start, track and stop executions
* Run workers for activities
* Get information for registered activities and state machines
* De-register state machines and activities

Note: this is not a tool to convert Python code into a Step Functions state
machine. For that, see [pyawssfn](https://github.com/bennorth/pyawssfn).

## Getting started
Prepend `sudo -H` to the following commands if elevated priviliges is
required.

### Prerequisites
* [Python 3](https://www.python.org/) and PIP
* [AWS](https://aws.amazon.com/) account, with
  [IAM](https://aws.amazon.com/iam/) credentials

### Installation
```bash
pip3 install sfini
```

## Usage
### Documentation
Check the [documentation](https://Epic_Wink.gitlab.io/aws-sfn-service) or use the
built-in help:
```bash
pydoc3 sfini
```

```python
import sfini
help(sfini)
```

#### Build documentation
```bash
sphinx-build -b html docs/src/ docs/_build/
```

### AWS Step Functions
[AWS Step Functions](https://aws.amazon.com/step-functions/) (SFN) is a
workflow-management service, providing the ability to coordinate tasks in a
straight-forward fashion.

Further documentation can be found in the [AWS documentation](
https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html).

### Role ARN
Every state-machine needs a role ARN. This is an AWS IAM role ARN which allows
the state-machine to process state executions. See AWS Step Functions
documentation for more information.

### Examples
#### File-processing
```python
import sfini
import pathlib
from PIL import Image

# Define activities
activities = sfini.ActivityRegistration("myPackage")


@activities.smart_activity("resizeActivity")
def resize_activity(image_dir, resized_image_dir, new_size=(64, 64)):
    image_dir = pathlib.Path(image_dir)
    resized_image_dir = pathlib.Path(resized_image_dir)
    for path in image_dir.iterdir():
        resized_path = resized_image_dir / path.relative_to(image_dir)
        Image.open(path).resize(new_size).save(resized_path)
 

@activities.activity("getCentresActivity")
def get_centres_activity(resized_image_dir):
    resized_image_dir = pathlib.Path(resized_image_dir)
    centres = []
    for path in resized_image_dir.iterdir():
        im = Image.open(path)
        centres.append(im.getpixel(im.size[0] // 2, im.size[1] // 2))
    return centres


# Define state-machine
sm = sfini.StateMachine("myStateMachine")

resize_images = sm.task("resizeImages", resize_activity, result_path=None)
sm.start_at(resize_images)

get_centres = sm.task(
    "getCentre",
    get_centres_activity,
    comment="get pixel values of centres of images",
    input_path="$.resized_image_dir",
    result_path="$.res")
resize_images.goes_to(get_centres)

# Register state-machine and activities
activities.register()
sm.register()

# Start activity workers
workers = [sfini.Worker(resize_activity), sfini.Worker(get_centres_activity)]
[w.start() for w in workers]

# Start execution
execution = sm.start_execution(
    execution_input={
        "image_dir": "~/data/images/",
        "resized_image_dir": "~/data/images-small/"})
print(execution.name)
# myStateMachine_2018-07-11T19-07_0354d790

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
```

#### Looping
```python
import sfini

# Define activities
activities = sfini.ActivityRegistration("myPackage")


@activities.activity("increment")
def increment_activity(data):
    return data["counter"] + data["increment"]


# Define state-machine
sm = sfini.StateMachine("myStateMachine")

initialise = sm.pass_("initialise", result=0, result_path="$.counter")
sm.start_at(initialise)

increment = sm.task(
    "increment",
    increment_activity,
    result_path="$.counter")
initialise.goes_to(increment)

check_counter = sm.choice("checkCounter")
increment.goes_to(check_counter)

check_counter.add(sfini.NumericLessThan("$.counter", 10, increment))

end = sm.succeed("end", output_path="$.counter")
check_counter.set_default(end)

# Register state-machine and activities
activities.register()
sm.register()

# Start activity workers
worker = sfini.Worker(increment_activity)
worker.start()

# Start execution
execution = sm.start_execution(execution_input={"increment": 3})
print(execution.name)
# myStateMachine_2018-07-11T19-07_0354d790

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
```

#### Parallel
```python
import sfini
import datetime
import logging as lg

# Define activities
activities = sfini.ActivityRegistration("myPackage")


@activities.activity("logActivity")
def log_message_activity(data):
    lg.log(data["level"], data["message"])


@activities.activity("printActivity")
def print_message_activity(message):
    print(message)
    diff = datetime.timedelta(seconds=len(message) * 5)
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    return now + diff


# Define state-machine
sm = sfini.StateMachine("myStateMachine")

print_and_log = sm.parallel(
    "printAndLog",
    result_path="$.parallel",
    output_path="$.parallel")
sm.start_at(print_and_log)

log_sm = sfini.StateMachine("logSM")
print_and_log.add(log_sm)

log = log_sm.task("log", log_message_activity, result_path=None)
log_sm.start_at(log)

print_sm = sfini.StateMachine("printSM")
print_and_log.add(print_sm)

print_ = print_sm.task("log", print_message_activity, result_path="$.until")
print_sm.start_at(print_)

wait = print_sm.wait("wait", "$.until")
print_.goes_to(wait)

# Register state-machine and activities
activities.register()
sm.register()

# Start activity workers
workers = [
    sfini.Worker(log_message_activity),
    sfini.Worker(print_message_activity)]
[w.start() for w in workers]

# Start execution
execution = sm.start_execution(execution_input={"level": 20, "message": "foo"})
print(execution.name)
# myStateMachine_2018-07-11T19-07-26.53_0354d790

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
```

#### CLI
```python
import sfini

# Define activities
activities = sfini.ActivityRegistration("myPackage")


@activities.activity("printActivity")
def print_activity(data):
    print(data)


# Define state-machine
sm = sfini.StateMachine("myStateMachine")
sm.start_at(sm.task("print", print_activity))

# Parse arguments
sfini.CLI(sm, activities, role_arn="...", version="1.0").parse_args()
```

#### Error handling
```python
import sfini
import time

# Define activities
activities = sfini.ActivityRegistration("myPackage")

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
sm = sfini.StateMachine("myStateMachine")

raise_ = sm.task("raise", raise_activity, timeout=10)
sm.start_at(raise_)

raise_.retry_for("Timeout", interval=3)

fail = sm.fail("fail", error="WorkerError", cause="MyError was raised")
raise_.catch(MyError, fail, result_path="$.error-info")

# Register state-machine and activities
activities.register()
sm.register()

# Start activity workers
worker = sfini.Worker(raise_activity)
worker.start()

# Start execution
execution = sm.start_execution(execution_input={})
print(execution.name)
# myStateMachine_2018-07-11T19-07_0354d790

# Wait for execution and print output
execution.wait()
print(execution.output)
# {"error-info": {"error": "WorkerError", "cause": "MyError was raised"}}

# Stop activity workers
worker.end()
worker.join()

# Deregister state-machine and activities
activities.deregister()
sm.deregister()
```
