# sfini
Create AWS Step Functions easily. Pronounced "SFIN-ee".

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
```bash
pydoc3 sfini
```

See [AWS Step Functions](https://aws.amazon.com/step-functions/)
[documentation](https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html)
for Step Functions usage.

You're free to run workers in other processes.

### Role ARN
Every state-machine needs a role ARN. This is an AWS IAM role ARN which allows
the state-machine to process state executions. See AWS Step Functions
documentation for more information.

### Examples

#### File-processing example
```python
import sfini
import pathlib
from PIL import Image

activities = sfini.Activities("myPackage", "1.0")


@activities.activity("resizeActivity")
def list_images_activity(image_dir, resized_image_dir, new_size=(64, 64)):
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
    

sm = sfini.StateMachine("myStateMachine", role_arn="...")
list_images = sm.task("listImages", list_images_activity)
get_centres = sm.task("getCentre", list_images)
sm.start_at(list_images)
list_images.goes_to(get_centres)

activities.register()
sm.register()

list_images_worker = sfini.Worker(list_images_activity)
get_centres_worker = sfini.Worker(get_centres_activity)
list_images_worker.start()
get_centres_worker.start()

execution = sm.start_execution(
    execution_input={
        "image_dir": "~/data/images/",
        "resized_image_dir": "~/data/images-small/"})
print(execution.name)
# myStateMachine_2018-07-11T19-07_0354d790-0b68-4849-a0ba-d4689fd86934

execution.wait()
print(execution.output)
# {
#     "image_dir": "~/data/images/",
#     "resized_image_dir": "~/data/images-small/"
#     "listImages": None,
#     "getCentre": [(128, 128, 128), (128, 255, 0), (0, 0, 0), (0, 0, 255)]}

list_images_worker.end()
get_centres_worker.end()
list_images_worker.join()
get_centres_worker.join()
```

#### Optimistic example
_This example displays functionality to be implemented. In particular, state
variables being defined by function parameters is not available._

Note function parameter names are used to identify state variables: values
are overwritten by return values, and signatures define what state
variables are passed to the function. Unused variables returned by
tasks are provided in execution output.

```python
import sfini

# Define activities
activities = sfini.Activities("myPackage", "1.0")


@activities.activity("buyCakeActivity")
def buy_cake_activity(store_name):
    print("bought cake from %s" % store_name)
    return {"cost": 23.0, "quality": 3.5}


@activities.activity("eatCakeActivity")
def eat_cake_activity(quality, quantity, satisfaction=0.0, remaining=1.0):
    if quality < 2.0:
        raise RuntimeError("Cake was bad!")
    print("Mmmmh, yummy!")
    return {
        "satisfaction": satisfaction + quality * quantity,
        "remaining": remaining - quantity,
        "quality": quality - 0.1}


@activities.activity("throwCakeActivity")
def throw_away_cake_activity():
    print("in the bin!")
    return {"satisfaction": 0.0, "remaining": 0.0}


# Define state-machine
sm = sfini.StateMachine("myStateMachine", role_arn="...")
buy_cake = sm.task("buyCake", buy_cake_activity, timeout=3)
eat_cake = sm.task("eatCake", eat_cake_activity)
throw_away_cake = sm.task(throw_away_cake_activity, "throwCake")
cake_finished = sm.succeed("cakeFinished")
check_cake_remains = sm.choice("cakeRemains")

sm.start_at(buy_cake)
buy_cake.retry_for(TypeError, interval=10, max_attempts=5)
buy_cake.goes_to(eat_cake)
eat_cake.catch(RuntimeError, throw_away_cake)
eat_cake.goes_to(check_cake_remains)
check_cake_remains.add(
    sfini.NumericGreaterThan("remaining", 0.0, eat_cake))
check_cake_remains.add(
    sfini.NumericLessThanEquals("remaining", 0.0, cake_finished))
check_cake_remains.default(throw_away_cake)  # shouldn't occur

# Register with SFN
activities.register()  # register activities with AWS
sm.register()  # register state-machine with AWS

# Start activity workers
workers = {}
for activity_name, activity in activities.activities.items():
    workers[activity_name] = sfini.Worker(activity)
    workers[activity_name].start()

# Run state-machine execution
execution = sm.start_execution(
    execution_input={"store_name": "bestCakeStore", "quantity": 1.1})
print(execution.name)
# myStateMachine_2018-07-11T19-07_0354d790-0b68-4849-a0ba-d4689fd86934

execution.wait()
print(execution.output)
# {'satisfaction': 14.74, 'cost': 23.0}

for worker in workers.values():
    worker.end()
for worker in workers.values():
    worker.join()
```
