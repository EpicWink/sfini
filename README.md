# sfini
Create AWS Step Functions easily. Pronounced "SFIN-ee".

## Getting started
Prepend `sudo -H` to the following commands if elevated priviliges is
required.

### Prerequisites
* Python 3
* AWS account, with IAM credentials

### Installation
```bash
pip3 install sfini
```

## Usage
```bash
pydoc3 sfini
```

See AWS Step Functions documentation for Step Functions usage.

### Example
Note function parameter names are used to identify state variables: values
are overwritten by return values, and signatures define what state
variables are passed to the function. Unused variables returned by
tasks are provided in execution output.

```python
import sfini

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


buy_cake = sfini.Task("buyCake", buy_cake_activity, timeout=3)
eat_cake = sfini.Task("eatCake", eat_cake_activity)
buy_cake.retry(TypeError, interval=10, max_attempts=5)
buy_cake.goes_to(eat_cake)
throw_away_cake = sfini.Task(throw_away_cake_activity, "throwCake")
eat_cake.catch(RuntimeError, throw_away_cake)
cake_finished = sfini.Succeed("cakeFinished")

check_cake_remains = sfini.Choice(
    "cakeRemains",
    choices=[
        sfini.NumericGreaterThan("remaining", 0.0, eat_cake),
        sfini.NumericLessThanEquals("remaining", 0.0, cake_finished)],
    default=throw_away_cake)
eat_cake.goes_to(check_cake_remains)

sm = sfini.StateMachine("myStateMachine", role_arn="...")
sm.start_at(buy_cake)

activities.register()  # register activities with AWS
sm.register()  # register state machine with AWS
sm.run_worker(block=False)  # start a task worker for all tasks

execution = sm.start_execution(
    execution_input={"store_name": "bestCakeStore", "quantity": 1.1})
print(execution.name)
# myStateMachine_2018-07-11T19-07_0354d790-0b68-4849-a0ba-d4689fd86934

execution.wait()
print(execution.output)
# {'satisfaction': 14.74, 'cost': 23.0}
```
