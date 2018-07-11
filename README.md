# sifne
Create AWS Step Functions easily. Pronounced "SIFF-nee".

## Getting started
Prepend `sudo -H` to the following commands if elevated priviliges is
required.

### Prerequisites
* Python 3
* AWS account, with IAM credentials

### Installation
```bash
pip3 install sifne
```

## Usage
```bash
pydoc3 sifne
```

See AWS Step Functions documentation for Step Functions usage.

### Example
Note function parameter names are used to identify state variables: values
are overwritten by return values, and signatures define what state
variables are passed to the function. Unused variables returned by
tasks are provided in execution output.

```python
import sifne


@sifne.task("buyCake", timeout=3.0)
def buy_cake(store_name):
    print("bought cake from %s" % store_name)
    return {"cost": 23.0, "quality": 3.5}


@sifne.task("eatCake")
def eat_cake(quality, quantity, satisfaction=0.0, remaining=1.0):
    if quality < 2.0:
        raise RuntimeError("Cake was bad!")
    print("Mmmmh, yummy!")
    return {
        "satisfaction": satisfaction + quality * quantity,
        "remaining": remaining - quantity,
        "quality": quality - 0.1}


@sifne.task("throwCake", end=True)
def throw_away_cake():
    print("in the bin!")
    return {"satisfaction": 0.0, "remaining": 0.0}


check_cake_remains = sifne.Choice(
    "cakeRemains",
    choices=[
        sifne.NumericGreaterThan("remaining", 0.0, eat_cake),
        sifne.NumericLessThanEquals("remaining", 0.0, sifne.Succeed)],
    default=throw_away_cake)

# State machine definition
sm = sifne.StateMachine("myStateMachine", role_arn="...")
sm.start_at(buy_cake)
buy_cake.goes_to(eat_cake)
buy_cake.retry(TypeError, interval=10, max_attempts=5)
eat_cake.catch(RuntimeError, throw_away_cake)
eat_cake.goes_to(check_cake_remains)
sm.output(["satisfaction"])  # unused variables are also output

sm.register()  # register state machine with AWS SFN
sm.run_worker(block=False)  # start a task worker for all tasks

execution = sm.start_execution(
    execution_input={"store_name": "bestCakeStore", "quantity": 1.1})
print(execution.name)
# myStateMachine_2018-07-11T19-07_0354d790-0b68-4849-a0ba-d4689fd86934

execution.wait()
print(execution.output)
# {'satisfaction': 14.74, 'cost': 23.0}
```

## Development
### Development installation
```bash
pip3 install -e .[dev]
```

### Testing
```bash
pytest -vvrxrs
```

### Style-guide
Follow PEP-8, then Google Python style-guide. In particular, use
Google-style docstrings. Use hanging-indent style, with 4 spaces for
indentation. No lines with just a closing bracket! 80-character lines.
