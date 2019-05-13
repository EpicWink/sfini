# sfini
Create, run and manage AWS Step Functions easily. Pronounced "SFIN-ee".

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
### Prerequisites
* [AWS](https://aws.amazon.com/) (Amazon Web Services) account, with
  access to Step Functions
* AWS [IAM](https://aws.amazon.com/iam/) (Identity and Access Management)
  credentials

### Installation
```bash
pip install sfini
```

## Usage
### Documentation
Check the [documentation](https://Epic_Wink.gitlab.io/aws-sfn-service) or use
the built-in help:
```bash
pydoc sfini
```

```python
import sfini
help(sfini)
```

To build the documentation:
```bash
sphinx-build -b html docs/src/ docs/_build/
```

### AWS Step Functions
[AWS Step Functions](https://aws.amazon.com/step-functions/) (SFN) is a
workflow-management service, providing the ability to coordinate tasks in a
straight-forward fashion. Further documentation can be found in the
[AWS documentation](
https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html).

Usage of Step Functions consists of two types: state-machines and activities.
A state-machine is a graph of operations which defines a workflow of an
application, comprised of multiple types of "states", or stages of the
workflow. An activity processes input to an output, and is used to process a
task "state" in the state-machine (multiple task states can have the same
activity assigned it.

Once a state-machine is defined and registered (along with the used
activities), you run executions of that state-machine on different inputs to
run the workflow. `sfini` allows you to start, stop and get the history of
these executions.

State-machines support conditional branching (and therefore loops), retries
(conditional and unconditional), exception-catching, external AWS service
support for running tasks, parallel execution and input/output processing.
External services including AWS Lambda, so you don't have to deploy your own
activity runners.

Once state-machines and activities are defined and registered, you can view and
update their details in [the SFN web console](
https://console.aws.amazon.com/states/home?#/).

### Role ARN
Every state-machine needs a role ARN (Amazon Resource Name). This is an AWS IAM
role ARN which allows the state-machine to process state executions. See AWS
Step Functions documentation for more information.

### Example
More examples found [in the documentation](
https://Epic_Wink.gitlab.io/aws-sfn-service/examples.html).

```python
import sfini

# Define activities
activities = sfini.ActivityRegistration(prefix="test")


@activities.activity("addActivity")
def add_activity(data):
    return data["a"] + data["b"]


# Define state-machine
sm = sfini.StateMachine("testAdding")

add = sm.task("add", add_activity)
sm.start_at(add)

# Register state-machine and activities
activities.register()
sm.register()

# Start activity worker
worker = sfini.Worker(add_activity)
worker.start()

# Start execution
execution = sm.start_execution(execution_input={"a": 3, "b": 42})
print(execution.name)
# testAdding_2019-05-13T19-07_0354d790

# Wait for execution and print output
execution.wait()
print(execution.output)
# 45

# Stop activity workers
worker.end()
worker.join()

# Deregister state-machine and activities
activities.deregister()
sm.deregister()
```
