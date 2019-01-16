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

activities = sfini.ActivityRegistration("myPackage", "1.0")


@activities.activity("resizeActivity")
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


sm = sfini.StateMachine("myStateMachine", role_arn="...")
resize_images = sm.task("resizeImages", resize_activity)
get_centres = sm.task("getCentre", get_centres_activity)
sm.start_at(resize_images)
resize_images.goes_to(get_centres)

activities.register()
sm.register()

workers = sfini.WorkersManager([resize_activity, get_centres_activity])
workers.start()

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
#     "_task_result": [(128, 128, 128), (128, 255, 0), (0, 0, 0), (0, 0, 255)]}

workers.end()
workers.join()
```
