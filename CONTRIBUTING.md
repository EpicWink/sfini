# sfini contribution giude
Thanks for wanting to help out!

## Environment installation
```bash
pip3 install -e .[dev]
```

## Testing
```bash
pytest
```

## Style-guide
Follow [PEP-8](https://www.python.org/dev/peps/pep-0008/?), then Google Python
style-guide (for the most part). In particular, use Google-style docstrings.
Use hanging-indent style, with 4 spaces for indentation. No lines with just a
closing bracket! 80-character lines.

## TODO
See the [issues page](https://gitlab.com/Epic_Wink/aws-sfn-service/issues) for
the current discussions on improvements, features and bugs.

### Version 0.1
- Close worker task polls when another worker picks up a task (currently not
  working)
  - Catch socket read fail after closing worker task poll
- Handle Lambda task state exceptions
- Write unit tests
- Write AWS Step Functions overview

### Version 1.0
- Figure out usage of `SecondsPath` in [`Wait`](
  https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-wait-state.html)
  state (getting the seconds to wait from an integer stored in a state
  variable)
- Support [resource tagging](
  https://docs.aws.amazon.com/step-functions/latest/dg/concepts-tagging.html)
- Support [connected services](
  https://docs.aws.amazon.com/step-functions/latest/dg/concepts-connectors.html)
  - Include synchronousity
  - Include output handling?
  - Include error handling
- Use `typing` for all public methods

### Future versions
- Allow the use of state-variable objects to track task inputs and outputs:
  this will require a local graph to be generated to determine input and output
  paths (development on hold)
