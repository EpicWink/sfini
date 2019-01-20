# sfini contribution giude
Thanks for wanting to help out!

## Environment installation
```bash
pip3 install -e .[dev]
```

## Testing
```bash
pytest -vvrxrs
```

## Style-guide
Follow [PEP-8](https://www.python.org/dev/peps/pep-0008/?), then Google Python
style-guide. In particular, use Google-style docstrings. Use hanging-indent
style, with 4 spaces for indentation. No lines with just a closing bracket!
80-character lines.

## TODO
- Write AWS Step Functions overview
- Write unit tests
- Allow the use of state-variable objects to track task inputs and outputs:
  this will require a local graph to be generated to determine input and output
  paths (development on hold)
- Catch socket read fail after closing worker task poll
- Close worker task polls when another worker picks up a task (currently not
  working)
- Figure out usage of `SecondsPath` in `Wait` state (getting the seconds to
  wait from an integer stored in a state variable)
- Handle Lambda task state exceptions
- Support connected services
  - Include synchronousity
  - Include output handling?
  - Include error handling
- Support resource tagging
