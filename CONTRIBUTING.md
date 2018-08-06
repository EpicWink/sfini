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
- Write more example(s)
- Write unit tests
- Allow user to specify input, result and output paths for tasks
- Properly deal with state-machine output
- Implement activity deletion from AWS SFN
- Handle execution history nicely
- Handle state-machines in the process of deletion
- Improve CLI service helper
