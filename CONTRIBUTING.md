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
See the [issues page](https://github.com/EpicWink/sfini/issues) for the current
discussions on improvements, features and bugs.

### Generating documentation
When the package structure is changed (moved/deleted/new packages and/or
modules), the documentation configuration must be regenerated:
```bash
sphinx-apidoc -ef -o docs/src/ sfini/ --ext-autodoc
```
