# sfini contribution giude
Thanks for wanting to help out!

## Environment installation
```bash
pip3 install -e .
pip3 install -r tests/requirements.txt
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

## Generating documentation
When the package structure is changed (moved/deleted/new packages/modules), the
documentation configuration must be regenerated:
```bash
sphinx-apidoc -ef -o docs/src/ src/sfini/ --ext-autodoc
```

To build the documentation:
```bash
pip3 install -r docs/requirements.txt
sphinx-build -b html docs/src/ docs/_build/
```

## Code of conduct
Please note that this project is released with a [Contributor Code of Conduct](
CODE_OF_CONDUCT.md).
By participating in this project you agree to abide by its terms.

Breaches of the code of conduct will be treated with according to severity as
deemed by project maintainers. This can include warnings, bannings from project
interaction and reports to GitHub. Correspondence will be made via email.
