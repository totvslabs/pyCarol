# Developer Guidelines

## Run tests

Most unit tests are located at ./test . To run unit tests please use:
```bash
coverage run -m pytest
```

To run a specific test:
```bash
pytest ./test/<test file name>
```

In order to check the results either use the textual output:
```bash
coverage report
```
either the html version
```bash
coverage html
```

## Python

### Formatting

This project use [`black`](https://github.com/psf/black) as formatting tool with 88 columns as max line length. Black can be added to most IDEs. For VS Code, it can be set in *settings.json* with:
```json
    "python.formatting.provider": "black",
    "python.formatting.blackArgs": [
        "--line-length=88"
    ],
    "python.formatting.blackPath": <path to black runnable>,
``` 
Installing black is easy as running 
```bash
pip install black
```

**Please run black on every file before any commit.**

### Linting

This project use pylint and flake8 for checking errors. On VS Code, we use this settings:
```json
    "python.linting.enabled": true,
    "python.linting.lintOnSave": true,
    "python.linting.flake8Enabled": true,
    "python.linting.flake8Path": <path to flake8>,
    "python.linting.flake8Args": [
        "--max-line-length=88",
        "--ignore=E203,E402,W605"
    ],
    "python.linting.pylintEnabled": true,
    "python.linting.pylintPath": <path to pylint>,
    "python.linting.pylintArgs": [
        "--generated-members=th.*,np.*,cv2.*,etree.*,carol.*",
        "--disable=W1202,W1203,W0702,W0612,W0611"
    ],
```
Installing pylint and flake:
```bash
pip install pylint
pip install flake8
```

For linting and conforming docstrings, we use pydocstyle. On VS Code:
```json
    "python.linting.pydocstyleEnabled": true,
    "python.linting.pydocstylePath": <path to pydocstyle>,
    "python.linting.pydocstyleArgs": [
        "--ignore=D100,D102,D103,D213,D406,D407,D413"
    ],
```

### Type hints

Type checks help readability and allow for static checking to find errors before running the code. We suggest that when possible you use [typing hints](https://docs.python.org/3/library/typing.html), specially for function arguments.
On VS Code, you can set them with:
```json
    "python.linting.mypyEnabled": true,
    "python.linting.mypyPath": <path to mypy>,
```

For installing mypy:
```bash
pip install mypy
```

## SQL Guidelines

### Formatting

This project uses [Prettier SQL formatting](https://github.com/sql-formatter-org/sql-formatter). Please use it. It can be installed as an VS Code extension, or you can use it directly online at:

https://sql-formatter-org.github.io/sql-formatter/

Please use the following configurations (VS Code):
```json
    "Prettier-SQL.expressionWidth": 88,
    "Prettier-SQL.keywordCase": "upper",
    "Prettier-SQL.ignoreTabSettings": true,
    "Prettier-SQL.tabSizeOverride": 4
```

## Deprecation
For deprecating functions/methods, please use:

```python

from .utils.deprecation_msgs import deprecated

@deprecated(
    deprecated_in="2.54.6",
    removed_in="2.54.7",
    details="Deprecation message.",
)
def _sync_query():
    ...
```

This decorator will add deprecation message to the function's docstring and it will warn
user of current and future deprecations.
