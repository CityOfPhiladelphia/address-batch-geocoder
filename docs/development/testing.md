# Testing

This package uses the pytest module to conduct unit tests. Tests are located in the `tests/` folder.

In order to run all tests, for example:

```
python3 pytest tests/
```

To run tests from one file:

```
python3 pytest tests/test_parser.py
```

To run one test within a file:

```
python3 pytest tests/test_parser.py::test_parse_address
```

## Running the end to end test

 To run the full suite of end to end tests, you will need to export an environment variable with the AIS API KEY. Otherwise, some tests will be skipped.

In mac/linux:

```
export AIS_API_KEY="<AIS_API_KEY>"
```

To do this permanently, you'll need to edit the shell configuration file.

In powershell:

```
$env:AIS_API_KEY = "<AIS_API_KEY>"
```

To do so permanently in powershell:

```
[System.Environment]::SetEnvironmentVariable("VARIABLE_NAME", "value", [System.EnvironmentVariableTarget]::User)
```