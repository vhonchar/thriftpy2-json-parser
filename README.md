# thriftpy2-json-parser

Parse JSON string or dictionary into an object, using [thriftpy2](https://github.com/Thriftpy/thriftpy2).
Example:
- [tests/example.thrift](tests/example.thrift)
- [tests/json2thrift_test.py](tests/json2thrift_test.py)

### How to build

1. `pipenv install` to create a new virtual env and install all dependencies
2. `pipenv run pytest --cov=json2thrift` to run tests