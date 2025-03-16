# memory-foam


[![pypi](https://img.shields.io/pypi/v/memory-foam.svg)](https://pypi.org/project/memory-foam/)
[![tests](https://github.com/mattseddon/memory-foam/actions/workflows/tests.yml/badge.svg)](https://github.com/mattseddon/memory-foam/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/mattseddon/memory-foam/graph/badge.svg?token=7TT8YRWTV9)](https://codecov.io/gh/mattseddon/memory-foam)

`memory-foam` is a Python package that provides performant iterators for loading files from S3 and GCS into memory for easy processing.

## Features

- **Unified Interface**: Seamlessly interact with files stored in S3 and GCS.
- **Asynchronous Support**: Efficiently load files using asynchronous iterators.

## Installation

You can install `memory-foam` using pip:

```bash
pip install memory-foam
```

## Example usage

```python
from memory_foam import iter_files

...

for pointer, contents in iter_files(uri, client_config):
    results = process(contents)
    data = pointer.to_dict_with(results)
    save(data)
```
