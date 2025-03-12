# memory-foam

`memory-foam` is a Python package that provides a set of iterators to load the contents of files from various cloud storage services (S3, GCS, and Azure) into memory for easy processing.

## Features

- **Unified Interface**: Seamlessly interact with files stored in S3, GCS, and Azure using a consistent API.
- **Asynchronous Support**: Efficiently load files using asynchronous iterators.
- **Version Awareness**: Handle different versions of files with ease.

## Installation

You can install `memory-foam` using pip:

```bash
pip install memory-foam
```

## Example usage

```python
from io import BytesIO
from memory_foam import get_entries
from PIL import Image
from ultralytics import YOLO


if __name__ == "__main__":
    yolo = YOLO("yolo11n.pt", verbose=False)
    uri = "s3://ldb-public/remote/data-lakes/ISIA_500/Croissant"
    for pointer, contents in get_entries(uri, {"anon": True}):
        results = yolo(Image.open(BytesIO(contents)))
```
