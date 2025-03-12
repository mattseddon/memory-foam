from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass
class FilePointer:
    """

    Attributes:
        source (str): The source of the file (e.g., 's3://bucket-name/').
        path (str): The path to the file (e.g., 'path/to/file.txt').
        size (int): The size of the file in bytes. Defaults to 0.
        version (str): The version of the file. Defaults to an empty string.
        last_modified (datetime): The last modified timestamp of the file.
            Defaults to Unix epoch (`1970-01-01T00:00:00`).
    """

    source: str
    path: str
    size: int
    version: str
    last_modified: datetime

    def to_dict_with(self, d: dict):
        return {**asdict(self), **d}


File = tuple[FilePointer, bytes]
