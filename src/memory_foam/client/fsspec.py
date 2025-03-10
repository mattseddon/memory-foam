from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass
from datetime import datetime
import multiprocessing
import os
from typing import Any, AsyncIterator, ClassVar, Optional, Sequence
from fsspec.asyn import get_loop
from fsspec.spec import AbstractFileSystem
from urllib.parse import urlparse

DELIMITER = "/"  # Path delimiter.
FETCH_WORKERS = 100


@dataclass
class File:
    """
    `DataModel` for reading binary files.

    Attributes:
        source (str): The source of the file (e.g., 's3://bucket-name/').
        contents (bytes): The contents of the file,
        path (str): The path to the file (e.g., 'path/to/file.txt').
        size (int): The size of the file in bytes. Defaults to 0.
        version (str): The version of the file. Defaults to an empty string.
        last_modified (datetime): The last modified timestamp of the file.
            Defaults to Unix epoch (`1970-01-01T00:00:00`).
    """

    contents: bytes
    source: str
    path: str
    size: int
    version: str
    last_modified: datetime


ResultQueue = asyncio.Queue[Optional[Sequence[File]]]


class ClientError(RuntimeError):
    def __init__(self, message, error_code=None):
        super().__init__(message)
        # error code from the cloud itself
        self.error_code = error_code


class Client(ABC):
    MAX_THREADS = multiprocessing.cpu_count()
    FS_CLASS: ClassVar[AbstractFileSystem]
    PREFIX: ClassVar[str]
    protocol: ClassVar[str]

    def __init__(self, name: str, fs_kwargs: dict[str, Any]) -> None:
        self.name = name
        self.fs_kwargs = fs_kwargs
        self._fs: Optional[AbstractFileSystem] = None
        self.uri = self.get_uri(self.name)

    @abstractmethod
    async def info_to_file(self, v: dict[str, Any], path: str) -> "File": ...

    @classmethod
    def create_fs(cls, **kwargs) -> "AbstractFileSystem":
        kwargs.setdefault("version_aware", True)
        fs = cls.FS_CLASS(**kwargs)
        fs.invalidate_cache()
        return fs

    @property
    def fs(self) -> AbstractFileSystem:
        if not self._fs:
            self._fs = self.create_fs(**self.fs_kwargs)
        return self._fs

    @staticmethod
    def get_implementation(url: str) -> type["Client"]:
        # from .azure import AzureClient
        # from .gcs import GCSClient
        from .s3 import ClientS3

        protocol = urlparse(url).scheme

        if not protocol:
            raise NotImplementedError(
                "Unsupported protocol: urlparse was not able to identify a scheme"
            )

        protocol = protocol.lower()
        if protocol == ClientS3.protocol:
            return ClientS3
        # if protocol == GCSClient.protocol:
        #     return GCSClient
        # if protocol == AzureClient.protocol:
        #     return AzureClient

        raise NotImplementedError(f"Unsupported protocol: {protocol}")

    @staticmethod
    def get_client(source: str, **kwargs) -> "Client":
        cls = Client.get_implementation(source)
        storage_url, _ = cls.split_url(source)
        if os.name == "nt":
            storage_url = storage_url.removeprefix("/")

        return cls.from_name(storage_url, kwargs)

    @classmethod
    def from_name(
        cls,
        name: str,
        kwargs: dict[str, Any],
    ) -> "Client":
        return cls(name, kwargs)

    def parse_url(self, source: str) -> tuple[str, str]:
        storage_name, rel_path = self.split_url(source)
        return self.get_uri(storage_name), rel_path

    def get_uri(self, name: str) -> str:
        return f"{self.PREFIX}{name}"

    @classmethod
    def split_url(self, url: str) -> tuple[str, str]:
        fill_path = url[len(self.PREFIX) :]
        path_split = fill_path.split("/", 1)
        bucket = path_split[0]
        path = path_split[1] if len(path_split) > 1 else ""
        return bucket, path

    def get_full_path(self, rel_path: str, version_id: Optional[str] = None) -> str:
        return self.version_path(f"{self.PREFIX}{self.name}/{rel_path}", version_id)

    def version_path(cls, path: str, version_id: Optional[str]) -> str:
        return path

    async def iter_file_contents(
        self, start_prefix: str
    ) -> AsyncIterator[Sequence["File"]]:
        result_queue: ResultQueue = asyncio.Queue()
        loop = get_loop()
        main_task = loop.create_task(self._fetch(start_prefix, result_queue))
        while (entry := await result_queue.get()) is not None:
            yield entry
        await main_task

    @abstractmethod
    async def _fetch(self, start_prefix: str, result_queue: ResultQueue) -> None: ...

    @staticmethod
    def _is_valid_key(key: str) -> bool:
        """
        Check if the key looks like a valid path.

        Invalid keys are ignored when indexing.
        """
        return not (key.startswith("/") or key.endswith("/") or "//" in key)
