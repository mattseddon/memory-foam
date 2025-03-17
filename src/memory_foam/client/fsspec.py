from abc import ABC, abstractmethod
import asyncio
import multiprocessing
import os
from typing import Any, AsyncIterator, ClassVar, Iterable, Optional
from fsspec.spec import AbstractFileSystem
from urllib.parse import urlparse

from ..file import File
from ..asyn import get_loop

DELIMITER = "/"  # Path delimiter.
FETCH_WORKERS = 100


ResultQueue = asyncio.Queue[Optional[File]]
PageQueue = asyncio.Queue[Optional[Iterable[dict[str, Any]]]]


class ClientError(RuntimeError):
    def __init__(self, message, error_code=None):
        super().__init__(message)
        # error code from the cloud itself
        self.error_code = error_code


class Client(ABC):
    MAX_THREADS = multiprocessing.cpu_count()
    FS_CLASS: ClassVar[type["AbstractFileSystem"]]
    PREFIX: ClassVar[str]
    protocol: ClassVar[str]

    def __init__(self, name: str, fs_kwargs: dict[str, Any]) -> None:
        self.name = name
        self.fs_kwargs = fs_kwargs
        self._fs: Optional[AbstractFileSystem] = None
        self.uri = self.get_uri(self.name)

    @classmethod
    def create_fs(cls, **kwargs) -> "AbstractFileSystem":
        kwargs.setdefault("version_aware", True)
        fs = cls.FS_CLASS(**kwargs)
        fs.invalidate_cache()
        return fs

    @abstractmethod
    def close(self) -> None: ...

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
        from .gcs import GCSClient

        protocol = urlparse(url).scheme

        if not protocol:
            raise NotImplementedError(
                "Unsupported protocol: urlparse was not able to identify a scheme"
            )

        protocol = protocol.lower()
        if protocol == ClientS3.protocol:
            return ClientS3
        if protocol == GCSClient.protocol:
            return GCSClient
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

    def rel_path(self, path: str) -> str:
        return self.fs.split_path(path)[1]

    def get_full_path(self, rel_path: str, version_id: Optional[str] = None) -> str:
        return self.version_path(f"{self.PREFIX}{self.name}/{rel_path}", version_id)

    def version_path(cls, path: str, version_id: Optional[str]) -> str:
        return path

    async def iter_files(
        self, start_prefix: str, glob: Optional[str] = None
    ) -> AsyncIterator[File]:
        result_queue: ResultQueue = asyncio.Queue(200)
        loop = get_loop()
        main_task = loop.create_task(self._fetch(start_prefix, glob, result_queue))

        while (file := await result_queue.get()) is not None:
            yield file

        await main_task

    @abstractmethod
    async def _fetch(
        self, start_prefix: str, glob: Optional[str], result_queue: ResultQueue
    ) -> None: ...

    @staticmethod
    def _is_valid_key(key: str) -> bool:
        """
        Check if the key looks like a valid path.

        Invalid keys are ignored when indexing.
        """
        return not (key.startswith("/") or key.endswith("/") or "//" in key)
