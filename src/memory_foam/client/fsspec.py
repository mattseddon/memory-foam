from abc import ABC, abstractmethod
import asyncio
import multiprocessing
import os
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    ClassVar,
    Iterable,
    Optional,
    Union,
)
from fsspec.spec import AbstractFileSystem
from urllib.parse import urlparse

from ..asyn import queue_task_result
from ..file import File, FilePointer
from ..glob import get_glob_match, is_match


DELIMITER = "/"  # Path delimiter.
FETCH_WORKERS = 100


ResultQueue = asyncio.Queue[Optional[File]]
PageQueue = asyncio.Queue[
    Optional[Union[Iterable[dict[str, Any]], AsyncIterable[dict[str, Any]]]]
]


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
    loop: asyncio.AbstractEventLoop
    max_concurrent_reads = asyncio.Semaphore(32)

    def __init__(
        self, name: str, loop: asyncio.AbstractEventLoop, fs_kwargs: dict[str, Any]
    ) -> None:
        self.name = name
        self.fs_kwargs = fs_kwargs
        self._fs: Optional[AbstractFileSystem] = None
        self.uri = self._get_uri(self.name)
        self.loop = loop

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    async def _get_pages(self, prefix: str, page_queue: PageQueue) -> None: ...

    @abstractmethod
    async def _read(self, path: str, version: Optional[str] = None) -> bytes: ...

    @abstractmethod
    def _info_to_file_pointer(self, v: dict[str, Any]) -> FilePointer: ...

    @property
    @abstractmethod
    def _path_key(self) -> str: ...

    @classmethod
    def create_fs(cls, **kwargs) -> "AbstractFileSystem":
        kwargs.setdefault("version_aware", True)
        fs = cls.FS_CLASS(**kwargs)
        fs.invalidate_cache()
        return fs

    @property
    def fs(self) -> AbstractFileSystem:
        if not self._fs:
            self._fs = self.create_fs(
                **self.fs_kwargs, asynchronous=True, loop=self.loop
            )
        return self._fs

    @staticmethod
    def get_implementation(url: str) -> type["Client"]:
        from .azure import AzureClient
        from .gcs import GCSClient
        from .s3 import ClientS3

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
        if protocol == AzureClient.protocol:
            return AzureClient

        raise NotImplementedError(f"Unsupported protocol: {protocol}")

    @staticmethod
    def get_client(source: str, loop: asyncio.AbstractEventLoop, **kwargs) -> "Client":
        cls = Client.get_implementation(source)
        storage_url, _ = cls.split_url(source)
        if os.name == "nt":
            storage_url = storage_url.removeprefix("/")

        return cls.from_name(storage_url, loop, kwargs)

    @classmethod
    def from_name(
        cls,
        name: str,
        loop: asyncio.AbstractEventLoop,
        kwargs: dict[str, Any],
    ) -> "Client":
        return cls(name, loop, kwargs)

    def parse_url(self, source: str) -> tuple[str, str]:
        storage_name, rel_path = self.split_url(source)
        return self._get_uri(storage_name), rel_path

    def _get_uri(self, name: str) -> str:
        return f"{self.PREFIX}{name}"

    @classmethod
    def split_url(self, url: str) -> tuple[str, str]:
        fill_path = url[len(self.PREFIX) :]
        path_split = fill_path.split("/", 1)
        bucket = path_split[0]
        path = path_split[1] if len(path_split) > 1 else ""
        return bucket, path

    def _rel_path(self, path: str) -> str:
        return self.fs.split_path(path)[1]

    def _get_full_path(self, rel_path: str, version_id: Optional[str] = None) -> str:
        return self._version_path(f"{self.PREFIX}{self.name}/{rel_path}", version_id)

    def _version_path(cls, path: str, version_id: Optional[str]) -> str:
        return path

    async def iter_files(
        self, start_prefix: str, glob: Optional[str] = None
    ) -> AsyncIterator[File]:
        result_queue: ResultQueue = asyncio.Queue(200)
        main_task = self.loop.create_task(
            self._fetch_prefix(start_prefix, glob, result_queue)
        )

        while (file := await result_queue.get()) is not None:
            yield file

        await main_task

    async def _fetch_prefix(
        self, start_prefix: str, glob: Optional[str], result_queue: ResultQueue
    ) -> None:
        try:
            prefix = start_prefix
            if prefix:
                prefix = prefix.lstrip(DELIMITER) + DELIMITER
            page_queue: PageQueue = asyncio.Queue(2)
            page_consumer = self.loop.create_task(
                self._process_pages(prefix, page_queue, glob, result_queue)
            )
            try:
                await asyncio.gather(self._get_pages(prefix, page_queue), page_consumer)
            finally:
                page_consumer.cancel()
        finally:
            await result_queue.put(None)

    # make abstract and move to syncPageProcessor class
    async def _process_pages(
        self,
        prefix: str,
        page_queue: PageQueue,
        glob: Optional[str],
        result_queue: ResultQueue,
    ):
        glob_match = get_glob_match(glob)

        try:
            found = False

            while (page := await page_queue.get()) is not None:
                if page:
                    found = True

                if not hasattr(page, "__aiter__"):
                    tasks = self._process_page(page, glob_match, result_queue)
                else:
                    assert hasattr(self, "_process_page_async")
                    tasks = await self._process_page_async(
                        page, glob_match, result_queue
                    )

                await asyncio.gather(*tasks)

            if not found:
                raise FileNotFoundError(f"Unable to resolve remote path: {prefix}")
        finally:
            await result_queue.put(None)

    def _process_page(
        self, page: Iterable, glob_match: Optional[Callable], result_queue: ResultQueue
    ):
        tasks = []
        for d in page:
            if not self._should_read(d, glob_match):
                continue
            pointer = self._info_to_file_pointer(d)
            task = queue_task_result(self._read_file(pointer), result_queue, self.loop)
            tasks.append(task)
        return tasks

    async def iter_pointers(self, pointers: list[FilePointer]) -> AsyncIterator[File]:
        result_queue: ResultQueue = asyncio.Queue(200)
        main_task = self.loop.create_task(self._fetch_list(pointers, result_queue))

        while (file := await result_queue.get()) is not None:
            yield file

        await main_task

    async def _fetch_list(
        self, pointers: list[FilePointer], result_queue: ResultQueue
    ) -> None:
        tasks = []
        for i, pointer in enumerate(pointers):
            task = queue_task_result(self._read_file(pointer), result_queue, self.loop)
            tasks.append(task)
            if i % 5000 == 0:
                await asyncio.gather(*tasks)
                tasks = []

        await asyncio.gather(*tasks)
        await result_queue.put(None)

    def _should_read(self, d: dict, glob_match: Optional[Callable]) -> bool:
        return self._is_valid_key(d[self._path_key]) and is_match(
            d[self._path_key], glob_match
        )

    @staticmethod
    def _is_valid_key(key: str) -> bool:
        """
        Check if the key looks like a valid path.

        Invalid keys are ignored when indexing.
        """
        return not (key.startswith("/") or key.endswith("/") or "//" in key)

    async def _read_file(self, pointer: FilePointer) -> File:
        async with self.max_concurrent_reads:
            contents = await self._read(pointer.path, pointer.version)
            return (pointer, contents)
