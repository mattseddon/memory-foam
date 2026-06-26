import errno
import os
from asyncio import Queue
from collections.abc import AsyncIterable, Callable
from datetime import datetime
from typing import Any, cast

from adlfs import AzureBlobFileSystem
from azure.core.exceptions import (
    ResourceNotFoundError,
)

from ..asyn import queue_task_result
from ..file import FilePointer
from .fsspec import Client, ResultQueue

PageQueue = Queue[AsyncIterable[dict[str, Any]] | None]


class AzureClient(Client):
    FS_CLASS = AzureBlobFileSystem
    PREFIX = "az://"
    protocol = "az"

    @property
    def fs(self) -> AzureBlobFileSystem:
        if not self._fs:
            self._fs = self._create_fs(
                **self._fs_kwargs, asynchronous=True, loop=self._loop
            )
        return cast(AzureBlobFileSystem, self._fs)

    def close(self):
        pass

    async def _get_pages(self, prefix: str, page_queue) -> None:
        try:
            async with self.fs.service_client.get_container_client(
                container=self.name
            ) as container_client:
                async for page in container_client.list_blobs(
                    include=["metadata", "versions"], name_starts_with=prefix
                ).by_page():
                    await page_queue.put(page)  # pyright: ignore[reportArgumentType]
        finally:
            await page_queue.put(None)

    async def _read(self, path: str, version: str | None = None) -> bytes:
        full_path = self._get_full_path(path, version)
        delimiter = "/"
        source, path, version = self.fs.split_path(full_path, delimiter=delimiter)

        try:
            async with self.fs.service_client.get_blob_client(
                source, path.rstrip(delimiter)
            ) as bc:
                stream = await bc.download_blob(
                    version_id=version,
                    max_concurrency=self.fs.max_concurrency,
                    **self.fs._timeout_kwargs,
                )
                return await stream.readall()
        except ResourceNotFoundError as exception:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path
            ) from exception

    def _info_to_file_pointer(self, d: dict[str, Any]) -> FilePointer:
        return FilePointer(
            source=self._uri,
            path=self._rel_path(d["name"]),
            version=d.get("version_id", ""),
            last_modified=d["last_modified"],
            size=d.get("size", ""),
        )

    @property
    def _path_key(self) -> str:
        return "name"

    def _get_last_modified(self, d: dict) -> datetime:
        return d["last_modified"]

    async def _process_page_async(
        self,
        page: AsyncIterable,
        glob_match: Callable | None,
        modified_after: datetime | None,
        result_queue: ResultQueue,
    ):
        tasks = []
        async for b in page:
            if not self._should_read(b, glob_match, modified_after):
                continue
            info = (await self.fs._details([b]))[0]
            pointer = self._info_to_file_pointer(info)
            task = queue_task_result(
                self._concurrent_read_file(pointer), result_queue, self._loop
            )
            tasks.append(task)
        return tasks
