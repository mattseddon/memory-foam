import asyncio
import os
import errno
from typing import Any, AsyncIterable, Optional
from urllib.parse import parse_qs, urlsplit, urlunsplit

from adlfs import AzureBlobFileSystem
from azure.core.exceptions import (
    ResourceNotFoundError,
)


from ..asyn import queue_task_result
from ..glob import get_glob_match, is_match
from ..file import FilePointer

from .fsspec import DELIMITER, Client, ResultQueue

PageQueue = asyncio.Queue[Optional[AsyncIterable[dict[str, Any]]]]


class AzureClient(Client):
    FS_CLASS = AzureBlobFileSystem
    PREFIX = "az://"
    protocol = "az"

    async def _fetch_prefix(
        self, start_prefix: str, glob: Optional[str], result_queue: ResultQueue
    ) -> None:
        try:

            async def get_pages(page_queue: PageQueue):
                prefix = start_prefix
                if prefix:
                    prefix = prefix.lstrip(DELIMITER) + DELIMITER

                try:
                    async with self.fs.service_client.get_container_client(
                        container=self.name
                    ) as container_client:
                        async for page in container_client.list_blobs(
                            include=["metadata", "versions"], name_starts_with=prefix
                        ).by_page():
                            await page_queue.put(page)
                finally:
                    await page_queue.put(None)

            async def process_pages(
                page_queue: PageQueue, glob: Optional[str], result_queue: ResultQueue
            ):
                found = False

                glob_match = get_glob_match(glob)

                while (page := await page_queue.get()) is not None:
                    if page:
                        found = True

                    tasks = []
                    async for b in page:
                        if not (
                            self._is_valid_key(b["name"])
                            and is_match(b["name"], glob_match)
                        ):
                            continue
                        info = (await self.fs._details([b]))[0]
                        pointer = self._info_to_file_pointer(info)
                        task = queue_task_result(
                            self._read_file(pointer), result_queue, self.loop
                        )
                        tasks.append(task)
                    await asyncio.gather(*tasks)

                if not found:
                    raise FileNotFoundError(
                        f"Unable to resolve remote path: {start_prefix}"
                    )

            page_queue: PageQueue = asyncio.Queue(2)
            page_consumer = self.loop.create_task(
                process_pages(page_queue, glob, result_queue)
            )
            try:
                await asyncio.gather(get_pages(page_queue), page_consumer)
            finally:
                page_consumer.cancel()

        finally:
            await result_queue.put(None)

    def _info_to_file_pointer(self, v: dict[str, Any]) -> FilePointer:
        return FilePointer(
            source=self.uri,
            path=self.rel_path(v["name"]),
            version=v.get("version_id", ""),
            last_modified=v["last_modified"],
            size=v.get("size", ""),
        )

    def close(self):
        pass

    async def _read(self, path: str, version: Optional[str] = None) -> bytes:
        full_path = self.get_full_path(path, version)
        delimiter = "/"
        source, path, version = self.fs.split_path(full_path, delimiter=delimiter)

        try:
            async with self.fs.service_client.get_blob_client(
                source, path.rstrip(delimiter)
            ) as bc:
                stream = await bc.download_blob(
                    version_id=version,
                    max_concurrency=1,
                    **self.fs._timeout_kwargs,
                )
                return await stream.readall()
        except ResourceNotFoundError as exception:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path
            ) from exception

    @classmethod
    def version_path(cls, path: str, version_id: Optional[str]) -> str:
        parts = list(urlsplit(path))
        query = parse_qs(parts[3])
        if "versionid" in query:
            raise ValueError("path already includes a version query")
        parts[3] = f"versionid={version_id}" if version_id else ""
        return urlunsplit(parts)
