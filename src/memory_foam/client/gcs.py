import asyncio
import json
import os
from datetime import datetime
from typing import Any, Optional, cast

from dateutil.parser import isoparse
from gcsfs import GCSFileSystem
from gcsfs.retry import retry_request


from ..asyn import queue_task_result, get_loop
from ..file import File, FilePointer
from ..glob import get_glob_match, is_match

from .fsspec import DELIMITER, Client, PageQueue, ResultQueue

# Patch gcsfs for consistency with s3fs
GCSFileSystem.set_session = GCSFileSystem._set_session


class GCSClient(Client):
    FS_CLASS = GCSFileSystem
    PREFIX = "gs://"
    protocol = "gs"

    @classmethod
    def create_fs(cls, **kwargs) -> GCSFileSystem:
        if os.environ.get("MF_GCP_CREDENTIALS"):
            kwargs["token"] = json.loads(os.environ["MF_GCP_CREDENTIALS"])
        if kwargs.pop("anon", False):
            kwargs["token"] = "anon"  # noqa: S105

        return cast(GCSFileSystem, super().create_fs(**kwargs))

    @staticmethod
    def parse_timestamp(timestamp: str) -> datetime:
        """
        Parse timestamp string returned by GCSFileSystem.

        This ensures that the passed timestamp is timezone aware.
        """
        dt = isoparse(timestamp)
        assert dt.tzinfo is not None
        return dt

    def close(self):
        pass

    async def _fetch(
        self, start_prefix: str, glob: Optional[str], result_queue: ResultQueue
    ) -> None:
        loop = get_loop()
        prefix = start_prefix
        if prefix:
            prefix = prefix.lstrip(DELIMITER) + DELIMITER
        found = False
        try:
            page_queue: PageQueue = asyncio.Queue(2)
            page_consumer = loop.create_task(
                self._process_pages(page_queue, glob, result_queue)
            )
            try:
                await self._get_pages(prefix, page_queue)
                found = await page_consumer
                if not found:
                    raise FileNotFoundError(f"Unable to resolve remote path: {prefix}")
            finally:
                page_consumer.cancel()  # In case _get_pages() raised
        finally:
            await result_queue.put(None)

    async def _process_pages(
        self, page_queue: PageQueue, glob: Optional[str], result_queue: ResultQueue
    ) -> bool:
        glob_match = get_glob_match(glob)
        max_concurrent_reads = asyncio.Semaphore(32)

        async def _read_file(pointer: FilePointer) -> File:
            async with max_concurrent_reads:
                contents = await self._read(pointer.path, pointer.version)
                return (pointer, contents)

        found = False
        while (page := await page_queue.get()) is not None:
            if page:
                found = True

                tasks = []
                for d in page:
                    if not (
                        self._is_valid_key(d["name"])
                        and is_match(d["name"], glob_match)
                    ):
                        continue
                    pointer = self._info_to_file_pointer(d)
                    task = queue_task_result(
                        _read_file(pointer), result_queue, get_loop()
                    )
                    tasks.append(task)
                await asyncio.gather(*tasks)

        return found

    async def _get_pages(self, path: str, page_queue: PageQueue) -> None:
        page_size = 5000
        try:
            next_page_token = None
            while True:
                page = await self.fs._call(
                    "GET",
                    "b/{}/o",
                    self.name,
                    delimiter="",
                    prefix=path,
                    maxResults=page_size,
                    pageToken=next_page_token,
                    json_out=True,
                    versions="true",
                )
                assert page["kind"] == "storage#objects"
                await page_queue.put(page.get("items", []))
                next_page_token = page.get("nextPageToken")
                if next_page_token is None:
                    break
        finally:
            await page_queue.put(None)

    def _info_to_file_pointer(self, d: dict[str, Any]) -> FilePointer:
        info = self.fs._process_object(self.name, d)
        return FilePointer(
            source=self.uri,
            path=self.rel_path(info["name"]),
            size=info.get("size", ""),
            version=info.get("generation", ""),
            last_modified=self.parse_timestamp(info["updated"]),
        )

    @retry_request(retries=6)
    async def _read(self, path: str, version: str):
        url = self.fs.url(self.get_full_path(path, version))
        await self.fs._set_session()
        async with self.fs.session.get(
            url=url,
            params=self.fs._get_params({}),
            headers=self.fs._get_headers(None),
            timeout=self.fs.requests_timeout,
        ) as r:
            r.raise_for_status()

            byts = b""

            while True:
                data = await r.content.read(4096 * 32)
                if not data:
                    break
                byts = byts + data

            return byts

    @classmethod
    def version_path(cls, path: str, version_id: Optional[str]) -> str:
        # return f"{path}#{version_id}" if version_id else path
        return path
