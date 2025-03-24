import json
import os
from datetime import datetime
from typing import Any, Optional, cast
from dateutil.parser import isoparse
from gcsfs import GCSFileSystem
from gcsfs.retry import retry_request

from .fsspec import Client, PageQueue
from ..file import FilePointer

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

    @property
    def _path_key(self):
        return "name"

    def _get_last_modified(self, d: dict):
        return self.parse_timestamp(d["updated"])

    async def _get_pages(self, prefix: str, page_queue: PageQueue) -> None:
        page_size = 5000
        try:
            next_page_token = None
            while True:
                page = await self.fs._call(
                    "GET",
                    "b/{}/o",
                    self.name,
                    delimiter="",
                    prefix=prefix,
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
            source=self._uri,
            path=self._rel_path(info["name"]),
            size=info.get("size", ""),
            version=info.get("generation", ""),
            last_modified=info.get("mtime", ""),
        )

    @retry_request(retries=6)
    async def _read(self, path: str, version: Optional[str] = None) -> bytes:
        url = self.fs.url(self._get_full_path(path, version))
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
    def _version_path(cls, path: str, version_id: Optional[str]) -> str:
        return f"{path}#{version_id}" if version_id else path
