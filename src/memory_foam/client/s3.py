from datetime import datetime
from typing import Any, Optional, cast
from s3fs import S3FileSystem
from botocore.exceptions import NoCredentialsError

from ..file import FilePointer
from .fsspec import Client, PageQueue, ResultQueue


class ClientS3(Client):
    FS_CLASS = S3FileSystem
    PREFIX = "s3://"
    protocol = "s3"

    @classmethod
    def create_fs(cls, **kwargs) -> S3FileSystem:
        if "aws_endpoint_url" in kwargs:
            kwargs.setdefault("client_kwargs", {}).setdefault(
                "endpoint_url", kwargs.pop("aws_endpoint_url")
            )
        if "aws_key" in kwargs:
            kwargs.setdefault("key", kwargs.pop("aws_key"))
        if "aws_secret" in kwargs:
            kwargs.setdefault("secret", kwargs.pop("aws_secret"))
        if "aws_token" in kwargs:
            kwargs.setdefault("token", kwargs.pop("aws_token"))

        # We want to use newer v4 signature version since regions added after
        # 2014 are not going to support v2 which is the older one.
        # All regions support v4.
        kwargs.setdefault("config_kwargs", {}).setdefault("signature_version", "s3v4")

        if "region_name" in kwargs:
            kwargs["config_kwargs"].setdefault("region_name", kwargs.pop("region_name"))
        if not kwargs.get("anon"):
            try:
                # Run an inexpensive check to see if credentials are available
                non_async_kwargs = {
                    k: v for k, v in kwargs.items() if k not in ["asynchronous", "loop"]
                }
                super().create_fs(**non_async_kwargs).sign("s3://bucket/object")
            except NoCredentialsError:
                kwargs["anon"] = True
            except NotImplementedError:
                pass

        return cast(S3FileSystem, super().create_fs(**kwargs))

    def close(self):
        self.fs.close_session(self._loop, self.s3)

    @property
    def _path_key(self):
        return "Key"

    def _get_last_modified(self, d: dict) -> datetime:
        return d.get("LastModified", "")

    async def _get_pages(self, prefix, page_queue: PageQueue):
        try:
            await self._setup_fs()

            method = "list_object_versions"
            contents_key = "Versions"
            pag = self.s3.get_paginator(method)
            it = pag.paginate(
                Bucket=self.name,
                Prefix=prefix,
                Delimiter="",
            )

            async for page in it:
                await page_queue.put(page.get(contents_key, []))
        finally:
            await page_queue.put(None)

    async def _setup_fs(self):
        fs = self.fs
        await fs.set_session()
        self.s3 = await fs.get_s3(self.name)

    async def _fetch_list(
        self, pointers: list[FilePointer], batch_size: int, result_queue: ResultQueue
    ):
        await self._setup_fs()
        return await super()._fetch_list(
            pointers, batch_size=batch_size, result_queue=result_queue
        )

    async def _read(self, path: str, version: Optional[str] = None) -> bytes:
        stream = await self.fs.open_async(self._get_full_path(path, version))
        return await stream.read()

    def _info_to_file_pointer(
        self,
        d: dict[str, Any],
    ) -> FilePointer:
        version = self._clean_s3_version(d.get("VersionId", ""))
        return FilePointer(
            source=self._uri,
            path=d["Key"],
            size=d["Size"],
            version=version,
            last_modified=d.get("LastModified", ""),
        )

    def _clean_s3_version(self, ver: Optional[str]) -> str:
        if ver is None or ver == "null":
            return ""
        return ver
