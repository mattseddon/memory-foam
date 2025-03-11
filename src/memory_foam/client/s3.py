import asyncio
from typing import Any, Optional, cast
from s3fs import S3FileSystem

from .fsspec import DELIMITER, Client, File, ResultQueue
from fsspec.asyn import get_loop

from botocore.exceptions import NoCredentialsError


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
                super().create_fs(**kwargs).sign("s3://bucket/object")
            except NoCredentialsError:
                kwargs["anon"] = True
            except NotImplementedError:
                pass

        return cast(S3FileSystem, super().create_fs(**kwargs, asynchronous=True))

    def close(self):
        self.fs.close_session(get_loop(), self.s3)

    async def _fetch(self, start_prefix: str, result_queue: ResultQueue) -> None:
        async def get_pages(it, page_queue):
            try:
                async for page in it:
                    await page_queue.put(page.get(contents_key, []))
            finally:
                await page_queue.put(None)

        async def process_file(file: File) -> File:
            contents = await self.read(file.path, file.version)
            file.set_contents(contents)
            return file

        async def process_pages(page_queue, result_queue):
            try:
                found = False

                while (res := await page_queue.get()) is not None:
                    if res:
                        found = True
                    for d in res:
                        if not self._is_valid_key(d["Key"]):
                            continue
                        file = self.info_to_file(d, d["Key"])
                        task = asyncio.ensure_future(process_file(file))
                        await result_queue.put(task)

                if not found:
                    raise FileNotFoundError(f"Unable to resolve remote path: {prefix}")
            finally:
                await result_queue.put(None)

        async def process_files(file_queue, result_queue):
            found = False

            while (file := await file_queue.get()) is not None:
                if file:
                    found = True
                task = asyncio.ensure_future(process_file(file))
                await result_queue.put(task)

            if not found:
                raise FileNotFoundError(f"Unable to resolve remote path: {prefix}")

        try:
            prefix = start_prefix
            if prefix:
                prefix = prefix.lstrip(DELIMITER) + DELIMITER
            versions = True
            fs = self.fs
            await fs.set_session()
            self.s3 = await fs.get_s3(self.name)
            if versions:
                method = "list_object_versions"
                contents_key = "Versions"
            else:
                method = "list_objects_v2"
                contents_key = "Contents"
            pag = self.s3.get_paginator(method)
            it = pag.paginate(
                Bucket=self.name,
                Prefix=prefix,
                Delimiter="",
            )
            page_queue: asyncio.Queue[list] = asyncio.Queue(2)
            page_consumer = asyncio.create_task(process_pages(page_queue, result_queue))
            try:
                await get_pages(it, page_queue)
                await page_consumer
            finally:
                page_consumer.cancel()  # In case get_pages() raised
        finally:
            result_queue.put_nowait(None)

    def _clean_s3_version(self, ver: Optional[str]) -> str:
        if ver is None or ver == "null":
            return ""
        return ver

    async def read(self, path, version) -> bytes:
        stream = await self.fs.open_async(self.get_full_path(path, version))
        return await stream.read()

    def info_to_file(self, v: dict[str, Any], path: str) -> File:
        version = self._clean_s3_version(v.get("VersionId", ""))
        return File(
            source=self.uri,
            path=v["Key"],
            size=v["Size"],
            version=version,
            last_modified=v.get("LastModified", ""),
        )
