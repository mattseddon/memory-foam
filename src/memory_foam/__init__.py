from datetime import datetime
from typing import AsyncIterator, Iterator, Optional
from .client import Client

from .file import File, FilePointer
from .asyn import sync_iter_async, get_loop


async def iter_files_async(
    uri: str,
    glob: Optional[str] = None,
    modified_after: Optional[datetime] = None,
    client_config: dict = {},
    loop=get_loop(),
) -> AsyncIterator[File]:
    """
    Asynchronously iterate over files in a given URI.

    Args:
        uri (str): The URI of the storage location.
        glob (Optional[str]): A glob pattern to filter files. Defaults to None.
        modified_after (Optional[datetime]): A datetime to filter to files modified after. Defaults to None.
        client_config (dict): Configuration options for the client. Defaults to an empty dictionary.
        loop: The event loop to use. Defaults to the default fsspec IO loop.

    Yields:
        File: A tuple containing a FilePointer to each file along with the file's contents.
    """
    with Client.get_client(uri, loop, **client_config) as client:
        _, path = client.parse_url(uri)
        async for file in client.iter_files(path.rstrip("/"), glob, modified_after):
            yield file


async def iter_pointers_async(
    bucket: str, pointers: list[FilePointer], client_config: dict = {}, loop=get_loop()
) -> AsyncIterator[File]:
    """
    Asynchronously iterate over files using a list of file pointers.

    Args:
        bucket (str): The bucket or container name.
        pointers (list[FilePointer]): A list of file pointers to iterate over.
        client_config (dict): Configuration options for the client. Defaults to an empty dictionary.
        loop: The event loop to use. Defaults to the default fsspec IO loop.

    Yields:
        File: A tuple containing a FilePointer to each file along with the file's contents.
    """
    with Client.get_client(bucket, loop, **client_config) as client:
        async for file in client.iter_pointers(pointers):
            yield file


def iter_files(
    uri: str,
    glob: Optional[str] = None,
    modified_after: Optional[datetime] = None,
    client_config: dict = {},
) -> Iterator[File]:
    """
    Synchronously iterate over files in a given URI.

    Args:
        uri (str): The URI of the storage location.
        glob (Optional[str]): A glob pattern to filter files. Defaults to None.
        modified_after (Optional[datetime]): A datetime to filter to files modified after. Defaults to None.
        client_config (dict): Configuration options for the client. Defaults to an empty dictionary.

    Yields:
        File: A tuple containing a FilePointer to each file along with the file's contents.
    """
    loop = get_loop()
    async_iter = iter_files_async(
        uri,
        glob=glob,
        modified_after=modified_after,
        client_config=client_config,
        loop=loop,
    )
    for file in sync_iter_async(async_iter, loop):
        yield file


def iter_pointers(
    bucket: str, pointers: list[FilePointer], client_config: dict = {}
) -> Iterator[File]:
    """
    Synchronously iterate over files using a list of file pointers.

    Args:
        bucket (str): The bucket or container name.
        pointers (list[FilePointer]): A list of file pointers to iterate over.
        client_config (dict): Configuration options for the client. Defaults to an empty dictionary.

    Yields:
        File: A tuple containing a FilePointer to each file along with the file's contents.
    """
    loop = get_loop()
    async_iter = iter_pointers_async(bucket, pointers, client_config, loop)
    for file in sync_iter_async(async_iter, loop):
        yield file


__all__ = [
    "File",
    "FilePointer",
    "iter_files_async",
    "iter_files",
    "iter_pointers_async",
    "iter_pointers",
]
