from datetime import datetime
from typing import AsyncIterator, Iterator, Optional
from .client import Client

from .file import File, FilePointer
from .asyn import sync_iter_async, get_loop


async def iter_files_async(
    uri: str,
    glob: Optional[str] = None,
    modified_after: Optional[datetime] = None,
    max_concurrent_reads: int = 32,
    max_queued_results: int = 200,
    max_prefetch_pages: int = 2,
    client_config: dict = {},
    loop=get_loop(),
) -> AsyncIterator[File]:
    """
    Asynchronously iterate over files in a given URI.

    Args:
        uri (str): The URI of the storage location.
        glob (Optional[str]): A glob pattern to filter files. Defaults to None.
        modified_after (Optional[datetime]): A datetime to filter to files modified after. Defaults to None.
        max_concurrent_reads (Optional[int]): The max number of files that can be read concurrently.
            Defaults to 32. Set to -1 for unlimited concurrent reads.
        max_queued_results (Optional[int]): The number of Files in the results queue at any time.
            Defaults to 200.
        max_prefetch_pages (Optional[int]): The max number of pages (from the fs paginated list output) queued
            for reading any time. Defaults to 2.
        client_config (dict): Configuration options for the client. Defaults to an empty dictionary.
        loop: The event loop to use. Defaults to the default fsspec IO loop.

    Yields:
        File: A tuple containing a FilePointer to each file along with the file's contents.
    """
    with Client.get_client(uri, loop, max_concurrent_reads, **client_config) as client:
        _, path = client.parse_url(uri)
        async for file in client.iter_files(
            path.rstrip("/"),
            glob=glob,
            modified_after=modified_after,
            max_queued_results=max_queued_results,
            max_prefetch_pages=max_prefetch_pages,
        ):
            yield file


async def iter_pointers_async(
    bucket: str,
    pointers: list[FilePointer],
    max_concurrent_reads: int = 32,
    max_queued_results: int = 200,
    batch_size: int = 5000,
    client_config: dict = {},
    loop=get_loop(),
) -> AsyncIterator[File]:
    """
    Asynchronously iterate over files using a list of file pointers.

    Args:
        bucket (str): The bucket or container name.
        pointers (list[FilePointer]): A list of file pointers to iterate over.
        max_concurrent_reads (Optional[int]): The max number of files that can be read concurrently.
            Defaults to 32. Set to -1 for unlimited concurrent reads.
        max_queued_results (Optional[int]): The max number of Files in the results queue at any time.
            Defaults to 200.
        batch_size: (Optional[int]): The number of FilePointers per batch. Defaults to 5000.
        client_config (dict): Configuration options for the client. Defaults to an empty dictionary.
        loop: The event loop to use. Defaults to the default fsspec IO loop.

    Yields:
        File: A tuple containing a FilePointer to each file along with the file's contents.
    """
    with Client.get_client(
        bucket, loop, max_concurrent_reads, **client_config
    ) as client:
        async for file in client.iter_pointers(
            pointers, max_queued_results=max_queued_results, batch_size=batch_size
        ):
            yield file


def iter_files(
    uri: str,
    glob: Optional[str] = None,
    modified_after: Optional[datetime] = None,
    max_concurrent_reads: int = 32,
    max_queued_results: int = 200,
    max_prefetch_pages: int = 2,
    client_config: dict = {},
) -> Iterator[File]:
    """
    Synchronously iterate over files in a given URI.

    Args:
        uri (str): The URI of the storage location.
        glob (Optional[str]): A glob pattern to filter files. Defaults to None.
        modified_after (Optional[datetime]): A datetime to filter to files modified after. Defaults to None.
        max_concurrent_reads (Optional[int]): The max number of files that can be read concurrently.
            Defaults to 32. Set to -1 for unlimited concurrent reads.
        max_queued_results (Optional[int]): The max number of Files in the results queue at any time.
            Defaults to 200.
        max_prefetch_pages (Optional[int]): The max number of pages (from the fs paginated list output) queued
            for reading any time. Defaults to 2.
        client_config (dict): Configuration options for the client. Defaults to an empty dictionary.

    Yields:
        File: A tuple containing a FilePointer to each file along with the file's contents.
    """
    loop = get_loop()
    async_iter = iter_files_async(
        uri,
        glob=glob,
        modified_after=modified_after,
        max_concurrent_reads=max_concurrent_reads,
        max_queued_results=max_queued_results,
        max_prefetch_pages=max_prefetch_pages,
        client_config=client_config,
        loop=loop,
    )
    for file in sync_iter_async(async_iter, loop):
        yield file


def iter_pointers(
    bucket: str,
    pointers: list[FilePointer],
    max_concurrent_reads: int = 32,
    max_queued_results: int = 200,
    batch_size: int = 5000,
    client_config: dict = {},
) -> Iterator[File]:
    """
    Synchronously iterate over files using a list of file pointers.

    Args:
        bucket (str): The bucket or container name.
        pointers (list[FilePointer]): A list of file pointers to iterate over.
        max_concurrent_reads (Optional[int]): The max number of files that can be read concurrently.
            Defaults to 32. Set to -1 for unlimited concurrent reads.
        max_queued_results (Optional[int]): The max number of Files in the results queue at any time.
            Defaults to 200.
        batch_size: (Optional[int]): The number of FilePointers per batch. Defaults to 5000.
        client_config (dict): Configuration options for the client. Defaults to an empty dictionary.

    Yields:
        File: A tuple containing a FilePointer to each file along with the file's contents.
    """
    loop = get_loop()
    async_iter = iter_pointers_async(
        bucket,
        pointers=pointers,
        max_concurrent_reads=max_concurrent_reads,
        max_queued_results=max_queued_results,
        batch_size=batch_size,
        client_config=client_config,
        loop=loop,
    )
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
