from typing import AsyncIterator, Iterator, Optional
from .client import Client

from .file import File, FilePointer
from .asyn import sync_iter_async, get_loop


async def iter_files_async(
    uri: str, glob: Optional[str] = None, client_config: dict = {}
) -> AsyncIterator[File]:
    with Client.get_client(uri, **client_config) as client:
        _, path = client.parse_url(uri)
        async for file in client.iter_files(path.rstrip("/"), glob):
            yield file


def iter_files(
    uri: str, glob: Optional[str] = None, client_config: dict = {}
) -> Iterator[File]:
    async_iter = iter_files_async(uri, glob, client_config)
    for file in sync_iter_async(async_iter, get_loop()):
        yield file


__all__ = ["iter_files", "iter_files_async", "File", "FilePointer"]
