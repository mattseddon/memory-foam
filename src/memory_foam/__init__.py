from typing import AsyncIterator, Iterator, Optional
from .client import Client

from .file import File, FilePointer
from .asyn import iter_over_async, get_loop


def iter_files(
    uri: str, glob: Optional[str] = None, client_config: dict = {}
) -> Iterator[File]:
    with Client.get_client(uri, **client_config) as client:
        _, path = client.parse_url(uri)
        for file in iter_over_async(
            client.iter_files(path.rstrip("/"), glob), get_loop()
        ):
            yield file


async def iter_files_async(
    uri: str, glob: Optional[str] = None, client_config: dict = {}
) -> AsyncIterator[File]:
    with Client.get_client(uri, **client_config) as client:
        _, path = client.parse_url(uri)
        async for file in client.iter_files(path.rstrip("/"), glob):
            yield file


__all__ = ["iter_files", "iter_files_async", "File", "FilePointer"]
