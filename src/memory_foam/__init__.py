from typing import Iterator, Optional
from .client import Client

from .file import File, FilePointer
from .asyn import iter_over_async, get_loop


def iter_files(
    uri: str, glob: Optional[str] = None, client_config: dict = {}
) -> Iterator[File]:
    client = Client.get_client(uri, **client_config)
    _, path = client.parse_url(uri)
    for file in iter_over_async(client.iter_files(path.rstrip("/"), glob), get_loop()):
        yield file
    client.close()


__all__ = ["iter_files", "File", "FilePointer"]
