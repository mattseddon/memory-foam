from typing import Iterator, Optional
from .client import Client

from .client.fsspec import File
from .asyn import iter_over_async, get_loop


def get_entries(uri: str, client_config: Optional[dict]) -> Iterator[File]:
    config = client_config or {}
    client = Client.get_client(uri, **config)
    _, path = client.parse_url(uri)
    for file in iter_over_async(
        client.iter_file_contents(path.rstrip("/")), get_loop()
    ):
        yield file
    client.close()


__all__ = ["get_entries"]
