from datetime import datetime, timezone
import pytest
from memory_foam.client.fsspec import Client, File
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from fsspec.asyn import sync, get_loop

from tests.conftest import DEFAULT_TREE

utc = timezone.utc

ENTRIES = [
    File(
        source="",
        path="description",
        version="7e589b7d-382c-49a5-931f-2b999c930c5e",
        last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
        size=13,
        contents=DEFAULT_TREE.get("description"),
    ),
    File(
        source="",
        path="trees/oak.jpeg",
        version="309eb4a4-bba9-47c1-afcd-d7c51110af6f",
        last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
        size=4,
        contents=DEFAULT_TREE.get("trees", {}).get("oak.jpeg"),
    ),
    File(
        source="",
        path="trees/pine.jpeg",
        version="f9d168d3-6d1b-47ef-8f6a-81fce48de141",
        last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
        size=4,
        contents=DEFAULT_TREE.get("trees", {}).get("pine.jpeg"),
    ),
    File(
        source="",
        path="books/book1.txt",
        version="b9c31cf7-d011-466a-bf16-cf9da0cb422a",
        last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
        size=4,
        contents=DEFAULT_TREE.get("books", {}).get("book1.txt"),
    ),
    File(
        path="books/book2.txt",
        source="",
        version="3a8bb6d9-38db-47a8-8bcb-8972ea95aa20",
        last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
        size=3,
        contents=DEFAULT_TREE.get("books", {}).get("book2.txt"),
    ),
    File(
        source="",
        path="books/book3.txt",
        version="ee49e963-36a8-492a-b03a-e801b93afb40",
        last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
        size=4,
        contents=DEFAULT_TREE.get("books", {}).get("book3.txt"),
    ),
    File(
        source="",
        path="books/others/book4.txt",
        version="c5969421-6900-4060-bc39-d54f4a49b9fc",
        last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
        size=4,
        contents=DEFAULT_TREE.get("books", {}).get("others", {}).get("book4.txt"),
    ),
]


@pytest.fixture
def client(cloud_server, cloud_server_credentials):
    return Client.get_client(cloud_server.src_uri, **cloud_server.client_config)


_non_null_text = st.text(
    alphabet=st.characters(blacklist_categories=["Cc", "Cs"]), min_size=1
)


def normalize_entries(entries):
    return {(e.path, e.contents) for e in entries}


def match_entries(result, expected):
    assert len(result) == len(expected)
    assert normalize_entries(result) == normalize_entries(expected)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None)
@given(rel_path=_non_null_text)
def test_parse_url(cloud_server, rel_path):
    bucket_uri = cloud_server.src_uri
    url = f"{bucket_uri}/{rel_path}"
    client = Client.get_client(url)
    uri, rel_part = client.parse_url(url)
    assert uri == bucket_uri
    assert rel_part == rel_path


def iter_file_contents(client, prefix):
    async def find(client, prefix):
        results = []
        async for entry in client.iter_file_contents(prefix):
            results.append(entry)
        return results

    return sync(get_loop(), find, client, prefix)


def test_iter_file_contents_success(client):
    results = iter_file_contents(client, "")
    match_entries(results, ENTRIES)
