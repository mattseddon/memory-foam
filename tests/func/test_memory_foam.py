from datetime import datetime, timezone
import pytest
from memory_foam import iter_files
from memory_foam.client import Client
from memory_foam.file import FilePointer
from hypothesis import strategies as st

from tests.conftest import DEFAULT_TREE

utc = timezone.utc

ENTRIES = [
    (
        FilePointer(
            source="",
            path="description",
            version="7e589b7d-382c-49a5-931f-2b999c930c5e",
            last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
            size=13,
        ),
        DEFAULT_TREE.get("description"),
    ),
    (
        FilePointer(
            source="",
            path="trees/oak.jpeg",
            version="309eb4a4-bba9-47c1-afcd-d7c51110af6f",
            last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
            size=4,
        ),
        DEFAULT_TREE.get("trees", {}).get("oak.jpeg"),
    ),
    (
        FilePointer(
            source="",
            path="trees/pine.jpeg",
            version="f9d168d3-6d1b-47ef-8f6a-81fce48de141",
            last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
            size=4,
        ),
        DEFAULT_TREE.get("trees", {}).get("pine.jpeg"),
    ),
    (
        FilePointer(
            source="",
            path="books/book1.txt",
            version="b9c31cf7-d011-466a-bf16-cf9da0cb422a",
            last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
            size=4,
        ),
        DEFAULT_TREE.get("books", {}).get("book1.txt"),
    ),
    (
        FilePointer(
            path="books/book2.txt",
            source="",
            version="3a8bb6d9-38db-47a8-8bcb-8972ea95aa20",
            last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
            size=3,
        ),
        DEFAULT_TREE.get("books", {}).get("book2.txt"),
    ),
    (
        FilePointer(
            source="",
            path="books/book3.txt",
            version="ee49e963-36a8-492a-b03a-e801b93afb40",
            last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
            size=4,
        ),
        DEFAULT_TREE.get("books", {}).get("book3.txt"),
    ),
    (
        FilePointer(
            source="",
            path="books/others/book4.txt",
            version="c5969421-6900-4060-bc39-d54f4a49b9fc",
            last_modified=datetime(2023, 2, 27, 18, 28, 54, tzinfo=utc),
            size=4,
        ),
        DEFAULT_TREE.get("books", {}).get("others", {}).get("book4.txt"),
    ),
]


@pytest.fixture
def client(cloud_server, cloud_server_credentials):
    return Client.get_client(cloud_server.src_uri, **cloud_server.client_config)


_non_null_text = st.text(
    alphabet=st.characters(blacklist_categories=["Cc", "Cs"]), min_size=1
)


def normalize_entries(entries):
    return {(e[0].path, e[1]) for e in entries}


def match_entries(result, expected):
    assert len(result) == len(expected)
    assert normalize_entries(result) == normalize_entries(expected)


def test_iter_files_success(client, mocker, cloud_type):
    mocker.patch("memory_foam.client.Client.get_client", return_value=client)
    results = [file for file in iter_files(f"{cloud_type}://fake-client/")]
    match_entries(results, ENTRIES)
