from argparse import _AppendAction
from typing import Any
import pytest
import attrs
from upath.implementations.cloud import CloudPath


class CommaSeparatedArgs(_AppendAction):
    def __call__(self, parser, namespace, values, option_string=None):
        items = getattr(namespace, self.dest) or []
        items.extend(v for value in values.split(",") if (v := value.strip()))
        setattr(namespace, self.dest, list(dict.fromkeys(items)))


def pytest_addoption(parser):
    parser.addoption(
        "--disable-remotes",
        action=CommaSeparatedArgs,
        default=[],
        help="Comma separated list of remotes to disable",
    )


def pytest_collection_modifyitems(config, items):
    disabled_remotes = config.getoption("--disable-remotes")
    if not disabled_remotes:
        return

    for item in items:
        if "cloud_server" in item.fixturenames:
            cloud_type = item.callspec.params.get("cloud_type")
            if cloud_type not in cloud_types:
                continue
            if cloud_type in disabled_remotes:
                reason = f"Skipping all tests for {cloud_type=}"
                item.add_marker(pytest.mark.skip(reason=reason))


DEFAULT_TREE: dict[str, Any] = {
    "description": b"Images of trees and book texts",
    "trees": {"oak.jpeg": b"\xff\xd8\xff\xe0", "pine.jpeg": b"\xff\xd8\xff\xe0"},
    "books": {
        "book1.txt": b"this is a book, which is not a good read",
        "book2.txt": b"some text about cats and dogs",
        "book3.txt": b"once upon a time some nuffy tried to access s3",
        "others": {"book4.txt": b"my friend once created a nested directory"},
    },
}
NUM_TREE = {f"{i:06d}": f"{i}" for i in range(1024)}


def instantiate_tree(path, tree):
    for key, value in tree.items():
        if isinstance(value, str):
            (path / key).write_text(value)
        elif isinstance(value, bytes):
            (path / key).write_bytes(value)
        elif isinstance(value, dict):
            (path / key).mkdir()
            instantiate_tree(path / key, value)
        else:
            raise TypeError(f"{value=}")


@pytest.fixture
def tmp_dir(tmp_path_factory, monkeypatch):
    dpath = tmp_path_factory.mktemp("memory_foam-test")
    monkeypatch.chdir(dpath)
    return dpath


@pytest.fixture(scope="session", params=[DEFAULT_TREE])
def tree(request):
    return request.param


@attrs.define
class CloudServer:
    kind: str
    src: CloudPath
    client_config: dict[str, str]

    @property
    def src_uri(self):
        if self.kind == "file":
            return self.src.as_uri()
        return str(self.src).rstrip("/")


cloud_types = [
    "azure",
    "gs",
    "s3",
]


@pytest.fixture(scope="session", params=cloud_types)
def cloud_type(request):
    return request.param


def make_cloud_server(src_path, cloud_type, tree):
    fs = src_path.fs
    if cloud_type == "s3":
        endpoint_url = fs.client_kwargs["endpoint_url"]
        client_config = {"aws_endpoint_url": endpoint_url}
    elif cloud_type in ("gs", "gcs"):
        endpoint_url = fs._endpoint
        client_config = {"endpoint_url": endpoint_url}
    elif cloud_type == "azure":
        client_config = fs.storage_options.copy()
    else:
        raise ValueError(f"invalid cloud_type: {cloud_type}")

    instantiate_tree(src_path, tree)
    return CloudServer(kind=cloud_type, src=src_path, client_config=client_config)


@pytest.fixture(scope="session", params=[False, True])
def version_aware(request):
    return request.param


@pytest.fixture
def cloud_server_credentials(cloud_server, monkeypatch):
    if cloud_server.kind == "s3":
        cfg = cloud_server.src.fs.client_kwargs
        try:
            monkeypatch.delenv("AWS_PROFILE")
        except KeyError:
            pass
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", cfg.get("aws_access_key_id"))
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", cfg.get("aws_secret_access_key"))
        monkeypatch.setenv("AWS_SESSION_TOKEN", cfg.get("aws_session_token"))
        monkeypatch.setenv("AWS_DEFAULT_REGION", cfg.get("region_name"))


@pytest.fixture(scope="session")
def cloud_server(request, tmp_upath_factory, cloud_type, version_aware, tree):
    if cloud_type == "azure" and version_aware:
        pytest.skip("Can't test versioning with Azure")
    else:
        src_path = tmp_upath_factory.mktemp(cloud_type, version_aware=version_aware)
    return make_cloud_server(src_path, cloud_type, tree)
