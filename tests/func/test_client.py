from memory_foam.client import Client
from memory_foam.asyn import get_loop
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st


_non_null_text = st.text(
    alphabet=st.characters(blacklist_categories=["Cc", "Cs"]), min_size=1
)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None)
@given(rel_path=_non_null_text)
def test_parse_url(cloud_server, rel_path):
    bucket_uri = cloud_server.src_uri
    url = f"{bucket_uri}/{rel_path}"
    client = Client.get_client(url, get_loop(), None)
    uri, rel_part = client.parse_url(url)
    assert uri == bucket_uri
    assert rel_part == rel_path
