from typing import Callable, Iterator, Optional

import pytest
from elasticsearch import Elasticsearch
from elasticsearch import __version__ as elastic_version


def elasticsearch(
    scope: str = "module",
    host: Optional[str] = "localhost",
    port: Optional[int] = 9200,
    username: Optional[str] = None,
    password: Optional[str] = None,
    cleanup_pattern: Optional[str] = None,
) -> Callable[[pytest.FixtureRequest], Iterator[Elasticsearch]]:
    """Create Elasticsearch client fixture.
    :param process_fixture_name: elasticsearch process fixture name
    """

    @pytest.fixture(scope=scope)
    def elasticsearch_fixture() -> Iterator[Elasticsearch]:
        """Elasticsearch client fixture."""
        client_def = {
            "host": host,
            "port": port,
            "scheme": "http",
        }
        if username and password:
            client_def["http_auth"] = f"{username}:{password}"

        client = Elasticsearch(
            hosts=[client_def],
            request_timeout=30,
            verify_certs=False,
        )
        if elastic_version >= (8, 0, 0):
            client.options(ignore_status=400)

        yield client
        if cleanup_pattern is not None:
            for index in client.indices.get_alias(cleanup_pattern):
                client.indices.delete(index=index)

    return elasticsearch_fixture


def create_records(es_client: Elasticsearch, index: str, records: list[dict]) -> None:
    """Create records in Elasticsearch."""
    for record in records:
        es_client.index(index=index, body=record)
