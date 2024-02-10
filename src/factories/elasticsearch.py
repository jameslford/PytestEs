from typing import Callable, Iterator, Optional

import pytest
from elasticsearch import Elasticsearch
from elasticsearch import __version__ as elastic_version


def elasticsearch(
    process_fixture_name: str,
    scope: str = "module",
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> Callable[[pytest.FixtureRequest], Iterator[Elasticsearch]]:
    """Create Elasticsearch client fixture.
    :param process_fixture_name: elasticsearch process fixture name
    """

    @pytest.fixture(scope=scope)
    def elasticsearch_fixture(
        request: pytest.FixtureRequest,
    ) -> Iterator[Elasticsearch]:
        """Elasticsearch client fixture."""
        process = request.getfixturevalue(process_fixture_name)
        if not process.running():
            process.start()
        client_def = {
            "host": process.host,
            "port": process.port,
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
        for index in client.indices.get_alias("*test*"):
            client.indices.delete(index=index)

    return elasticsearch_fixture
