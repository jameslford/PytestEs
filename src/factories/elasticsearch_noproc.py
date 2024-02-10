from typing import Callable, Iterator, Optional

import pytest
from pytest import FixtureRequest


def elasticsearch_noproc(
    host: Optional[str] = None, port: Optional[int] = None
) -> Callable[[FixtureRequest], Iterator[NoopElasticsearch]]:
    """Elasticsearch noprocess factory.

    :param host: hostname
    :param port: exact port (e.g. '8000', 8000)
    :returns: function which makes a elasticsearch process
    """

    @pytest.fixture(scope="session")
    def elasticsearch_noproc_fixture(
        request: FixtureRequest,
    ) -> Iterator[NoopElasticsearch]:
        """Noop Process fixture for PostgreSQL.

        :param FixtureRequest request: fixture request object
        :rtype: pytest_dbfixtures.executors.TCPExecutor
        :returns: tcp executor-like object
        """
        config = get_config(request)
        es_host = host or config["host"]
        assert es_host
        es_port = port or config["port"] or 9300
        assert es_port

        yield NoopElasticsearch(host=es_host, port=es_port)

    return elasticsearch_noproc_fixture
