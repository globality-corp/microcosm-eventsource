import asyncio

import pytest
from httpx import AsyncClient
from microcosm_postgres.models import Model

# from babilonia.app import create_app


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def client(graph):
    async with AsyncClient(app=graph.app, base_url="http://localhost") as client:
        yield client


@pytest.fixture
async def setup_db(event_loop, graph):
    # TODO: Refactor into the common fastapi library code
    async with graph.postgres_async.begin() as connection:
        await connection.run_sync(Model.metadata.drop_all)
        await connection.run_sync(Model.metadata.create_all)


@pytest.fixture
def test_graph(graph):
    graph.sns_producer.sns_client.reset_mock()
    yield graph
