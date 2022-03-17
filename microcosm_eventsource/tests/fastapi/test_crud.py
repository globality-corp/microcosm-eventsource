"""
Test task crud routes.

"""
from datetime import datetime
from itertools import islice
from json import dumps, loads
from os import environ
from os.path import dirname
from types import SimpleNamespace
from unittest.mock import call, patch

import pytest
from hamcrest import (
    assert_that,
    contains,
    equal_to,
    has_entry,
    is_,
    none,
)
from microcosm.api import create_object_graph
from microcosm.loaders import load_from_dict
from microcosm_postgres.identifiers import new_object_id
from sqlalchemy import select
from sqlalchemy.sql.schema import Sequence

from microcosm_eventsource.tests.fastapi.fixtures import Task, TaskEvent, TaskEventType


@pytest.fixture(scope="session")
def graph():
    loader = load_from_dict(
        secret=dict(
            postgres=dict(
                host=environ.get("MICROCOSM_EVENTSOURCE__POSTGRES__HOST", "localhost"),
                password=environ.get("MICROCOSM_EVENTSOURCE__POSTGRES__PASSWORD", ""),
            ),
        ),
        postgres=dict(
            host=environ.get("MICROCOSM_EVENTSOURCE__POSTGRES__HOST", "localhost"),
            password=environ.get("MICROCOSM_EVENTSOURCE__POSTGRES__PASSWORD", ""),
        ),
        sns_producer=dict(
            mock_sns=False,
        ),
    )
    graph = create_object_graph(
        "microcosm_eventsource",
        loader=loader,
        root_path=dirname(__file__),
        testing=True,
    )
    graph.use(
        # "session_factory",
        "sessionmaker",

        "session_maker_async",
        "postgres_async",

        "task_store_async",
        "task_event_store_async",
        "task_event_controller_async",
        "task_crud_routes_async",
    )
    return graph


@pytest.fixture
def test_graph(graph):
    graph.sns_producer.sns_client.reset_mock()
    yield graph


@pytest.fixture
async def db_fixtures(test_graph):
    async with test_graph.session_maker_async() as session:
        async with test_graph.task_store_async.with_transaction(session):
            task = await Task().create()

            seq = Sequence("task_event_clock_seq")
            offset = await session.execute(select(seq.next_value()))
            offset = offset.scalar()

    return SimpleNamespace(
        task=task,
        offset=offset,
    )


@pytest.mark.asyncio
async def test_created_event(client, test_graph, db_fixtures):
    created_event_id = new_object_id()
    with patch.object(test_graph.task_event_store_async, "new_object_id") as mocked:
        mocked.return_value = created_event_id
        response = await client.post(
            "/api/v1/task_event",
            data=dumps(dict(
                taskId=str(db_fixtures.task.id),
                eventType=TaskEventType.CREATED.name,
            )),
        )
    assert_that(response.status_code, is_(equal_to(201)))

    data = response.json()
    assert_that(data, has_entry("id", str(created_event_id)))
    assert_that(data, has_entry("taskId", str(db_fixtures.task.id)))
    assert_that(data, has_entry("clock", 1 + db_fixtures.offset))
    assert_that(data, has_entry("parentId", none()))

    test_graph.sns_producer.produce.assert_called_with(
        media_type="application/vnd.globality.pubsub._.created.task_event.created",
        uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
    )

    async with test_graph.session_maker_async() as session:
        task_event = await test_graph.task_event_store_async.retrieve(data["id"])
        assert_that(task_event.state, is_(contains(TaskEventType.CREATED)))
