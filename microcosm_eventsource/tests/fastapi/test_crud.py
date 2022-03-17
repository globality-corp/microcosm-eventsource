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
            # breakpoint()

            # with session.connection() as conn:
            # conn = await session.connection()
            # seq = Sequence("task_event_clock_seq")
            # offset = await conn.run_sync(seq)

            seq = Sequence("task_event_clock_seq")
            # offset = await session.execute(seq)
            # We will have to fix this...
            offset = 6

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

    # data = loads(response.data.decode("utf-8"))
    # assert_that(data, has_entry("id", str(created_event_id)))
    # assert_that(data, has_entry("taskId", str(self.task.id)))
    # assert_that(data, has_entry("clock", 1 + self.offset))
    # assert_that(data, has_entry("parentId", none()))
    #
    # self.graph.sns_producer.produce.assert_called_with(
    #     media_type="application/vnd.globality.pubsub._.created.task_event.created",
    #     uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
    # )
    #
    # with SessionContext(self.graph), transaction():
    #     task_event = self.graph.task_event_store.retrieve(data["id"])
    #     assert_that(task_event.state, is_(contains(TaskEventType.CREATED)))


    # def test_assigned_event(self):
    #     with SessionContext(self.graph), transaction():
    #         created_event = list(islice(self.iter_events(), 1))[-1]
    #         assert_that(created_event.event_type, is_(equal_to(TaskEventType.CREATED)))
    #
    #     assigned_event_id = new_object_id()
    #     with patch.object(self.graph.task_event_store, "new_object_id") as mocked:
    #         mocked.return_value = assigned_event_id
    #         response = self.client.post(
    #             "/api/v1/task_event",
    #             data=dumps(dict(
    #                 assignee="Alice",
    #                 taskId=str(self.task.id),
    #                 eventType=TaskEventType.ASSIGNED.name,
    #             )),
    #         )
    #     assert_that(response.status_code, is_(equal_to(201)))
    #
    #     data = loads(response.data.decode("utf-8"))
    #     assert_that(data, has_entry("id", str(assigned_event_id)))
    #     assert_that(data, has_entry("taskId", str(self.task.id)))
    #     assert_that(data, has_entry("clock", 2 + self.offset))
    #     assert_that(data, has_entry("parentId", str(created_event.id)))
    #     assert_that(data, has_entry("version", 1))
    #
    #     self.graph.sns_producer.produce.assert_called_with(
    #         media_type="application/vnd.globality.pubsub._.created.task_event.assigned",
    #         uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
    #     )
    #
    #     with SessionContext(self.graph), transaction():
    #         task_event = self.graph.task_event_store.retrieve(data["id"])
    #         assert_that(task_event.state, is_(contains(TaskEventType.ASSIGNED, TaskEventType.CREATED)))
    #
    # def test_missing_required_field(self):
    #     with SessionContext(self.graph), transaction():
    #         created_event = list(islice(self.iter_events(), 1))[-1]
    #         assert_that(created_event.event_type, is_(equal_to(TaskEventType.CREATED)))
    #
    #     response = self.client.post(
    #         "/api/v1/task_event",
    #         data=dumps(dict(
    #             taskId=str(self.task.id),
    #             eventType=TaskEventType.ASSIGNED.name,
    #         )),
    #     )
    #     assert_that(response.status_code, is_(equal_to(422)))
    #
    # def test_transition_to_started(self):
    #     with SessionContext(self.graph), transaction():
    #         scheduled_event = list(islice(self.iter_events(), 3))[-1]
    #         assert_that(scheduled_event.event_type, is_(equal_to(TaskEventType.SCHEDULED)))
    #
    #     response = self.client.post(
    #         "/api/v1/task_event",
    #         data=dumps(dict(
    #             taskId=str(self.task.id),
    #             eventType=TaskEventType.STARTED.name,
    #         )),
    #     )
    #     assert_that(response.status_code, is_(equal_to(201)))
    #
    #     data = loads(response.data.decode("utf-8"))
    #     assert_that(data, has_entry("taskId", str(self.task.id)))
    #     assert_that(data, has_entry("clock", 4 + self.offset))
    #     assert_that(data, has_entry("parentId", str(scheduled_event.id)))
    #     assert_that(data, has_entry("version", 1))
    #
    #     self.graph.sns_producer.produce.assert_called_with(
    #         media_type="application/vnd.globality.pubsub._.created.task_event.started",
    #         uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
    #     )
    #
    #     with SessionContext(self.graph), transaction():
    #         task_event = self.graph.task_event_store.retrieve(data["id"])
    #         assert_that(task_event.state, is_(contains(TaskEventType.STARTED)))
    #
    # def test_transition_to_reassigned(self):
    #     with SessionContext(self.graph), transaction():
    #         started_event = list(islice(self.iter_events(), 4))[-1]
    #         assert_that(started_event.event_type, is_(equal_to(TaskEventType.STARTED)))
    #
    #     response = self.client.post(
    #         "/api/v1/task_event",
    #         data=dumps(dict(
    #             assignee="Bob",
    #             taskId=str(self.task.id),
    #             eventType=TaskEventType.REASSIGNED.name,
    #         )),
    #     )
    #     assert_that(response.status_code, is_(equal_to(201)))
    #
    #     data = loads(response.data.decode("utf-8"))
    #     assert_that(data, has_entry("assignee", "Bob"))
    #     assert_that(data, has_entry("taskId", str(self.task.id)))
    #     assert_that(data, has_entry("clock", 5 + self.offset))
    #     assert_that(data, has_entry("parentId", str(started_event.id)))
    #     assert_that(data, has_entry("version", 1))
    #
    #     self.graph.sns_producer.produce.assert_called_with(
    #         media_type="application/vnd.globality.pubsub._.created.task_event.reassigned",
    #         uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
    #     )
    #
    #     with SessionContext(self.graph), transaction():
    #         task_event = self.graph.task_event_store.retrieve(data["id"])
    #         assert_that(task_event.state, is_(contains(TaskEventType.STARTED)))
    #
    # def test_transition_to_reassigned_again(self):
    #     with SessionContext(self.graph), transaction():
    #         reassigned_event = list(islice(self.iter_events(), 5))[-1]
    #         assert_that(reassigned_event.event_type, is_(equal_to(TaskEventType.REASSIGNED)))
    #
    #     response = self.client.post(
    #         "/api/v1/task_event",
    #         data=dumps(dict(
    #             assignee="Alice",
    #             taskId=str(self.task.id),
    #             eventType=TaskEventType.REASSIGNED.name,
    #         )),
    #     )
    #     assert_that(response.status_code, is_(equal_to(201)))
    #
    #     data = loads(response.data.decode("utf-8"))
    #     assert_that(data, has_entry("assignee", "Alice"))
    #     assert_that(data, has_entry("taskId", str(self.task.id)))
    #     assert_that(data, has_entry("clock", 6 + self.offset))
    #     assert_that(data, has_entry("parentId", str(reassigned_event.id)))
    #     assert_that(data, has_entry("version", 1))
    #
    #     self.graph.sns_producer.produce.assert_called_with(
    #         media_type="application/vnd.globality.pubsub._.created.task_event.reassigned",
    #         uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
    #     )
    #
    #     with SessionContext(self.graph), transaction():
    #         task_event = self.graph.task_event_store.retrieve(data["id"])
    #         assert_that(task_event.state, is_(contains(TaskEventType.STARTED)))
    #
    # def test_transition_to_revised(self):
    #     with SessionContext(self.graph), transaction():
    #         reassigned_event = list(islice(self.iter_events(), 4))[-1]
    #         assert_that(reassigned_event.event_type, is_(equal_to(TaskEventType.STARTED)))
    #
    #     response = self.client.post(
    #         "/api/v1/task_event",
    #         data=dumps(dict(
    #             taskId=str(self.task.id),
    #             eventType=TaskEventType.REVISED.name,
    #         )),
    #     )
    #     assert_that(response.status_code, is_(equal_to(201)))
    #
    #     data = loads(response.data.decode("utf-8"))
    #     assert_that(data, has_entry("taskId", str(self.task.id)))
    #     assert_that(data, has_entry("clock", 5 + self.offset))
    #     assert_that(data, has_entry("parentId", str(reassigned_event.id)))
    #     assert_that(data, has_entry("version", 2))
    #
    #     self.graph.sns_producer.produce.assert_called_with(
    #         media_type="application/vnd.globality.pubsub._.created.task_event.revised",
    #         uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
    #     )
    #
    #     with SessionContext(self.graph), transaction():
    #         task_event = self.graph.task_event_store.retrieve(data["id"])
    #         assert_that(task_event.state, is_(contains(TaskEventType.CREATED)))
    #
    # def test_auto_transition(self):
    #     with SessionContext(self.graph), transaction():
    #         started_event = list(islice(self.iter_events(), 4))[-1]
    #         assert_that(started_event.event_type, is_(equal_to(TaskEventType.STARTED)))
    #
    #     response = self.client.post(
    #         "/api/v1/task_event",
    #         data=dumps(dict(
    #             taskId=str(self.task.id),
    #             eventType=TaskEventType.COMPLETED.name,
    #         )),
    #     )
    #     assert_that(response.status_code, is_(equal_to(201)))
    #
    #     data = loads(response.data.decode("utf-8"))
    #     assert_that(data, has_entry("eventType", str(TaskEventType.COMPLETED)))
    #
    #     with SessionContext(self.graph), transaction():
    #         completed_task_event = self.graph.task_event_store.retrieve(data["id"])
    #         assert_that(completed_task_event.event_type, is_(TaskEventType.COMPLETED))
    #         assert_that(completed_task_event.clock, is_(5 + self.offset))
    #         ended_task_event = self.graph.task_event_store.retrieve_most_recent(task_id=self.task.id)
    #         assert_that(ended_task_event.event_type, is_(TaskEventType.ENDED))
    #         assert_that(ended_task_event.clock, is_(6 + self.offset))
    #
    #     self.graph.sns_producer.produce.assert_has_calls(
    #         [
    #             call(
    #                 media_type="application/vnd.globality.pubsub._.created.task_event.completed",
    #                 uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
    #             ),
    #             call(
    #                 media_type="application/vnd.globality.pubsub._.created.task_event.ended",
    #                 uri="http://localhost/api/v1/task_event/{}".format(ended_task_event.id),
    #             ),
    #         ],
    #     )
    #
    # def test_configure_pubsub_event(self):
    #     created_event_id = new_object_id()
    #     with patch.object(self.graph.task_event_controller, "get_event_factory_kwargs") as mocked_factory_kwargs:
    #         mocked_factory_kwargs.return_value = dict(
    #             publish_event_pubsub=False,
    #             publish_model_pubsub=True,
    #         )
    #         with patch.object(self.graph.task_event_store, "new_object_id") as mocked:
    #             mocked.return_value = created_event_id
    #             response = self.client.post(
    #                 "/api/v1/task_event",
    #                 data=dumps(dict(
    #                     taskId=str(self.task.id),
    #                     eventType=TaskEventType.CREATED.name,
    #                 )),
    #             )
    #     assert_that(response.status_code, is_(equal_to(201)))
    #     self.graph.sns_producer.produce.assert_called_with(
    #         media_type="application/vnd.globality.pubsub._.created.task_event",
    #         uri="http://localhost/api/v1/task_event/{}".format(created_event_id),
    #     )
    #
    # def test_search_task_events_by_clock(self):
    #     with SessionContext(self.graph), transaction():
    #         created_event = list(islice(self.iter_events(), 4))[-1]
    #         assert_that(created_event.event_type, is_(equal_to(TaskEventType.STARTED)))
    #
    #     descending_response = self.client.get(
    #         "/api/v1/task_event?sort_by_clock=true",
    #     )
    #     assert_that(descending_response.status_code, is_(equal_to(200)))
    #     data = loads(descending_response.data.decode("utf-8"))
    #     descending_order_clock_list = [event['clock'] for event in data["items"]]
    #     assert_that(descending_order_clock_list,
    #                 is_(equal_to([4 + self.offset, 3 + self.offset, 2 + self.offset, 1 + self.offset])))
    #
    #     ascending_response = self.client.get(
    #         "/api/v1/task_event?sort_by_clock=true&sort_clock_in_ascending_order=true",
    #     )
    #     assert_that(ascending_response.status_code, is_(equal_to(200)))
    #     data = loads(ascending_response.data.decode("utf-8"))
    #     ascending_order_clock_list = [event['clock'] for event in data["items"]]
    #     assert_that(ascending_order_clock_list,
    #                 is_(equal_to([1 + self.offset, 2 + self.offset, 3 + self.offset, 4 + self.offset])))
    #
    #     invalid_response = self.client.get(
    #         "/api/v1/task_event?sort_clock_in_ascending_order=true",
    #     )
    #     assert_that(invalid_response.status_code, is_(equal_to(422)))
