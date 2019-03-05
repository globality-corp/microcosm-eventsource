"""
Test task crud routes.

"""
from datetime import datetime
from itertools import islice
from json import dumps, loads
from os import environ
from os.path import dirname
from unittest.mock import call, patch

from hamcrest import (
    assert_that,
    contains,
    has_entry,
    equal_to,
    is_,
    none,
)
from microcosm.api import create_object_graph
from microcosm.loaders import load_from_dict
from microcosm_postgres.identifiers import new_object_id
from microcosm_postgres.operations import recreate_all
from microcosm_postgres.context import SessionContext, transaction

from microcosm_eventsource.tests.fixtures import (
    Task,
    TaskEvent,
    TaskEventType,
)


class TestTaskEventCRUDRoutes:
    def setup(self):
        loader = load_from_dict(
            secret=dict(
                postgres=dict(
                    host=environ.get("EXAMPLE__POSTGRES__HOST", "localhost"),
                ),
            ),
            postgres=dict(
                host=environ.get("EXAMPLE__POSTGRES__HOST", "localhost"),
            ),
            sns_producer=dict(
                mock_sns=False,
            ),
        )
        self.graph = create_object_graph(
            "microcosm_eventsource",
            loader=loader,
            root_path=dirname(__file__),
            testing=True,
        )
        self.graph.use(
            "session_factory",
            "task_store",
            "task_event_store",
            "task_event_controller",
            "task_crud_routes",
        )
        self.client = self.graph.flask.test_client()
        recreate_all(self.graph)

        with SessionContext(self.graph), transaction():
            self.task = Task().create()
            self.graph.sns_producer.sns_client.reset_mock()

    def iter_events(self):
        """
        Walk through the event state machine.

        """
        created = TaskEvent(
            event_type=TaskEventType.CREATED,
            state=[TaskEventType.CREATED],
            task_id=self.task.id,
        ).create()
        yield created

        assigned = TaskEvent(
            assignee="Alice",
            event_type=TaskEventType.ASSIGNED,
            parent_id=created.id,
            state=[TaskEventType.CREATED, TaskEventType.ASSIGNED],
            task_id=self.task.id,
        ).create()
        yield assigned

        scheduled = TaskEvent(
            deadline=datetime.utcnow(),
            event_type=TaskEventType.SCHEDULED,
            parent_id=assigned.id,
            state=[TaskEventType.CREATED, TaskEventType.ASSIGNED, TaskEventType.SCHEDULED],
            task_id=self.task.id,
        ).create()
        yield scheduled

        started = TaskEvent(
            event_type=TaskEventType.STARTED,
            parent_id=scheduled.id,
            state=[TaskEventType.STARTED],
            task_id=self.task.id,
        ).create()
        yield started

        reassigned = TaskEvent(
            assignee="Bob",
            event_type=TaskEventType.REASSIGNED,
            parent_id=started.id,
            state=[TaskEventType.STARTED],
            task_id=self.task.id,
        ).create()
        yield reassigned

    def test_created_event(self):
        created_event_id = new_object_id()
        with patch.object(self.graph.task_event_store, "new_object_id") as mocked:
            mocked.return_value = created_event_id
            response = self.client.post(
                "/api/v1/task_event",
                data=dumps(dict(
                    taskId=str(self.task.id),
                    eventType=TaskEventType.CREATED.name,
                    parentId=str(self.task.id),
                )),
            )
        assert_that(response.status_code, is_(equal_to(201)))

        data = loads(response.data.decode("utf-8"))
        assert_that(data, has_entry("id", str(created_event_id)))
        assert_that(data, has_entry("taskId", str(self.task.id)))
        assert_that(data, has_entry("clock", 1))
        assert_that(data, has_entry("parentId", none()))

        self.graph.sns_producer.produce.assert_called_with(
            media_type="application/vnd.globality.pubsub._.created.task_event.created",
            uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
        )

        with SessionContext(self.graph), transaction():
            task_event = self.graph.task_event_store.retrieve(data["id"])
            assert_that(task_event.state, is_(contains(TaskEventType.CREATED)))

    def test_assigned_event(self):
        with SessionContext(self.graph), transaction():
            created_event = list(islice(self.iter_events(), 1))[-1]
            assert_that(created_event.event_type, is_(equal_to(TaskEventType.CREATED)))

        assigned_event_id = new_object_id()
        with patch.object(self.graph.task_event_store, "new_object_id") as mocked:
            mocked.return_value = assigned_event_id
            response = self.client.post(
                "/api/v1/task_event",
                data=dumps(dict(
                    assignee="Alice",
                    taskId=str(self.task.id),
                    eventType=TaskEventType.ASSIGNED.name,
                )),
            )
        assert_that(response.status_code, is_(equal_to(201)))

        data = loads(response.data.decode("utf-8"))
        assert_that(data, has_entry("id", str(assigned_event_id)))
        assert_that(data, has_entry("taskId", str(self.task.id)))
        assert_that(data, has_entry("clock", 2))
        assert_that(data, has_entry("parentId", str(created_event.id)))
        assert_that(data, has_entry("version", 1))

        self.graph.sns_producer.produce.assert_called_with(
            media_type="application/vnd.globality.pubsub._.created.task_event.assigned",
            uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
        )

        with SessionContext(self.graph), transaction():
            task_event = self.graph.task_event_store.retrieve(data["id"])
            assert_that(task_event.state, is_(contains(TaskEventType.ASSIGNED, TaskEventType.CREATED)))

    def test_missing_required_field(self):
        with SessionContext(self.graph), transaction():
            created_event = list(islice(self.iter_events(), 1))[-1]
            assert_that(created_event.event_type, is_(equal_to(TaskEventType.CREATED)))

        response = self.client.post(
            "/api/v1/task_event",
            data=dumps(dict(
                taskId=str(self.task.id),
                eventType=TaskEventType.ASSIGNED.name,
            )),
        )
        assert_that(response.status_code, is_(equal_to(422)))

    def test_transition_to_started(self):
        with SessionContext(self.graph), transaction():
            scheduled_event = list(islice(self.iter_events(), 3))[-1]
            assert_that(scheduled_event.event_type, is_(equal_to(TaskEventType.SCHEDULED)))

        response = self.client.post(
            "/api/v1/task_event",
            data=dumps(dict(
                taskId=str(self.task.id),
                eventType=TaskEventType.STARTED.name,
            )),
        )
        assert_that(response.status_code, is_(equal_to(201)))

        data = loads(response.data.decode("utf-8"))
        assert_that(data, has_entry("taskId", str(self.task.id)))
        assert_that(data, has_entry("clock", 4))
        assert_that(data, has_entry("parentId", str(scheduled_event.id)))
        assert_that(data, has_entry("version", 1))

        self.graph.sns_producer.produce.assert_called_with(
            media_type="application/vnd.globality.pubsub._.created.task_event.started",
            uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
        )

        with SessionContext(self.graph), transaction():
            task_event = self.graph.task_event_store.retrieve(data["id"])
            assert_that(task_event.state, is_(contains(TaskEventType.STARTED)))

    def test_transition_to_reassigned(self):
        with SessionContext(self.graph), transaction():
            started_event = list(islice(self.iter_events(), 4))[-1]
            assert_that(started_event.event_type, is_(equal_to(TaskEventType.STARTED)))

        response = self.client.post(
            "/api/v1/task_event",
            data=dumps(dict(
                assignee="Bob",
                taskId=str(self.task.id),
                eventType=TaskEventType.REASSIGNED.name,
            )),
        )
        assert_that(response.status_code, is_(equal_to(201)))

        data = loads(response.data.decode("utf-8"))
        assert_that(data, has_entry("assignee", "Bob"))
        assert_that(data, has_entry("taskId", str(self.task.id)))
        assert_that(data, has_entry("clock", 5))
        assert_that(data, has_entry("parentId", str(started_event.id)))
        assert_that(data, has_entry("version", 1))

        self.graph.sns_producer.produce.assert_called_with(
            media_type="application/vnd.globality.pubsub._.created.task_event.reassigned",
            uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
        )

        with SessionContext(self.graph), transaction():
            task_event = self.graph.task_event_store.retrieve(data["id"])
            assert_that(task_event.state, is_(contains(TaskEventType.STARTED)))

    def test_transition_to_reassigned_again(self):
        with SessionContext(self.graph), transaction():
            reassigned_event = list(islice(self.iter_events(), 5))[-1]
            assert_that(reassigned_event.event_type, is_(equal_to(TaskEventType.REASSIGNED)))

        response = self.client.post(
            "/api/v1/task_event",
            data=dumps(dict(
                assignee="Alice",
                taskId=str(self.task.id),
                eventType=TaskEventType.REASSIGNED.name,
            )),
        )
        assert_that(response.status_code, is_(equal_to(201)))

        data = loads(response.data.decode("utf-8"))
        assert_that(data, has_entry("assignee", "Alice"))
        assert_that(data, has_entry("taskId", str(self.task.id)))
        assert_that(data, has_entry("clock", 6))
        assert_that(data, has_entry("parentId", str(reassigned_event.id)))
        assert_that(data, has_entry("version", 1))

        self.graph.sns_producer.produce.assert_called_with(
            media_type="application/vnd.globality.pubsub._.created.task_event.reassigned",
            uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
        )

        with SessionContext(self.graph), transaction():
            task_event = self.graph.task_event_store.retrieve(data["id"])
            assert_that(task_event.state, is_(contains(TaskEventType.STARTED)))

    def test_transition_to_revised(self):
        with SessionContext(self.graph), transaction():
            reassigned_event = list(islice(self.iter_events(), 4))[-1]
            assert_that(reassigned_event.event_type, is_(equal_to(TaskEventType.STARTED)))

        response = self.client.post(
            "/api/v1/task_event",
            data=dumps(dict(
                taskId=str(self.task.id),
                eventType=TaskEventType.REVISED.name,
            )),
        )
        assert_that(response.status_code, is_(equal_to(201)))

        data = loads(response.data.decode("utf-8"))
        assert_that(data, has_entry("taskId", str(self.task.id)))
        assert_that(data, has_entry("clock", 5))
        assert_that(data, has_entry("parentId", str(reassigned_event.id)))
        assert_that(data, has_entry("version", 2))

        self.graph.sns_producer.produce.assert_called_with(
            media_type="application/vnd.globality.pubsub._.created.task_event.revised",
            uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
        )

        with SessionContext(self.graph), transaction():
            task_event = self.graph.task_event_store.retrieve(data["id"])
            assert_that(task_event.state, is_(contains(TaskEventType.CREATED)))

    def test_auto_transition(self):
        with SessionContext(self.graph), transaction():
            started_event = list(islice(self.iter_events(), 4))[-1]
            assert_that(started_event.event_type, is_(equal_to(TaskEventType.STARTED)))

        response = self.client.post(
            "/api/v1/task_event",
            data=dumps(dict(
                taskId=str(self.task.id),
                eventType=TaskEventType.COMPLETED.name,
            )),
        )
        assert_that(response.status_code, is_(equal_to(201)))

        data = loads(response.data.decode("utf-8"))
        assert_that(data, has_entry("eventType", str(TaskEventType.COMPLETED)))

        with SessionContext(self.graph), transaction():
            completed_task_event = self.graph.task_event_store.retrieve(data["id"])
            assert_that(completed_task_event.event_type, is_(TaskEventType.COMPLETED))
            assert_that(completed_task_event.clock, is_(5))
            ended_task_event = self.graph.task_event_store.retrieve_most_recent(task_id=self.task.id)
            assert_that(ended_task_event.event_type, is_(TaskEventType.ENDED))
            assert_that(ended_task_event.clock, is_(6))

        self.graph.sns_producer.produce.assert_has_calls(
            [
                call(
                    media_type="application/vnd.globality.pubsub._.created.task_event.completed",
                    uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
                ),
                call(
                    media_type="application/vnd.globality.pubsub._.created.task_event.ended",
                    uri="http://localhost/api/v1/task_event/{}".format(ended_task_event.id),
                ),
            ],
        )

    def test_configure_pubsub_event(self):
        created_event_id = new_object_id()
        with patch.object(self.graph.task_event_controller, "get_event_factory_kwargs") as mocked_factory_kwargs:
            mocked_factory_kwargs.return_value = dict(
                publish_event_pubsub=False,
                publish_model_pubsub=True,
            )
            with patch.object(self.graph.task_event_store, "new_object_id") as mocked:
                mocked.return_value = created_event_id
                response = self.client.post(
                    "/api/v1/task_event",
                    data=dumps(dict(
                        taskId=str(self.task.id),
                        eventType=TaskEventType.CREATED.name,
                        parentId=str(self.task.id),
                    )),
                )
        assert_that(response.status_code, is_(equal_to(201)))
        self.graph.sns_producer.produce.assert_called_with(
            media_type="application/vnd.globality.pubsub._.created.task_event",
            uri="http://localhost/api/v1/task_event/{}".format(created_event_id),
        )

    def test_search_task_events_by_clock(self):
        with SessionContext(self.graph), transaction():
            created_event = list(islice(self.iter_events(), 4))[-1]
            assert_that(created_event.event_type, is_(equal_to(TaskEventType.STARTED)))

        descending_response = self.client.get(
            "/api/v1/task_event?sort_by_clock=true",
        )
        assert_that(descending_response.status_code, is_(equal_to(200)))
        data = loads(descending_response.data.decode("utf-8"))
        descending_order_clock_list = [event['clock'] for event in data["items"]]
        assert_that(descending_order_clock_list, is_(equal_to([4, 3, 2, 1])))

        ascending_response = self.client.get(
            "/api/v1/task_event?sort_by_clock=true&sort_clock_in_ascending_order=true",
        )
        assert_that(ascending_response.status_code, is_(equal_to(200)))
        data = loads(ascending_response.data.decode("utf-8"))
        ascending_order_clock_list = [event['clock'] for event in data["items"]]
        assert_that(ascending_order_clock_list, is_(equal_to([1, 2, 3, 4])))

        invalid_response = self.client.get(
            "/api/v1/task_event?sort_clock_in_ascending_order=true",
        )
        assert_that(invalid_response.status_code, is_(equal_to(422)))
