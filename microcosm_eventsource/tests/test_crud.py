"""
Test task crud routes.

"""
from json import dumps, loads
from os.path import dirname

from hamcrest import (
    assert_that,
    has_entry,
    equal_to,
    is_,
    none,
)
from microcosm.api import create_object_graph
from microcosm.loaders import load_from_dict
from microcosm_postgres.operations import recreate_all
from microcosm_postgres.context import SessionContext, transaction

from microcosm_eventsource.tests.fixtures import (
    Task,
    TaskEvent,
    TaskEventType,
)


class TestTaskEventCRUDRoutes(object):
    def setup(self):
        loader = load_from_dict(
            sns_producer=dict(
                mock_sns=False,
            ),
        )
        self.graph = create_object_graph(
            "example",
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

    def test_create_first_event(self):
        response = self.client.post(
            "/api/v1/task_event",
            data=dumps(dict(
                taskId=str(self.task.id),
                eventType=TaskEventType.CREATED.name,
            )),
        )
        assert_that(response.status_code, is_(equal_to(201)))

        data = loads(response.data.decode("utf-8"))
        assert_that(data, has_entry("taskId", str(self.task.id)))
        assert_that(data, has_entry("clock", 1))
        assert_that(data, has_entry("parentId", none()))

        self.graph.sns_producer.produce.assert_called_with(
            media_type="application/vnd.globality.pubsub._.created.task_event.created",
            uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
        )

    def test_create_second_event(self):
        with SessionContext(self.graph), transaction():
            first_event = TaskEvent(
                task_id=self.task.id,
                event_type=TaskEventType.CREATED,
            ).create()

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
        assert_that(data, has_entry("taskId", str(self.task.id)))
        assert_that(data, has_entry("clock", 2))
        assert_that(data, has_entry("parentId", str(first_event.id)))

        self.graph.sns_producer.produce.assert_called_with(
            media_type="application/vnd.globality.pubsub._.created.task_event.assigned",
            uri="http://localhost/api/v1/task_event/{}".format(data["id"]),
        )

    def test_missing_required_field(self):
        with SessionContext(self.graph), transaction():
            TaskEvent(
                task_id=self.task.id,
                event_type=TaskEventType.CREATED,
            ).create()

        response = self.client.post(
            "/api/v1/task_event",
            data=dumps(dict(
                taskId=str(self.task.id),
                eventType=TaskEventType.ASSIGNED.name,
            )),
        )
        assert_that(response.status_code, is_(equal_to(422)))
