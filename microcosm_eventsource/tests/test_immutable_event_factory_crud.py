"""
Test task crud routes.

"""
from json import dumps
from os import environ
from os.path import dirname

from hamcrest import (
    assert_that,
    calling,
    contains,
    equal_to,
    is_,
    none,
    raises,
)
from microcosm.api import create_object_graph
from microcosm.loaders import load_from_dict
from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.operations import recreate_all

from microcosm_eventsource.immutable_event_factory import (
    DuplicateEventHandlerRegistrationAttempted,
    common_container_mutator,
    event_specific_container_mutator,
)
from microcosm_eventsource.tests.fixtures import (
    ImmutableTask,
    ImmutableTaskEvent,
    ImmutableTaskEventType,
)


class TestTaskEventCRUDRoutes:
    def setup(self):
        loader = load_from_dict(
            secret=dict(
                postgres=dict(
                    host=environ.get("MICROCOSM_EVENTSOURCE__POSTGRES__HOST", "localhost"),
                ),
            ),
            postgres=dict(
                host=environ.get("MICROCOSM_EVENTSOURCE__POSTGRES__HOST", "localhost"),
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
            "immutable_task_store",
            "immutable_task_event_store",
            "immutable_task_event_controller",
            "immutable_task_crud_routes",
        )
        self.client = self.graph.flask.test_client()
        recreate_all(self.graph)

        with SessionContext(self.graph), transaction():
            self.task = ImmutableTask().create()

    def test_created_event_immutable_factory(self):
        response = self.client.post(
            "/api/v1/immutable_task_event",
            data=dumps(dict(
                immutableTaskId=str(self.task.id),
                eventType=ImmutableTaskEventType.CREATED.name,
            )),
        )
        task_event_id = response.json["id"]
        assert_that(response.status_code, is_(equal_to(201)))

        with SessionContext(self.graph), transaction():
            task_event = self.graph.immutable_task_event_store.retrieve(task_event_id)
            assert_that(task_event.state, is_(contains(ImmutableTaskEventType.CREATED)))

    def test_register_common_event_handler(self):
        with SessionContext(self.graph), transaction():
            task = self.graph.immutable_task_store.retrieve(self.task.id)
            assert_that(task.latest_task_event, is_(none()))
        response = self.client.post(
            "/api/v1/immutable_task_event",
            data=dumps(dict(
                immutableTaskId=str(self.task.id),
                eventType=ImmutableTaskEventType.CREATED.name,
            )),
        )
        assert_that(response.status_code, is_(equal_to(201)))

        with SessionContext(self.graph), transaction():
            task = self.graph.immutable_task_store.retrieve(self.task.id)
            assert_that(task.latest_task_event, is_(equal_to(ImmutableTaskEventType.CREATED)))

    def test_register_event_specific_handler(self):
        with SessionContext(self.graph), transaction():
            task = self.graph.immutable_task_store.retrieve(self.task.id)
            assert_that(task.is_assigned, is_(equal_to(None)))
        response = self.client.post(
            "/api/v1/immutable_task_event",
            data=dumps(dict(
                immutableTaskId=str(self.task.id),
                eventType=ImmutableTaskEventType.CREATED.name,
            )),
        )
        assert_that(response.status_code, is_(equal_to(201)))

        response = self.client.post(
            "/api/v1/immutable_task_event",
            data=dumps(dict(
                immutableTaskId=str(self.task.id),
                eventType=ImmutableTaskEventType.ASSIGNED.name,
                assignee="some assignee",
            )),
        )
        assert_that(response.status_code, is_(equal_to(201)))

        with SessionContext(self.graph), transaction():
            task = self.graph.immutable_task_store.retrieve(self.task.id)
            assert_that(task.is_assigned, is_(equal_to(True)))
            assert_that(task.latest_task_event, is_(equal_to(ImmutableTaskEventType.ASSIGNED)))

    def test_register_multiple_event_specific_handler_on_same_event_raises_exception(self):
        registered_event_specific_func = event_specific_container_mutator(
            event_instance_type=ImmutableTaskEventType.SCHEDULED)
        assert_that(calling(registered_event_specific_func).with_args("any args"),
                    raises(DuplicateEventHandlerRegistrationAttempted,
                           "Mutator is already registered for event instance type: %s" %
                           ImmutableTaskEventType.SCHEDULED.name))

        registered_common_func = common_container_mutator(event_type=ImmutableTaskEvent)
        assert_that(calling(registered_common_func).with_args("any args"),
                    raises(DuplicateEventHandlerRegistrationAttempted,
                           "Mutator is already registered for event type: %s" % ImmutableTaskEvent.__name__))
