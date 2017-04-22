"""
Persistence tests.

"""
from os.path import dirname

from hamcrest import (
    assert_that,
    calling,
    contains,
    equal_to,
    is_,
    none,
    not_none,
    raises,
)
from microcosm.api import create_object_graph
from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.errors import DuplicateModelError, ModelIntegrityError

from microcosm_eventsource.tests.fixtures import (
    Task,
    TaskEvent,
    TaskEventType,
)


class TestEventStore(object):

    def setup(self):
        self.graph = create_object_graph(
            "example",
            root_path=dirname(__file__),
            testing=True,
        )
        self.graph.use(
            "task_store",
            "task_event_store",
        )
        self.store = self.graph.task_event_store

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

        with transaction():
            self.task = Task().create()

    def teardown(self):
        self.context.close()
        self.graph.postgres.dispose()

    def test_column_declarations(self):
        assert_that(TaskEvent.clock, is_(not_none()))
        assert_that(TaskEvent.event_type, is_(not_none()))
        assert_that(TaskEvent.task_id, is_(not_none()))
        assert_that(TaskEvent.parent_id, is_(not_none()))
        assert_that(TaskEvent.version, is_(not_none()))

    def test_column_alias(self):
        assert_that(TaskEvent.container_id, is_(equal_to(TaskEvent.task_id)))

        task_event = TaskEvent()
        task_event.container_id = self.task.id
        assert_that(task_event.container_id, is_(equal_to(self.task.id)))
        assert_that(task_event.task_id, is_(equal_to(self.task.id)))

    def test_create_retrieve(self):
        """
        An event can be retrieved after it is created.

        """
        with transaction():
            task_event = TaskEvent(
                task_id=self.task.id,
                event_type=TaskEventType.CREATED,
            )
            self.store.create(task_event)

        assert_that(task_event.clock, is_(equal_to(1)))
        assert_that(task_event.parent_id, is_(none()))
        assert_that(task_event.state, contains(TaskEventType.CREATED))
        assert_that(task_event.version, is_(equal_to(1)))

        assert_that(
            self.store.retrieve(task_event.id),
            is_(equal_to(task_event)),
        )

    def test_non_initial_event_requires_parent_id(self):
        """
        A non-initial event must have a previous event.

        """
        task_event = TaskEvent(
            task_id=self.task.id,
            event_type=TaskEventType.STARTED,
        )
        assert_that(
            calling(self.store.create).with_args(task_event),
            raises(ModelIntegrityError),
        )

    def test_multi_valued_state(self):
        """
        A state can contain multiple values.

        """
        with transaction():
            task_event = TaskEvent(
                task_id=self.task.id,
                event_type=TaskEventType.CREATED,
                state=(TaskEventType.CREATED, TaskEventType.ASSIGNED),
            )
            self.store.create(task_event)

        assert_that(
            sorted(task_event.state),
            contains(TaskEventType.ASSIGNED, TaskEventType.CREATED),
        )

    def test_retrieve_most_recent(self):
        """
        The logical clock provides a total ordering on the main foreign key.

        """
        with transaction():
            created_event = TaskEvent(
                task_id=self.task.id,
                event_type=TaskEventType.CREATED,
            )
            self.store.create(created_event)
            assigned_event = TaskEvent(
                task_id=self.task.id,
                event_type=TaskEventType.ASSIGNED,
                assignee="Alice",
                parent_id=created_event.id,
            )
            self.store.create(assigned_event)

        assert_that(
            self.store.retrieve_most_recent(task_id=self.task.id),
            is_(equal_to(assigned_event)),
        )

    def test_unique_event(self):
        """
        Events are unique per container, parent, and version.

        """
        with transaction():
            task_event = TaskEvent(
                task_id=self.task.id,
                event_type=TaskEventType.CREATED,
            )
            self.store.create(task_event)

        upserted = self.store.upsert_unique_event(
            TaskEvent(
                task_id=self.task.id,
                event_type=TaskEventType.CREATED,
            )
        )

        assert_that(task_event.id, is_(equal_to(upserted.id)))

    def test_upsert_unique_event(self):
        """
        Events that represent a unique state can be upserted.

        """
        with transaction():
            task_event = TaskEvent(
                task_id=self.task.id,
                event_type=TaskEventType.CREATED,
            )
            self.store.create(task_event)

        assert_that(
            calling(self.store.create).with_args(TaskEvent(
                task_id=self.task.id,
                event_type=TaskEventType.CREATED,
            )),
            raises(DuplicateModelError),
        )
