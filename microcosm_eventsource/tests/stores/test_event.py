"""
Persistence tests.

"""
from os import pardir
from os.path import dirname, join
from unittest.mock import patch

import psycopg2
from hamcrest import (
    assert_that,
    calling,
    contains,
    contains_inanyorder,
    equal_to,
    is_,
    none,
    not_none,
    raises,
)
from microcosm.api import create_object_graph
from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.errors import DuplicateModelError, ModelIntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Query
from sqlalchemy.sql.schema import Sequence

from microcosm_eventsource.errors import (
    ConcurrentStateConflictError,
    ContainerLockNotAvailableRetry,
)
from microcosm_eventsource.tests.fixtures import (
    Activity,
    ActivityEvent,
    ActivityEventType,
    Task,
    TaskEvent,
    TaskEventType,
)


class TestEventStore:

    def setup(self):
        self.graph = create_object_graph(
            "microcosm_eventsource",
            root_path=join(dirname(__file__), pardir),
            testing=True,
        )
        self.graph.use(
            "task_store",
            "task_event_store",
            "activity_store",
            "activity_event_store",
        )
        self.store = self.graph.task_event_store

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

        with transaction() as session:
            self.task = Task().create()

            self.offset = session.execute(Sequence("task_event_clock_seq"))

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
                event_type=TaskEventType.CREATED,
                task_id=self.task.id,
            )
            self.store.create(task_event)

        assert_that(task_event.clock, is_(equal_to(1 + self.offset)))
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
            event_type=TaskEventType.STARTED,
            task_id=self.task.id,
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
                event_type=TaskEventType.CREATED,
                state=(TaskEventType.CREATED, TaskEventType.ASSIGNED),
                task_id=self.task.id,
            )
            self.store.create(task_event)

        assert_that(
            task_event.state,
            contains_inanyorder(TaskEventType.ASSIGNED, TaskEventType.CREATED),
        )

    def test_retrieve_most_recent(self):
        """
        The logical clock provides a total ordering on the main foreign key.

        """
        with transaction():
            created_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                task_id=self.task.id,
            )
            self.store.create(created_event)
            assigned_event = TaskEvent(
                assignee="Alice",
                event_type=TaskEventType.ASSIGNED,
                parent_id=created_event.id,
                task_id=self.task.id,
            )
            self.store.create(assigned_event)

        assert_that(
            self.store.retrieve_most_recent(task_id=self.task.id),
            is_(equal_to(assigned_event)),
        )

    def test_unique_parent_id(self):
        """
        Events are unique per parent.

        """
        with transaction():
            created_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                task_id=self.task.id,
            )
            self.store.create(created_event)
            task_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                parent_id=created_event.id,
                task_id=self.task.id,
            )
            self.store.create(task_event)

        assert_that(
            calling(self.store.create).with_args(
                TaskEvent(
                    event_type=TaskEventType.CREATED,
                    parent_id=created_event.id,
                    task_id=self.task.id,
                ),
            ),
            raises(DuplicateModelError),
        )

    def test_upsert_on_index_elements(self):
        """
        Events with a duplicate index elements can be upserted.

        """
        with transaction():
            created_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                task_id=self.task.id,
            )
            self.store.create(created_event)
            task_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                parent_id=created_event.id,
                task_id=self.task.id,
            )
            self.store.create(task_event)

        upserted = self.store.upsert_on_index_elements(
            TaskEvent(
                event_type=TaskEventType.CREATED,
                parent_id=created_event.id,
                task_id=self.task.id,
            )
        )

        assert_that(task_event.id, is_(equal_to(upserted.id)))

    def test_upsert_on_index_elements_mismatch(self):
        """
        Events with a duplicate index elements cannot be upsert if they don't match.

        """
        with transaction():
            created_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                task_id=self.task.id,
            )
            self.store.create(created_event)
            task_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                parent_id=created_event.id,
                task_id=self.task.id,
            )
            self.store.create(task_event)

        assert_that(
            calling(self.store.upsert_on_index_elements).with_args(TaskEvent(
                event_type=TaskEventType.REVISED,
                parent_id=created_event.id,
                task_id=self.task.id,
            )),
            raises(ConcurrentStateConflictError),
        )

    def test_multiple_children_per_parent(self):
        """
        Events are not unique per parent for False unique_parent events.

        """
        with transaction():
            self.activity = Activity().create()
            created_event = ActivityEvent(
                event_type=ActivityEventType.CREATED,
                activity_id=self.activity.id,
            )
            self.store.create(created_event)
            task_event = ActivityEvent(
                event_type=ActivityEventType.CANCELED,
                parent_id=created_event.id,
                activity_id=self.activity.id,
            )
            self.store.create(task_event)
            same_parent_task_event = ActivityEvent(
                event_type=ActivityEventType.CANCELED,
                parent_id=created_event.id,
                activity_id=self.activity.id,
            )
            self.store.create(same_parent_task_event)

        assert_that(same_parent_task_event.parent_id, is_(created_event.id))

    def test_retrieve_with_update_lock(self):
        with transaction():
            created_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                task_id=self.task.id,
            )
            self.store.create(created_event)
        assert_that(
            self.store.retrieve_most_recent_with_update_lock(task_id=self.task.id),
            is_(equal_to(created_event)),
        )

    def test_retrieve_with_update_lock_exception(self):
        with transaction():
            created_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                task_id=self.task.id,
            )
            self.store.create(created_event)

        with patch.object(Query, 'with_for_update') as mocked_with_for_update:
            mocked_with_for_update.side_effect = OperationalError(
                statement="", params="",
                orig=psycopg2.errors.LockNotAvailable())

            assert_that(
                calling(self.store.retrieve_most_recent_with_update_lock).with_args(
                    task_id=self.task.id,
                ), raises(ContainerLockNotAvailableRetry),
            )

        with patch.object(Query, 'with_for_update') as mocked_with_for_update:
            mocked_with_for_update.side_effect = Exception()

            assert_that(
                calling(self.store.retrieve_most_recent_with_update_lock).with_args(
                    task_id=self.task.id,
                ), raises(Exception),
            )
