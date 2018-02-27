"""
Rolled up event tests.

"""
from os import pardir
from os.path import dirname, join

from hamcrest import (
    assert_that,
    calling,
    contains,
    equal_to,
    has_length,
    has_properties,
    is_,
    raises,
)
from microcosm.api import create_object_graph
from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.errors import ModelNotFoundError
from microcosm_postgres.identifiers import new_object_id

from microcosm_eventsource.stores import RolledUpEventStore
from microcosm_eventsource.tests.fixtures import (
    Task,
    TaskEvent,
    TaskEventType,
)


class TestRolledUpEventStore:

    def setup(self):
        self.graph = create_object_graph(
            "example",
            root_path=join(dirname(__file__), pardir),
            testing=True,
        )
        self.graph.use(
            "task_store",
            "task_event_store",
            "activity_store",
            "activity_event_store",
        )
        self.store = RolledUpEventStore(
            self.graph.task_store,
            self.graph.task_event_store,
        )

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

        with transaction():
            self.task1 = Task().create()
            self.task2 = Task().create()
            self.task1_created_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                task_id=self.task1.id,
            ).create()
            self.task2_created_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                task_id=self.task2.id,
            ).create()
            self.task2_assigned_event = TaskEvent(
                assignee="Alice",
                event_type=TaskEventType.ASSIGNED,
                parent_id=self.task2_created_event.id,
                task_id=self.task2.id,
            ).create()

    def teardown(self):
        self.context.close()
        self.graph.postgres.dispose()

    def test_count(self):
        count = self.store.count()
        assert_that(count, is_(equal_to(2)))

    def test_retrieve(self):
        rollup = self.store.retrieve(self.task2.id)
        assert_that(rollup, has_properties(
            _event=self.task2_assigned_event,
            _container=self.task2,
            _rank=1,
        ))

    def test_retrieve_not_found(self):
        assert_that(
            calling(self.store.retrieve).with_args(new_object_id()),
            raises(ModelNotFoundError),
        )

    def test_search(self):
        results = self.store.search()
        assert_that(results, has_length(2))
        assert_that(results, contains(
            has_properties(
                _event=self.task2_assigned_event,
                _container=self.task2,
                _rank=1,
            ),
            has_properties(
                _event=self.task1_created_event,
                _container=self.task1,
                _rank=1,
            ),
        ))
