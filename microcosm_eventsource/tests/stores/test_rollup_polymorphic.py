from hamcrest import (
    assert_that,
    has_properties,
)
from os import pardir
from os.path import dirname, join
from microcosm.api import create_object_graph
from microcosm_postgres.context import SessionContext, transaction

from microcosm_eventsource.func import last
from microcosm_eventsource.stores import RollUpStore
from microcosm_eventsource.tests.fixtures import (
    SubTask,
    SubTaskEvent,
    SubTaskEventType,
)


class SubTaskRollUpStore(RollUpStore):

    def __init__(self, graph):
        super().__init__(
            graph.sub_task_store,
            graph.sub_task_event_store,
        )

    def _aggregate(self, **kwargs):
        aggregate = super()._aggregate(**kwargs)
        aggregate.update(
            assignee=last.of(SubTaskEvent.assignee),
        )

        return aggregate


class TestPolymorphicEntityRolledUpEventStore:

    def setup(self):
        self.graph = create_object_graph(
            "example",
            root_path=join(dirname(__file__), pardir),
            testing=True,
        )
        self.graph.use(
            "sub_task_store",
            "sub_task_event_store",
        )
        self.store = SubTaskRollUpStore(self.graph)

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

        with transaction():
            self.sub_task = SubTask().create()
            self.sub_task_created_event = SubTaskEvent(
                event_type=SubTaskEventType.CREATED,
                sub_task_id=self.sub_task.id,
            ).create()
            self.sub_task_assigned_event = SubTaskEvent(
                assignee="Alice",
                event_type=SubTaskEventType.ASSIGNED,
                parent_id=self.sub_task_created_event.id,
                sub_task_id=self.sub_task.id,
            ).create()

    def teardown(self):
        self.context.close()
        self.graph.postgres.dispose()

    def test_retrieve(self):
        rollup = self.store.retrieve(self.sub_task.id)
        assert_that(rollup, has_properties(
            _event=self.sub_task_assigned_event,
            _container=self.sub_task,
            _rank=1,
            _assignee="Alice",
        ))
