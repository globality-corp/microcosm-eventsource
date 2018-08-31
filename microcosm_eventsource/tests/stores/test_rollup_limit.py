"""
Rolled up event limit tests.

"""
from os import pardir
from os.path import dirname, join

from hamcrest import (
    assert_that,
    calling,
    contains,
    contains_inanyorder,
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

from microcosm_eventsource.func import last
from microcosm_eventsource.stores import RollUpStore
from microcosm_eventsource.tests.fixtures import (
    SimpleTestObject,
    SimpleTestObjectEvent,
    SimpleTestObjectEventType,
)


class SimpleObjectTestRollupStore(RollUpStore):

    def __init__(self, graph):
        super().__init__(
            graph.simple_test_object_store,
            graph.simple_test_object_event_store,
        )

    def _aggregate(self, **kwargs):
        aggregate = super()._aggregate(**kwargs)
        aggregate.update(
            event_type=last.of(SimpleTestObjectEvent.event_type),
        )
        return aggregate

    def _filter(self, query, aggregate, event_type=None, **kwargs):
        if event_type:
            query = query.filter(
                aggregate["event_type"] == str(event_type)
            )

        return super()._filter(query, aggregate, **kwargs)


class TestRolledUpEventStore:

    def setup(self):
        self.graph = create_object_graph(
            "example",
            root_path=join(dirname(__file__), pardir),
            testing=True,
        )
        self.graph.use(
            "simple_test_object_store",
            "simple_test_object_event_store",
        )
        self.store = SimpleObjectTestRollupStore(self.graph)
        self.event_store = self.graph.simple_test_object_event_store

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

    def teardown(self):
        self.context.close()
        self.graph.postgres.dispose()

    def create_events(self, until=None, **kwargs):
        events = []
        for event in self.iter_events(**kwargs):
            if event.event_type == str(until):
                return events
            events.append(event)
        return events

    def iter_events(self, simple_test_object=None):
        with transaction():
            event = self.event_store.create(
                SimpleTestObjectEvent(
                    event_type=str(SimpleTestObjectEventType.CREATED),
                    simple_test_object_id=simple_test_object.id,
                ),
            )

        yield event

        with transaction():
            event = self.event_store.create(
                SimpleTestObjectEvent(
                    event_type=str(SimpleTestObjectEventType.READY),
                    parent_id=event.id,
                    simple_test_object_id=simple_test_object.id,
                ),
            )

        yield event

        with transaction():
            event = self.event_store.create(
                SimpleTestObjectEvent(
                    event_type=str(SimpleTestObjectEventType.DONE),
                    parent_id=event.id,
                    simple_test_object_id=simple_test_object.id,
                ),
            )

        yield event

    def test_search_by_limit(self):
        with transaction():
            object1 = SimpleTestObject().create()
            object2 = SimpleTestObject().create()
            object3 = SimpleTestObject().create()
            object4 = SimpleTestObject().create()

            self.create_events(simple_test_object=object1, until=SimpleTestObjectEventType.CREATED)
            self.create_events(simple_test_object=object2, until=SimpleTestObjectEventType.READY)
            self.create_events(simple_test_object=object3, until=SimpleTestObjectEventType.DONE)
            self.create_events(simple_test_object=object4, until=SimpleTestObjectEventType.DONE)

            result = self.store.search()

            assert_that(result, has_length(4))
            assert_that(
                result,
                contains_inanyorder(
                    has_properties(
                        id=object1.id,
                        _event_type=str(SimpleTestObjectEventType.CREATED),
                    ),
                    has_properties(
                        id=object2.id,
                        _event_type=str(SimpleTestObjectEventType.READY),
                    ),
                    has_properties(
                        id=object3.id,
                        _event_type=str(SimpleTestObjectEventType.DONE),
                    ),
                    has_properties(
                        id=object4.id,
                        _event_type=str(SimpleTestObjectEventType.DONE),
                    ),
                ),
            )

            result = self.store.search(event_type=str(SimpleTestObjectEventType.DONE))

            assert_that(result, has_length(2))
            assert_that(
                result,
                contains_inanyorder(
                    has_properties(
                        id=object3.id,
                        _event_type=str(SimpleTestObjectEventType.DONE),
                    ),
                    has_properties(
                        id=object4.id,
                        _event_type=str(SimpleTestObjectEventType.DONE),
                    ),
                ),
            )
