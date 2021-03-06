"""
Event controllers.

"""
from microcosm_flask.conventions.crud_adapter import CRUDStoreAdapter

from microcosm_eventsource.factory import EventFactory


class EventController(CRUDStoreAdapter):

    def __init__(self, graph, store):
        super().__init__(graph, store)
        self.sns_producer = graph.sns_producer

    @property
    def event_factory(self):
        """
        By default, create a new event factory.

        """
        return EventFactory(
            event_store=self.store,
            identifier_key=self.identifier_key,
        )

    def create(self, event_type, **kwargs):
        """
        Delegate to the event factory.

        """
        return self.event_factory.create(self.ns, self.sns_producer, event_type, **kwargs)
