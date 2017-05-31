"""
Event controllers.

"""
from microcosm_flask.conventions.crud_adapter import CRUDStoreAdapter

from microcosm_eventsource.factory import EventFactory, EventInfo


class EventController(CRUDStoreAdapter):

    @property
    def sns_producer(self):
        return self.graph.sns_producer

    def create(self, event_type, **kwargs):
        event_factory = EventFactory(self.ns, self.sns_producer, self.identifier_key, self.store)
        event_info = EventInfo(event_type)
        return event_factory.create(event_info, **kwargs)
