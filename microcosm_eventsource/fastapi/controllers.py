"""
Async Event controllers.

"""
from microcosm_fastapi.conventions.crud_adapter import CRUDStoreAdapter
from microcosm_fastapi.naming import name_for

from microcosm_eventsource.fastapi.factory import EventFactoryAsync
from fastapi import Request


class EventController(CRUDStoreAdapter):

    def __init__(self, graph, store):
        super().__init__(graph, store)
        self.sns_producer = graph.sns_producer

    @property
    def identifier_key(self):
        return "{}_id".format(name_for(self.store.model_class))

    @property
    def event_factory(self):
        """
        By default, create a new event factory.

        """
        return EventFactoryAsync(
            event_store=self.store,
            identifier_key=self.identifier_key,
        )

    async def create(self, request: Request, event_type, session, **kwargs):
        """
        Delegate to the event factory.

        """
        return await self.event_factory.create(request, self.ns, self.sns_producer, event_type, session=session, **kwargs)