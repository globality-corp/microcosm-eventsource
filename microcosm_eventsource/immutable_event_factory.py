"""
Factory for immutable events.

In extension of EventFactory, this class provides a native support of event updating container objects based
on event types

"""

from microcosm_eventsource.factory import EventFactory


class ContainerMutatorEventFactory(EventFactory):

    def __init__(
            self,
            event_store,
            container_store,
            default_ns=None,
            identifier_key=None,
            publish_event_pubsub=True,
            publish_model_pubsub=False,
    ):
        super().__init__(
            event_store=event_store,
            default_ns=default_ns,
            identifier_key=identifier_key,
            publish_event_pubsub=publish_event_pubsub,
            publish_model_pubsub=publish_model_pubsub,
        )
        self.container_store = container_store

    def create_event(self, event_info, skip_publish=False, **kwargs):
        super().create_event(
            event_info=event_info,
            skip_publish=skip_publish,
            **kwargs)
        self._update_container(event_info.event)

    def _update_container(self, event):
        # This is the same logic we use in the store meta class:
        # https://github.com/globality-corp/microcosm-eventsource/blob/develop/microcosm_eventsource/models/meta.py#L75
        container_identifier = getattr(event, f"{event.__container__.__tablename__}_id")
        container = self.container_store.retrieve(identifier=container_identifier)

        event.event_type.update_container_from_event(container, event)
        self.container_store.update(identifier=container_identifier, new_instance=container)
