"""
Factory for immutable events.

In extension of EventFactory, this class provides a native support of event updating container objects based
on event types

"""
from collections import defaultdict

from microcosm_eventsource.factory import EventFactory


container_common_mutator_registry = defaultdict(list)
container_event_specific_mutator_registry = defaultdict(list)


def register_container_mutator_common(event_type):
    def decorator(func):
        container_common_mutator_registry[event_type.__name__].append(func.__name__)
        return func

    return decorator


def register_container_mutator_by_event_type(event_instance_type):
    def decorator(func):
        container_event_specific_mutator_registry[event_instance_type.name].append(func.__name__)
        return func

    return decorator


def get_specific_handler_names_for_event_type(event):
    return container_event_specific_mutator_registry[event.event_type.name]


def get_common_handler_names_for_event(event):
    event_type_name = f"{event.__class__.__container__.__name__}Event"
    return container_common_mutator_registry[event_type_name]


class ImmutableEventFactory(EventFactory):
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

        for common_handler_name in get_common_handler_names_for_event(event):
            getattr(self, common_handler_name)(container, event)

        for event_specific_handler_name in get_specific_handler_names_for_event_type(event):
            getattr(self, event_specific_handler_name)(container, event)

        self.container_store.update(identifier=container_identifier, new_instance=container)
