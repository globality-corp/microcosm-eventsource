"""
Factory for immutable events.

In extension of EventFactory, this class provides a native support of event updating container objects based
on event types

"""
from typing import Dict

from microcosm_eventsource.factory import EventFactory


_common_container_mutator_registry: Dict[str, str] = {}
_event_specific_container_mutator_registry: Dict[str, str] = {}


class DuplicateEventHandlerRegistrationAttempted(Exception):
    pass


def common_container_mutator(event_type):
    def decorator(func):
        if event_type.__name__ in _common_container_mutator_registry:
            raise DuplicateEventHandlerRegistrationAttempted(
                "Mutator is already registered for event type: %s" % event_type.__name__)
        _common_container_mutator_registry[event_type.__name__] = func.__name__
        return func

    return decorator


def event_specific_container_mutator(event_instance_type):
    def decorator(func):
        if event_instance_type.name in _event_specific_container_mutator_registry:
            raise DuplicateEventHandlerRegistrationAttempted(
                "Mutator is already registered for event instance type: %s" % event_instance_type.name)
        _event_specific_container_mutator_registry[event_instance_type.name] = func.__name__
        return func

    return decorator


def get_specific_handler_name_for_event_type(event):
    return _event_specific_container_mutator_registry.get(event.event_type.name)


def get_common_handler_name_for_event(event):
    event_type_name = f"{event.__class__.__container__.__name__}Event"
    return _common_container_mutator_registry.get(event_type_name)


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

        common_handler_name = get_common_handler_name_for_event(event)
        if common_handler_name and hasattr(self, common_handler_name):
            getattr(self, common_handler_name)(container, event)

        event_specific_handler_name = get_specific_handler_name_for_event_type(event)
        if event_specific_handler_name and hasattr(self, event_specific_handler_name):
            getattr(self, event_specific_handler_name)(container, event)

        self.container_store.update(identifier=container_identifier, new_instance=container)
