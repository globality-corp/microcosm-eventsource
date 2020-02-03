"""
Factory for immutable events.

In extension of EventFactory, this class provides a native support of event updating container objects based
on event types

"""
from collections import defaultdict

container_event_mutator_registry = defaultdict(list)


def register_container_mutator_common(event_type):
    def decorator(func):
        container_event_mutator_registry[event_type.__name__].append(func.__name__)
        return func

    return decorator


def register_container_mutator_by_event_type(event_instance_type):
    def decorator(func):
        container_event_mutator_registry[event_instance_type.name].append(func.__name__)
        return func

    return decorator


def get_handler_names_for_event_type(event_type):
    return container_event_mutator_registry[event_type.name]


def get_common_handler_names_for_event(event_type):
    event_type_name = event_type.__class__.__name__.replace('Type', '')
    return container_event_mutator_registry[event_type_name]


class ImmutableEventFactory(EventFactory):
    def create(self, ns, sns_producer, event_type, parent=None, version=None, **kwargs):
        event = super().create(
            ns=ns,
            sns_producer=sns_producer,
            event_type=event_type,
            parent=parent,
            version=version,
            **kwargs
        )
        self._update_container(event)
        return event

    def _update_container(self, event):
        # This looks a little crazy this this is the same logic we use in the store meta class:
        # https://github.com/globality-corp/microcosm-eventsource/blob/develop/microcosm_eventsource/models/meta.py#L75
        container_identifier = getattr(event, f"{event.__container__.__tablename__}_id")
        container = self.container_store.retrieve(identifier=container_identifier)

        for common_handler_name in get_common_handler_names_for_event(event.event_type):
            getattr(self, common_handler_name)(container, event)

        for event_type_handler_name in get_handler_names_for_event_type(event.event_type):
            getattr(self, event_type_handler_name)(container, event)

        self.container_store.update(identifier=container_identifier, new_instance=container)
