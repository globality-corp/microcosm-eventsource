"""
Event controllers.

"""
from inflection import camelize

from microcosm_flask.conventions.crud_adapter import CRUDStoreAdapter
from microcosm_flask.conventions.encoding import with_context
from microcosm_flask.naming import name_for
from microcosm_flask.operations import Operation
from microcosm_pubsub.conventions import created
from werkzeug.exceptions import UnprocessableEntity

from microcosm_eventsource.errors import (
    IllegalInitialStateError,
    IllegalStateTransitionError,
)


class EventController(CRUDStoreAdapter):

    @property
    def sns_producer(self):
        return self.graph.sns_producer

    def create(self, event_type, **kwargs):
        """
        Create an event, validating the underlying state machine.

        """
        self.validate_required_fields(event_type, **kwargs)
        parent = self.store.retrieve_most_recent(**kwargs)
        version, state = self.process_state_transition(parent, event_type)
        event = self.create_event(parent, event_type, version, state, **kwargs)
        self.publish_event(event)
        return event

    def validate_required_fields(self, event_type, **kwargs):
        """
        Validate type-specific required fields.

        """
        missing_required_fields = [
            required_field
            for required_field in event_type.value.requires
            if kwargs.get(required_field) is None
        ]
        if missing_required_fields:
            raise with_context(
                UnprocessableEntity("Validation error"), [
                    {
                        "message": "Missing required field: '{}'".format(
                            camelize(required_field, uppercase_first_letter=False),
                        ),
                        "field": camelize(required_field, uppercase_first_letter=False),
                        "reasons": [
                            "Event type '{}' requires '{}'".format(
                                event_type.name,
                                camelize(required_field, uppercase_first_letter=False),
                            )
                        ],
                    }
                    for required_field in missing_required_fields
                ],
            )

    def process_state_transition(self, parent, event_type):
        """
        Process a state transition.

        Determines whether a state transition is legal, what version to use, and what state
        to persist.

        """
        if not event_type.is_legal_after(parent):
            if parent is None:
                # event may not be initial
                raise IllegalInitialStateError("Event type '{}' may not be initial".format(
                    event_type.name,
                ))
            else:
                # event may not follow previous
                raise IllegalStateTransitionError("Event type '{}' may not follow [{}]".format(
                    event_type.name,
                    ", ".join(item.name for item in parent.state),
                ))

        parent_version = parent.version if parent else 1
        parent_state = parent.state if parent else []

        if event_type.is_accumulating:
            state = parent_state + [event_type]
        else:
            state = [event_type]

        if event_type.is_restarting:
            version = parent_version + 1
        else:
            version = parent_version

        return version, state

    def create_event(self, parent, event_type, version, state, **kwargs):
        """
        Create the event.

        """
        # XXX we should upsert here as long as we can reason about the constraints
        return self.store.create(
            self.store.model_class(
                event_type=event_type,
                parent_id=parent.id if parent else None,
                state=state,
                version=version,
                **kwargs
            ),
        )

    def publish_event(self, new_event):
        """
        Publish that an event occurred so that other services can react.

        """
        uri_kwargs = dict(_external=True)
        uri_kwargs[self.identifier_key] = new_event.id
        uri = self.ns.url_for(Operation.Retrieve, **uri_kwargs)

        self.sns_producer.produce(
            media_type=created("{}.{}".format(
                name_for(self.store.model_class),
                new_event.event_type.name,
            )),
            uri=uri,
        )
