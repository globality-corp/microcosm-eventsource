"""
Factory for events.

Allows event creation logic to be decoupled from controllers.

"""
from inflection import camelize
from microcosm_flask.conventions.encoding import with_context
from microcosm_flask.naming import name_for
from microcosm_flask.operations import Operation
from microcosm_pubsub.conventions import created
from werkzeug.exceptions import UnprocessableEntity


class EventInfo(object):
    """
    Encapsulate information needed to create an event.

    """
    def __init__(self, ns, sns_producer, event_type, parent=None):
        self.ns = ns
        self.sns_producer = sns_producer
        self.event_type = event_type
        self.parent = parent
        self.version = None
        self.state = None
        self.event = None

    def publish_event(self, media_type, **kwargs):
        """
        Publish that an event occurred so that other services can react.

        """
        uri = self.ns.url_for(Operation.Retrieve, **kwargs)
        self.sns_producer.produce(
            media_type=media_type,
            uri=uri,
        )


class EventFactory(object):
    """
    Base class for creating an event.

    """
    def __init__(self, event_store, default_ns=None, identifier_key=None):
        self.event_store = event_store
        self.default_ns = default_ns
        self.identifier_key = identifier_key

    def create(self, ns, sns_producer, event_type, **kwargs):
        """
        Create an event, validating the underlying state machine.

        """
        event_info = EventInfo(ns or self.default_ns, sns_producer, event_type)
        self.validate_required_fields(event_info, **kwargs)
        event_info.parent = self.event_store.retrieve_most_recent(**kwargs)
        self.create_transition(event_info, **kwargs)
        return event_info.event

    def create_transition(self, event_info, **kwargs):
        """
        Process an event state transition.

        This function allows chaining of state transitions by feeding the output
        of `create_event` with another event type.

        :raises: IllegalStateTransitionError

        """
        self.process_state_transition(event_info)
        self.create_event(event_info, **kwargs)

    def validate_required_fields(self, event_info, **kwargs):
        """
        Validate type-specific required fields.

        """
        missing_required_fields = [
            required_field
            for required_field in event_info.event_type.value.requires
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
                                event_info.event_type.name,
                                camelize(required_field, uppercase_first_letter=False),
                            )
                        ],
                    }
                    for required_field in missing_required_fields
                ],
            )

    def process_state_transition(self, event_info):
        """
        Process a state transition.

        Determines whether a state transition is legal, what version to use, and what state
        to persist.

        """
        state = set(event_info.parent.state) if event_info.parent else set()
        event_info.event_type.validate_transition(state)
        event_info.state = event_info.event_type.accumulate_state(state)
        parent_version = event_info.parent.version if event_info.parent else None
        event_info.version = event_info.event_type.next_version(parent_version)

    def create_event(self, event_info, **kwargs):
        """
        Create the event and publish that it was created.

        If the event has a parent id, uses an upsert to handle concurrent operations
        that produce the *same* event.

        """
        parent_id = None if event_info.parent is None else event_info.parent.id

        instance = self.event_store.model_class(
            event_type=event_info.event_type,
            parent_id=parent_id,
            state=event_info.state,
            version=event_info.version,
            **kwargs
        )

        event_info.event = self.create_instance(event_info, instance)

        event_info.publish_event(
            media_type=self.make_media_type(event_info),
            **self.make_uri_kwargs(event_info)
        )

    def create_instance(self, event_info, instance):
        if event_info.parent is None:
            return self.event_store.create(instance)
        else:
            return self.event_store.upsert_on_parent_id(instance)

    def make_media_type(self, event_info):
        return created("{}.{}".format(
            name_for(self.event_store.model_class),
            event_info.event.event_type.name,
        ))

    def make_uri_kwargs(self, event_info):
        uri_kwargs = dict(_external=True)
        uri_kwargs[self.identifier_key] = event_info.event.id
        return uri_kwargs
