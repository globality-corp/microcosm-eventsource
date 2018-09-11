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


class EventInfo:
    """
    Encapsulate information needed to create an event.

    """
    def __init__(self, ns, sns_producer, event_type, parent=None, version=None):
        self.ns = ns
        self.sns_producer = sns_producer
        self.event_type = event_type
        self.parent = parent
        self.version = version
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


class EventFactory:
    """
    Base class for creating an event.

    """
    def __init__(
        self,
        event_store,
        default_ns=None,
        identifier_key=None,
        publish_event_pubsub=True,
        publish_model_pubsub=False,
    ):
        self.event_store = event_store
        self.default_ns = default_ns
        self.identifier_key = identifier_key
        self.publish_event_pubsub = publish_event_pubsub
        self.publish_model_pubsub = publish_model_pubsub

    def create(self, ns, sns_producer, event_type, parent=None, version=None, **kwargs):
        """
        Create an event, validating the underlying state machine.

        """
        event_info = EventInfo(ns or self.default_ns, sns_producer, event_type, parent, version)
        self.validate_required_fields(event_info, **kwargs)
        self.validate_transition(event_info, **kwargs)
        if not event_info.parent:
            event_info.parent = self.event_store.retrieve_most_recent(**kwargs)
        self.create_transition(event_info, **kwargs)
        self.create_auto_transition_event(ns, sns_producer, parent=event_info.event, version=version, **kwargs)
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

    def create_auto_transition_event(self, ns, sns_producer, parent, **kwargs):
        """
        Creates the next auto-transition event if exist

        """
        auto_transition_events = [
            event_type for event_type in parent.event_type.auto_transition_events()
            if event_type.may_transition(parent.state)
        ]
        if not auto_transition_events:
            return
        self.create(ns, sns_producer, event_type=auto_transition_events[0], parent=parent, **kwargs)

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
                            ),
                        ],
                    }
                    for required_field in missing_required_fields
                ],
            )

    def validate_transition(self, event_info, **kwargs):
        """
        Allows implementations of event source to define custom validation.

        """
        pass

    def process_state_transition(self, event_info):
        """
        Process a state transition.

        Determines whether a state transition is legal, what version to use, and what state
        to persist.

        """
        state = set(event_info.parent.state) if event_info.parent else set()
        event_info.event_type.validate_transition(state)
        event_info.state = event_info.event_type.accumulate_state(state)
        if event_info.version is not None:
            return
        parent_version = event_info.parent.version if event_info.parent else None
        event_info.version = event_info.event_type.next_version(parent_version)

    def create_event(self, event_info, skip_publish=False, **kwargs):
        """
        Create the event and publish that it was created.

        We may wish to skip event publishing in certain cases (e.g. during batch
        event operations)

        If the event has a parent id, uses an upsert to handle concurrent operations
        that produce the *same* event.

        """
        parent_id = None if event_info.parent is None else event_info.parent.id

        # NB: setting the id here so that it can easily be mocked in tests
        instance = self.event_store.model_class(
            id=self.event_store.new_object_id(),
            event_type=event_info.event_type,
            parent_id=parent_id,
            state=event_info.state,
            version=event_info.version,
            **kwargs
        )

        event_info.event = self.create_instance(event_info, instance)

        if not skip_publish:
            self.publish_event(event_info)

    def create_instance(self, event_info, instance):
        if event_info.parent is None:
            return self.event_store.create(instance)
        else:
            return self.event_store.upsert_on_index_elements(instance)

    def publish_event(self, event_info):
        """
        Publish a Created(Model.EventType) and / or Created(Model) pubsub messages.
        Set by publish_event_pubsub and publish_model_pubsub

        """
        uri_kwargs = self.make_uri_kwargs(event_info)
        if self.publish_event_pubsub:
            event_info.publish_event(
                media_type=self.make_media_type(event_info),
                **uri_kwargs,
            )
        if self.publish_model_pubsub:
            event_info.publish_event(
                media_type=self.make_media_type(event_info, True),
                **uri_kwargs,
            )

    def make_media_type(self, event_info, discard_event_type=False):
        if discard_event_type:
            return created("{}".format(
                name_for(self.event_store.model_class),
            ))
        return created("{}.{}".format(
            name_for(self.event_store.model_class),
            event_info.event.event_type.name,
        ))

    def make_uri_kwargs(self, event_info):
        uri_kwargs = dict(_external=True)
        uri_kwargs[self.identifier_key] = event_info.event.id
        return uri_kwargs
