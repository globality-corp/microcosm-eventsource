"""
Event type enumerations.

As events are processed, they accumulate a `state` as a list of event types, using
a pluggable rule system:

 -  The state can be the current event: `current()`
 -  The state can be a union of the current event and the current state: `union()`
 -  The state can be another state: `alias()`

State machine transitions are defined using a DNF-compatible mini-language that processes
this accumulated `state` and the current event:

 -  An event can legally follow another another event: `event(name)`
 -  An event can legally follow a disjunction of conditions: `any_of(...)`
 -  An event can legally follow a conjunction of conditions: `all_of(...)`
 -  An event can legally follow a negation of a conditon: `but_not(...)`

"""
from enum import Enum
from itertools import chain

from microcosm_eventsource.errors import (
    IllegalInitialStateError,
    IllegalStateTransitionError,
)
from microcosm_eventsource.accumulation import current
from microcosm_eventsource.transitioning import nothing


class EventTypeInfo:
    """
    Event type meta data.

    """
    def __init__(self, follows=None, accumulate=None, restarting=False, requires=(), autotransition=False):
        """
        :param follows:         an instance of the event mini-grammar
        :param accumulate:      whether the event type should accumulating state
        :param restarting:      whether the event type may restart a (new) version
        :param requires:        validate type-specific required fields
        :param autotransition:  whether this event should be autogenereated when possible

        """
        self.follows = follows if follows is not None else nothing()
        self.accumulate = accumulate if accumulate is not None else current()
        self.restarting = restarting
        self.requires = requires
        self.autotransition = autotransition


class EventType(Enum):
    """
    Based event type enum.

    """
    @property
    def is_initial(self):
        """
        Can this event type be used for an initial event?

        """
        return not bool(self.value.follows)

    @property
    def is_accumulating(self):
        """
        Does this event type accumulate state?

        """
        return self.value.accumulating

    @property
    def is_restarting(self):
        """
        Does this event type restart a new version?

        """
        return self.value.restarting

    @property
    def is_autotransition(self):
        """
        Does this event type would be autogenerated if possible?

        """
        return self.value.autotransition

    @classmethod
    def autotransition_events(cls):
        """
        Which events are autotransition events?

        """
        return [event_type for event_type in cls if event_type.is_autotransition]

    @classmethod
    def required_column_names(cls):
        return {
            column_name
            for event_type in cls
            for column_name in event_type.value.requires
        }

    @classmethod
    def requires(cls, column_name):
        return [
            event_type
            for event_type in cls
            if column_name in event_type.value.requires
        ]

    def may_transition(self, state):
        """
        Can this event type transition from the given state?

        :param state: a set of event types

        """
        return self.value.follows(self.__class__, state)

    @classmethod
    def available_transitions(cls, state):
        """
        To which events the given state may transition?

        :param state: a set of event types

        """
        return [event_type for event_type in cls if event_type.may_transition(state)]

    def validate_transition(self, state):
        """
        Asssert that a transition is legal from the given state.

        :param state: a set of event types

        """
        if self.may_transition(state):
            return

        if not state:
            # event may not be initial
            raise IllegalInitialStateError("Event type '{}' may not be initial".format(
                self.name,
            ))

        # event may not follow previous
        raise IllegalStateTransitionError("Event type '{}' may not follow [{}]".format(
            self.name,
            ", ".join(event_type.name for event_type in state),
        ))

    def accumulate_state(self, state):
        """
        Accumulate state.

        """
        return self.value.accumulate(state, self)

    def next_version(self, version):
        """
        Compute next version.

        """
        if version is None:
            return 1
        if self.is_restarting:
            return version + 1
        return version

    @classmethod
    def all_states(cls):
        """
        All states that the state machine can reach

        """
        # Prefer to use frozenset and not set to represent states in this context
        # It will allow us easly ignore for duplications
        intial_states = [frozenset((event_type,)) for event_type in cls if event_type.is_initial]
        next_states = intial_states
        known_states = set(intial_states)

        while True:
            if not next_states:
                return known_states
            current_states = next_states
            next_states = []
            for state in current_states:
                for event_type in cls.available_transitions(state):
                    new_state = frozenset(event_type.accumulate_state(state))
                    if new_state in known_states:
                        continue
                    next_states.append(new_state)
                    known_states.add(new_state)

    @classmethod
    def has_only_valid_transiations(cls):
        """
        Check that the state machine has only valid transiations:
        * only one (or zero) autotransition event can follow any state. No other event may follow.

        """
        for state in cls.all_states():
            auto_transition_events = [
                event_type for event_type in cls.autotransition_events()
                if event_type.may_transition(state)
            ]
            if auto_transition_events and len(cls.available_transitions(state)) > 1:
                return False
        return True

    def __lt__(self, other):
        return self.name < other.name

    def __str__(self):
        return self.name


def event_info(*args, **kwargs):
    return EventTypeInfo(*args, **kwargs)


def EventTypeUnion(name, *event_types):
    """
    Create a new `EventType` as a union of other enums.

    """
    return EventType(name, [(item.name, item.value) for item in chain(*event_types)])
