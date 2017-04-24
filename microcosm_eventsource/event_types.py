"""
Event type enumerations.

An event type defines a unique event and the logic for evaluating state transitions using
a DNF-compatible mini-grammar:

 -  An event can legally follow another another event (`event(name)`)
 -  An event can legally follow a disjunction of conditions (`any_of(...)`)
 -  An event can legally follow a conjunction of conditions (`all_of(...)`)
 -  An event can legally follow a negation of a conditon (`but_not(...)`)

This matching logic is applied against accumulated event `state`, which will be an iterable
of the N most recent event type values, where N is determined by the most recent event type
that does not defines the attribute `accumulating=True`.


"""
from enum import Enum

from microcosm_eventsource.errors import (
    IllegalInitialStateError,
    IllegalStateTransitionError,
)


class Initial(object):

    def __call__(self, cls, state):
        return False

    def __bool__(self):
        return False

    __nonzero__ = __bool__


def event(name):
    """
    Mini grammar to match a specific event type.

    """
    return lambda cls, state: cls[name] in state


def normalize(value):
    """
    Normalize string values into id functions.

    """
    if callable(value):
        return value
    return event(value)


def any_of(*args):
    """
    Mini grammar to match a list of event types.

    """
    return lambda cls, state: any(normalize(item)(cls, state) for item in args)


def all_of(*args):
    """
    Mini grammar to match a list of event types.

    """
    return lambda cls, state: all(normalize(item)(cls, state) for item in args)


def but_not(func):
    """
    Mini grammar to not match a specific event type.

    """
    return lambda cls, state: not normalize(func)(cls, state)


class EventTypeInfo(object):
    """
    Event type meta data.

    """
    def __init__(self, follows=None, accumulating=False, restarting=False, requires=()):
        """
        :params follows:    an instance of the event mini-grammar
        :param accumulating:  whether the event type should accumulating state
        :param restarting: whether the event type may restart a (new) version

        """
        self.follows = follows or Initial()
        self.accumulating = accumulating
        self.restarting = restarting
        self.requires = requires


class EventType(Enum):
    """
    Based event type enum.

    """
    @property
    def is_initial(self):
        """
        Can this event type be the initial event?

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
        if not state:
            # only initial event types may come first
            return self.is_initial

        return self.value.follows(self.__class__, state)

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
        if self.is_accumulating:
            return sorted(state | {self, })
        else:
            return [self]

    def next_version(self, version):
        """
        Compute next version.

        """
        if version is None:
            return 1
        if self.is_restarting:
            return version + 1
        return version

    def __lt__(self, other):
        return self.name < other.name

    def __str__(self):
        return self.name


def event_info(*args, **kwargs):
    return EventTypeInfo(*args, **kwargs)
