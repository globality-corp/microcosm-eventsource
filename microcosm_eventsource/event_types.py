"""
Event type enumerations.

An event type defines a unique event and the logic for evaluating state transitions using
a DNF mini grammar:

 -  An event can *follow* a disjuction of following conditions
 -  A following condition can match an event type exactly
 -  A following condition can match a conjunction of following conditions
 -  A following condition can match the negation of a following conditions

This matching logic depends is applied again *state* accumulation. By default, event state
**DOES NOT** accumulate; but individual event types may choose to accumulate via configuration.

"""
from enum import Enum


def id_(name):
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
    return id_(value)


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
    def __init__(self, follows=(), accumulating=False, restarting=False, requires=()):
        """
        :params follows:    an iterable (disjunction) of following conditions
        :param accumulating:  whether the event type should accumulating state
        :param restarting: whether the event type may restart a (new) version

        """
        self.follows = frozenset(normalize(value) for value in follows)
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
    def requires(cls, column_name):
        return [
            event_type
            for event_type in cls
            if column_name in event_type.value.requires
        ]

    def is_legal_after(self, parent):
        """
        Can this event type follow the given parent?

        Uses a mini grammar based on the `EventTypeInfo` state. More complex grammar may be
        necessary in the future (e.g. negation).

        """
        # only initial event types may come first
        if parent is None:
            return self.is_initial

        # otherwise, inspect state
        state = {event_type for event_type in parent.state}

        # `follows` defines a disjuction of conjuctions
        for condition in self.value.follows:
            if condition(self.__class__, state):
                return True
        return False

    def __lt__(self, other):
        return self.name < other.name

    def __str__(self):
        return self.name


def info(*args, **kwargs):
    return EventTypeInfo(*args, **kwargs)
