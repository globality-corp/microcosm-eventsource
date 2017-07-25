"""
Accumulation functions.

"""
from enum import Enum


def as_enum(value, event_type):
    if isinstance(value, Enum):
        return value
    return event_type.__class__[value]


def current():
    """
    Return the current event type.

    """
    return lambda state, event_type: [event_type]


def keep():
    """
    Keep the current state.

    """
    return lambda state, event_type: state


def alias(other_event_type):
    """
    Return another event type.

    """
    return lambda state, event_type: [as_enum(other_event_type, event_type)]


def difference(other_event_type):
    """
    Remove one event type

    """
    return lambda state, event_type: sorted(state - {as_enum(other_event_type, event_type), })


def union():
    """
    Return the aggregated event type.

    """
    return lambda state, event_type: sorted(state | {event_type, })
