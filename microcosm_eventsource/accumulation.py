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


def addition(*other_event_types):
    """
    Add event types

    """
    def _addition(state, event_type):
        to_add = {
            as_enum(other_event_type, event_type)
            for other_event_type in other_event_types
        }
        return sorted(state | to_add)
    return _addition


def difference(*other_event_types):
    """
    Remove event types

    """
    def _difference(state, event_type):
        to_add = {
            as_enum(other_event_type, event_type)
            for other_event_type in other_event_types
        }
        return sorted(state - to_add)
    return _difference


def compose(*funcs):
    """
    Compose multiple accumulations.

    """
    def _compose(state, event_type):
        new_state = state
        for func in funcs:
            new_state = func(set(new_state), event_type)
        return new_state
    return _compose


def union():
    """
    Return the aggregated event type.

    """
    return lambda state, event_type: sorted(state | {event_type, })
