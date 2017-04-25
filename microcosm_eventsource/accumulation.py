"""
Accumulation functions.

"""


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
    return lambda state, event_type: [other_event_type]


def union():
    """
    Return the aggregated event type.

    """
    return lambda state, event_type: sorted(state | {event_type, })
