"""
Accumulation functions.

"""


def current():
    """
    Return the current event type.

    """
    return lambda state, event_type: [event_type]


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
