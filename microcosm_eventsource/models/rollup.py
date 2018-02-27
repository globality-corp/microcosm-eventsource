"""
RolledUp event model.

"""


class RollUp:
    """
    Wrap a container and an event into rolled up state.

    The expectation is that attribute access will primarily refer to the container
    object and that specific event attributes will be exposed via @property syntax.

    """
    def __init__(self, event, container, rank, *args, **kwargs):
        self._event = event
        self._container = container
        self._rank = rank

    @property
    def latest_event_type(self):
        """
        Expose the most recent event type.

        """
        return self._event.event_type

    def __getattr__(self, key):
        """
        By default, resolve attributes to the container.

        """
        return getattr(self._container, key)
