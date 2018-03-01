"""
RolledUp event model.

"""


class RollUp:
    """
    Wrap a container and an event into rolled up state.

    The expectation is that attribute access will primarily refer to the container
    object and that specific event attributes will be exposed via @property syntax.

    """
    def __init__(self, event, container, aggregate, *args):
        """
        Roll ups are constructed with a single row from a roll up store query.

        By default, every row will contain:

        :param event: the most recent event in the roll up
        :param container: the container of the rolled up events
        :param aggregate: any other aggregates (window functions) across rolled up events

        """
        self._event = event
        self._container = container
        for key, value in aggregate.items():
            setattr(self, f"_{key}", value)

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
