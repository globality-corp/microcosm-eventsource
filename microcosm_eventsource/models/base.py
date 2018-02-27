"""
Event base class.

"""


class BaseEvent:

    def is_similar_to(self, other):
        """
        Are two events similar enough to activate upserting?

        """
        return all((
            self.event_type == other.event_type,
            self.parent_id == other.parent_id,
            self.container_id == other.container_id
        ))
