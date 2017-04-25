"""
Event store.

"""
from microcosm_postgres.store import Store
from sqlalchemy.dialects.postgresql import insert

from microcosm_eventsource.errors import ConcurrentStateConflictError


class EventStore(Store):
    """
    Event persistence operations.

    """
    def retrieve_most_recent(self, **kwargs):
        """
        Retrieve the most recent by container id and event type.

        """
        container_id = kwargs.pop(self.model_class.container_id_name)
        return self._retrieve_most_recent(
            self.model_class.container_id == container_id,
        )

    def retrieve_most_recent_by_event_type(self, event_type, **kwargs):
        """
        Retrieve the most recent by container id and event type.

        """
        container_id = kwargs.pop(self.model_class.container_id_name)
        return self._retrieve_most_recent(
            self.model_class.container_id == container_id,
            self.model_class.event_type == event_type,
        )

    def upsert_on_parent_id(self, instance):
        """
        Upsert an event by parent id.

        Uses ON CONFLICT ... DO NOTHING to handle uniqueness constraint violations without
        invalidating the current transactions completely.

        Depends on the parent_id unique constraint to find the resulting entry.

        """
        with self.flushing():
            insert_statement = insert(self.model_class).values(
                instance._members(),
            )
            upsert_statement = insert_statement.on_conflict_do_nothing(
                index_elements=["parent_id"],
            )
            self.session.execute(upsert_statement)

        most_recent = self._retrieve_most_recent(
            self.model_class.parent_id == instance.parent_id,
        )
        if not most_recent.is_similar_to(instance):
            raise ConcurrentStateConflictError()

        return most_recent

    def _filter(self,
                query,
                event_type=None,
                clock=None,
                min_clock=None,
                max_clock=None,
                parent_id=None,
                version=None,
                **kwargs):
        """
        Filter events by standard criteria.

        """
        container_id = kwargs.pop(self.model_class.container_id_name, None)
        if container_id is not None:
            query = query.filter(self.model_class.container_id == container_id)
        if event_type is not None:
            query = query.filter(self.model_class.event_type == event_type)
        if clock is not None:
            query = query.filter(self.model_class.clock == clock)
        if min_clock is not None:
            query = query.filter(self.model_class.clock >= min_clock)
        if max_clock is not None:
            query = query.filter(self.model_class.clock <= max_clock)
        if parent_id is not None:
            query = query.filter(self.model_class.parent_id == parent_id)
        if version is not None:
            query = query.filter(self.model_class.version == version)

        return super(EventStore, self)._filter(query, **kwargs)

    def _order_by(self, query, **kwargs):
        """
        Order events by logical clock.

        """
        return query.order_by(
            self.model_class.container_id.desc(),
            self.model_class.clock.desc(),
        )

    def _retrieve_most_recent(self, *criterion):
        """
        Retrieve the most recent event by some criterion.

        Note that the default `_order_by` enforces clock ordering.

        """
        return self._order_by(self._query(
            *criterion
        )).first()
