"""
Rolled up event store.

"""
from sqlalchemy.orm import aliased
from sqlalchemy.orm.exc import NoResultFound

from microcosm_eventsource.func import ranked
from microcosm_eventsource.models.rollup import RollUp
from microcosm_postgres.context import SessionContext
from microcosm_postgres.errors import ModelNotFoundError


class RollUpStore:

    def __init__(self, container_store, event_store, rollup=RollUp):
        self.container_store = container_store
        self.event_store = event_store
        self.rollup = rollup

    @property
    def model_class(self):
        return self.rollup

    @property
    def container_type(self):
        return self.container_store.model_class

    @property
    def event_type(self):
        return self.event_store.model_class

    def retrieve(self, identifier):
        """
        Retrieve a single rolled-up event.

        """
        container = self._retrieve_container(identifier)
        aggregate = self._aggregate()

        try:
            return self._to_model(
                aggregate,
                *self._filter(
                    self._rollup_query(
                        container,
                        aggregate,
                    ),
                    aggregate
                ).one(),
            )
        except NoResultFound as error:
            raise ModelNotFoundError(
                "{} not found".format(
                    self.container_type.__name__,
                ),
                error,
            )

    def count(self, **kwargs):
        """
        Query the number of possible rolled-up rows.

        Note that this count avoids joining across the event store; this logic works as long as
        every container row has at least one event row; we consider this a best practice.
        For the exact results - use exact_count.

        """
        return self.container_store.count(**kwargs)

    def exact_count(self, **kwargs):
        """
        Query the number of possible rolled-up rows.

        Note that this count joins across the event store - and costs more to calculate.

        """
        return self._search_query(**kwargs).count()

    def search(self, **kwargs):
        """
        Implement a rolled-up search of containers by their most recent event.

        """
        aggregate = self._aggregate(**kwargs)
        return [
            self._to_model(aggregate, *row)
            for row in self._search_query(aggregate, **kwargs).all()
        ]

    def _search_query(self, aggregate=None, limit=None, offset=None, **kwargs):
        """
        Create the query for a rolled-up search of containers by their most recent event.

        Attempt to use the container object's store's filtering to limit the number of events
        that needs to be rolled up.

        """
        # SELECT
        #   <event.*>,
        #   <container.*>,
        #   rank...
        #   FROM (
        #     SELECT
        #       <event.*>,
        #       rank() OVER (
        #         PARTITION BY <event>.<container_id> ORDER BY <event>.clock DESC
        #       ) as rank
        #       ...
        #       FROM <event>
        #       JOIN (
        #         SELECT <container.*>
        #           FROM <container>
        #          WHERE <filter>
        #       ) as <container>
        #         ON <container>.id = <event>.<container_id>
        #      ORDER BY <order>
        #   )
        #   WHERE rank = 1
        #   LIMIT <limit> OFFSET <offset>

        # N.B. this method will handle the limit and offset instead of passing it down the
        # container store. This is to prevent unexpected results where the the number of
        # results returned from this method does not match the limit provided

        container = self._search_container(**kwargs)
        aggregate = aggregate or self._aggregate(**kwargs)
        query = self._filter(
            self._rollup_query(
                container,
                aggregate,
                **kwargs
            ),
            aggregate,
            **kwargs
        )

        if offset is not None:
            query = query.offset(offset)
        if limit is not None:
            query = query.limit(limit)

        return query

    def _retrieve_container(self, identifier):
        """
        Generate a subquery against the container type, filtering by identifier.

        """
        # SELECT * FROM <container> WHERE id = <identifier>
        return self._container_subquery(
            self.container_store._query().filter(
                self.container_type.id == identifier,
            )
        )

    def _search_container(self, **kwargs):
        """
        Generate a subquery against the container type, filtering by kwargs.

        """
        # SELECT * FROM <container> WHERE <filter>
        return self._container_subquery(
            self.container_store._filter(
                self.container_store._order_by(
                    self.container_store._query(),
                ),
                **kwargs
            )
        )

    def _container_subquery(self, query):
        """
        Wrap a container query so that it can be used in an aggregation.

        If the container class is polymorphic, then the operation reduces columns - in case
        of redundant names (e.g. joined table inheritance), otherwise operation explicitly maps
        the subquery to the table name so that ordering can be applied in `_rollup_query` without modifying
        the container store. This operation further wraps the subquery in an alias
        to the container type so that it can be referenced in `_rollup_query` as an entity.

        """
        if self._is_joined_polymorphic():
            subquery = query.subquery(
                reduce_columns=True,
                with_labels=True,
            )
        else:
            subquery = query.subquery(
                self.container_type.__tablename__,
            )

        return aliased(self.container_type, subquery)

    def _aggregate(self, **kwargs):
        """
        Emit a dictionary of window functions by which to aggregate events.

        By default, `_aggregate` performs a rank by clock values to pick the most recent row per logical time.

        """
        # rank() OVER (partition by <container_id> order by clock desc)
        # ... <possibly more>
        return dict(
            rank=ranked(self.event_type),
        )

    def _rollup_query(self, container, aggregate, **kwargs):
        """
        Query for events and aggregates that match the container subquery.

        We expect that the number of (filtered) container rows (e.g. via a LIMIT statement) to be much less than
        the number of total event rows. Note that if we were to query by events first and then by the container
        type, we might select many more rows.

        The query applies `from_self` so that aggregates can be legally used in the `_filter` WHERE clause.
        (Window functons are otherwise not allowed in WHERE because they are applied after other conditions.)

        """
        # SELECT *
        #   FROM (
        #     SELECT <event.*>, <container.*>, rank
        #       FROM <event_type>
        #       JOIN <container_type> ON <container_type>.id = <event_type>.container_id
        #      ORDER BY <order>
        #   )
        return self.container_store._order_by(
            self._query(
                container,
                aggregate,
            ),
            **kwargs
        ).from_self()

    def _query(self, container, aggregate):
        """
        Query events, containers, and aggregates together.

        """
        query = SessionContext.session.query(
            self.event_type,
            container,
        ).add_columns(
            *aggregate.values(),
        ).join(
            container,
            container.id == self.event_type.container_id,
        )

        if self._is_joined_polymorphic():
            return query.join(
                # extra join for reusing ordinary `_order_by` from container_store
                self.container_type,
                self.container_type.id == self.event_type.container_id,
            )

        return query

    def _filter(self, query, aggregate, **kwargs):
        """
        Filter by aggregates.

        By default, selects only events with the top rank (e.g. most recent clock).

        """
        return query.filter(
            aggregate["rank"] == 1,
        )

    def _to_model(self, aggregate, event, container, *args):
        keys = aggregate.keys()
        values = args[:len(keys)]
        return self.rollup(
            event,
            container,
            {
                key: value
                for (key, value) in zip(keys, values)
            },
            *args[len(keys):]
        )

    def _is_joined_polymorphic(self):
        if hasattr(self.container_type, "__mapper_args__"):
            return all([
                # joined polymorphic entity must have a polymorphic_identity set
                "polymorphic_identity" in self.container_type.__mapper_args__,
                # and there must exist foreign key on the identifier
                hasattr(self.container_type.id, "foreign_keys"),
            ])

        return False
