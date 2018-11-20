"""
Custom database functions.

"""
from pkg_resources import resource_string

from microcosm_postgres.models import Model
from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import DDL, func
from sqlalchemy.event import listen


def load_ddl(name, action):
    return resource_string("microcosm_eventsource", f"ddl/{name}.{action}.ddl").decode("utf-8")


# register custom DDL when calling `create_all` or `drop_all`
listen(
    Model.metadata,
    "after_create",
    DDL(
        load_ddl("array_sort_unique", "create") +
        load_ddl("proc_events_create", "create") +
        load_ddl("proc_events_delete", "create") +
        load_ddl("proc_event_type_delete", "create") +
        load_ddl("last_agg_sfunc", "create") +
        load_ddl("last_agg", "create") +
        load_ddl("proc_event_type_replace", "create")
    ),
)
listen(
    Model.metadata,
    "after_drop",
    DDL(
        load_ddl("array_sort_unique", "drop") +
        load_ddl("proc_events_create", "drop") +
        load_ddl("proc_events_delete", "drop") +
        load_ddl("proc_event_type_delete", "drop") +
        load_ddl("last_agg", "drop") +
        load_ddl("last_agg_sfunc", "drop") +
        load_ddl("proc_event_type_replace", "drop")
    ),
)


class last(FunctionElement):
    """
    Define a SQLAlchemy function that maps to a postgres function to select the last non-null value in window.

    """
    name = "last"

    @classmethod
    def of(cls, column, *filter_by):
        """
        Generate a window function over an event's column.

        """
        event = column.class_
        return cls(column).filter(*filter_by).over(
            order_by=event.clock.asc(),
            partition_by=event.container_id,
        )


@compiles(last)
def compile(element, compiler, **kwargs):
    """
    By default, `last` is not defined.

    """
    raise NotImplementedError("last is only defined for postgresql")


@compiles(last, "postgresql")
def compile_postgres(element, compiler, **kwargs):
    """
    Define `last` in terms of the custom functions.

    """
    return f"last_agg({compiler.process(element.clauses)})"


def ranked(event_type):
    return func.rank().over(
        order_by=event_type.clock.desc(),
        partition_by=event_type.container_id,
    )
