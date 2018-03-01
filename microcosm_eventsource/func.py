"""
Custom database functions.

"""
from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.ext.compiler import compiles


class last(FunctionElement):
    """
    Define a SQLAlchemy function that maps to a postgres function to select the last non-null value in window.

    """
    name = "last"

    @classmethod
    def over_(cls, column):
        event = column.class_
        return cls(column).over(
            order_by=event.clock.asc(),
            partition_by=event.container_id,
        )

    @staticmethod
    def create(connection):
        """
        Create the postgres functions for this implementation.

        XXX Not currently called in any automatic way.

        """
        connection.execute("""
          CREATE OR REPLACE FUNCTION last_agg_sfunc (state anyelement, value anyelement)
                 RETURNS anyelement
                 LANGUAGE SQL
                 IMMUTABLE
          AS $$
            SELECT coalesce(value, state);
          $$;
        """)
        connection.execute("""
          CREATE AGGREGATE last_agg (anyelement) (
                SFUNC = last_agg_sfunc,
                STYPE = anyelement
          );
        """)

    @staticmethod
    def drop(connection):
        """
        Drop the postgres functions for this implementation.


        XXX Not currently called in any automatic way.
        """
        connection.execute("DROP AGGREGATE IF EXISTS last_agg (anyelement);")
        connection.execute("DROP FUNCTION IF EXISTS last_agg_sfunc (state anyelement, value anyelement);")


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
