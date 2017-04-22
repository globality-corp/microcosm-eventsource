"""
Event model.

"""
from microcosm_postgres.models import Model
from microcosm_postgres.types import EnumType, Serial
from sqlalchemy import (
    CheckConstraint,
    Column,
    FetchedValue,
    ForeignKey,
    Index,
    Integer,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy_utils import UUIDType


# preserve the SQLAlchemy metaclass
MetaClass = type(Model)


class ColumnAlias(object):
    """
    Descriptor to reference a column by a well known alias.

    """
    def __init__(self, name):
        self.name = name

    def __get__(self, cls, owner):
        return getattr(cls or owner, self.name)

    def __set__(self, cls, value):
        return setattr(cls, self.name, value)


def default_state(context):
    return (context.current_parameters["event_type"],)


def join_event_types(event_types):
    return ",".join(
        "'{}'".format(event_type.name)
        for event_type in event_types
    )


class EventMeta(MetaClass):
    """
    Construct event models using a metaclass.

    SQLAlchemy's declarative base assumes that extending the base class (e.g. `Model`)
    declares a new table. As such, model subtypes are best defined with through mixins
    or metaclasses. In this case, a metaclass is more appropriate because we want some
    of the event attributes to be generated dynamically.

    """
    def __new__(cls, name, bases, dct):
        """
        Generate a new event type from the provided class.

        Requires that the class dictionary include the following declarations:

         -  `__container__` a reference to the container model class
         -  `__eventtype__` a reference to the event type enumeration
         -  `__tablename__` the usual SQLAlchemy table name

        """
        # add model to expected bases
        bases = bases + (Model,)

        # declare event columns and indexes
        dct.update(cls.make_declarations(
            cls,
            container_name=dct["__container__"].__tablename__,
            event_type=dct["__eventtype__"],
            table_name=dct["__tablename__"],
        ))

        return super(EventMeta, cls).__new__(cls, name, bases, dct)

    def make_declarations(cls, container_name, event_type, table_name):
        """
        Declare columns and indexes.

        An event assumes the following:

         -  Each event belongs to (and has a foreign key to) another "container" table.

            Typically, the container table has immutable rows that define the context of an entity
            and events track state changes to rows of this table.

         -  Each event has a well-defined type, derived from an enumeration.

         -  Each event has a well-defined state, consisting of one or more enumerated values.

         -  Each event has an integer version, starting from one.

            Not every event uses versions; those that do will generally have a uniqueness contraint
            on some event types per version of a container row.

         -  Each event has a nullable parent event id where a null value represents the first event
            in a version and subsequent event have a unique parent id to ensure semantic ordering.

         -  Each event has a non-nullable serial clock to ensure total ordering.

        """
        container_id = "{}.id".format(container_name)
        container_id_name = "{}_id".format(container_name)
        parent_id = "{}.id".format(table_name)

        return {
            # columns
            container_id_name: Column(UUIDType, ForeignKey(container_id), nullable=False),
            "event_type": Column(EnumType(event_type), nullable=False),
            "clock": Column(Serial, server_default=FetchedValue(), nullable=False, unique=True),
            "parent_id": Column(UUIDType, ForeignKey(parent_id), nullable=True, unique=True),
            "state": Column(ARRAY(EnumType(event_type)), nullable=False, default=default_state),
            "version": Column(Integer, default=1, nullable=False),

            # shortcuts
            "container_id": ColumnAlias(container_id_name),
            "container_id_name": container_id_name,

            # indexes and constraints
            "__table_args__": cls.make_table_args(cls, container_id_name, event_type),
        }

    def make_table_args(cls, container_id_name, event_type):
        return cls.make_indexes(
            cls,
            container_id_name,
        ) + cls.make_state_machine_constraints(
            cls,
            event_type,
        ) + cls.make_column_constraints(
            cls,
            event_type,
        )

    def make_indexes(cls, container_id_name):
        return (
            # logical clock is unique and indexed
            Index(
                "unique_logical_clock",
                container_id_name,
                "clock",
                unique=True,
            ),
            # events are unique per container, event type, and version
            # XXX this won't work for some types of events
            Index(
                "unique_event_type",
                container_id_name,
                "event_type",
                "version",
                unique=True,
            ),
        )

    def make_state_machine_constraints(cls, event_type):
        return (
            # events must have a parent unless they are initial and its the first version
            CheckConstraint(
                name="require_parent_id",
                sqltext="parent_id IS NOT NULL OR (version = 1 AND event_type IN ({}))".format(
                    join_event_types(item for item in event_type if item.is_initial),
                ),
            ),
        )

    def make_column_constraints(cls, event_type):
        return (
            CheckConstraint(
                name="require_{}".format("assignee"),
                sqltext="{} IS NOT NULL OR event_type NOT IN ({})".format(
                    "assignee",
                    join_event_types(item for item in event_type.requires("assignee"))
                ),
            ),
            CheckConstraint(
                name="require_{}".format("deadline"),
                sqltext="{} IS NOT NULL OR event_type NOT IN ({})".format(
                    "deadline",
                    join_event_types(item for item in event_type.requires("deadline"))
                ),
            ),
        )
