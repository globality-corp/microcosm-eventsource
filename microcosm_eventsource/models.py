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


class BaseEvent(object):

    def is_similar_to(self, other):
        """
        Are two events similar enough to activate upserting?

        """
        return all((
            self.event_type == other.event_type,
            self.parent_id == other.parent_id,
            self.container_id == other.container_id
        ))


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

         Can optionally include the following declarations:

         -  `__unique_parent__` a flag indicating whether or not a unique parent constraint is created
         May be set to False in cases like having a unique parent for each version of the event
         If the flag is set to False, a similar unique constraint should be set on the event class

        """
        if any(type(base) is EventMeta for base in bases):
            return super(EventMeta, cls).__new__(cls, name, bases, dct)

        # add model to expected bases
        bases = bases + (BaseEvent, Model,)

        # declare event columns and indexes
        dct.update(cls.make_declarations(
            cls,
            container_name=dct["__container__"].__tablename__,
            event_type=dct["__eventtype__"],
            table_name=dct["__tablename__"],
            table_args=dct.get("__table_args__", ()),
            unique_parent=dct.get("__unique_parent__", True)
        ))

        return super(EventMeta, cls).__new__(cls, name, bases, dct)

    def make_declarations(cls, container_name, event_type, table_name, table_args, unique_parent):
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
            "parent_id": Column(UUIDType, ForeignKey(parent_id), nullable=True, unique=unique_parent),
            "state": Column(ARRAY(EnumType(event_type)), nullable=False, default=default_state),
            "version": Column(Integer, default=1, nullable=False),

            # shortcuts
            "container_id": ColumnAlias(container_id_name),
            "container_id_name": container_id_name,

            # indexes and constraints
            "__table_args__": table_args + cls.make_table_args(cls, table_name, container_id_name, event_type),
        }

    def make_table_args(cls, table_name, container_id_name, event_type):
        """
        Generate the event table's `__table_args__` value.

        """
        return cls.make_indexes(
            cls,
            table_name,
            container_id_name,
        ) + cls.make_state_machine_constraints(
            cls,
            table_name,
            event_type,
        ) + cls.make_column_constraints(
            cls,
            table_name,
            event_type,
        )

    def make_indexes(cls, table_name, container_id_name):
        """
        Declare expected indexes.

        """
        return (
            # logical clock is unique and indexed
            Index(
                "{}_unique_logical_clock".format(table_name),
                container_id_name,
                "clock",
                unique=True,
            ),
            # NB: it's often but (not always) appropriate to have a unique index on the
            # combination of container id, event type, and version; for now this should
            # be added by the user.
        )

    def make_state_machine_constraints(cls, table_name, event_type):
        """
        Enforce that each state machine defines a proper linked list.

        """
        return (
            # events must have a parent unless they are initial and its the first version
            CheckConstraint(
                name="require_{}_parent_id".format(table_name),
                sqltext="parent_id IS NOT NULL OR (version = 1 AND event_type IN ({}))".format(
                    join_event_types(item for item in event_type if item.is_initial),
                ),
            ),
        )

    def make_column_constraints(cls, table_name, event_type):
        """
        Event tables are polymorphic and cannot enforce column-level nullability.

        Fortunately, check contraints can enforce non-nullability by event type.

        """
        return tuple(
            CheckConstraint(
                name="require_{}_{}".format(table_name, column_name),
                sqltext="{} IS NOT NULL OR event_type NOT IN ({})".format(
                    column_name,
                    join_event_types(item for item in event_type.requires(column_name))
                ),
            )
            for column_name in event_type.required_column_names()
        )
