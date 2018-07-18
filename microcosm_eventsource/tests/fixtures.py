"""
Test fixtures.

"""
from enum import Enum

from marshmallow import fields, Schema
from microcosm.api import binding
from microcosm_flask.fields import EnumField
from microcosm_flask.namespaces import Namespace
from microcosm_flask.session import register_session_factory
from microcosm_postgres.context import SessionContext
from microcosm_postgres.models import Model, UnixTimestampEntityMixin
from microcosm_postgres.store import Store
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy_utils import UUIDType

from microcosm_eventsource.accumulation import alias, keep, union
from microcosm_eventsource.controllers import EventController
from microcosm_eventsource.transitioning import all_of, any_of, but_not, event, nothing
from microcosm_eventsource.event_types import event_info, EventType, EventTypeUnion
from microcosm_eventsource.factory import EventFactory
from microcosm_eventsource.models import EventMeta
from microcosm_eventsource.resources import EventSchema, SearchEventSchema
from microcosm_eventsource.routes import configure_event_crud
from microcosm_eventsource.stores import EventStore


class FlexibleTaskEventType(EventType):
    # Used to test is_initial
    CREATED = event_info(
        follows=any_of(
            "CREATED",
            nothing(),
        ),
    )


class BasicTaskEventType(EventType):
    CREATED = event_info(
        follows=nothing(),
    )
    ASSIGNED = event_info(
        follows=all_of("CREATED", but_not("ASSIGNED")),
        accumulate=union(),
        requires=["assignee"],
    )
    SCHEDULED = event_info(
        follows=all_of("CREATED", but_not("SCHEDULED")),
        accumulate=union(),
        requires=["deadline"],
    )
    STARTED = event_info(
        follows=all_of("ASSIGNED", "SCHEDULED"),
    )
    CANCELED = event_info(
        follows=event("STARTED"),
    )
    COMPLETED = event_info(
        follows=event("STARTED"),
    )
    ENDED = event_info(
        follows=event("COMPLETED"),
        auto_transition=True,
    )


class AdvancedTaskEventType(Enum):
    REASSIGNED = event_info(
        follows=event("STARTED"),
        accumulate=keep(),
        requires=["assignee"],
    )
    RESCHEDULED = event_info(
        follows=event("STARTED"),
        accumulate=keep(),
        requires=["deadline"],
    )
    REVISED = event_info(
        follows=any_of("CREATED", "STARTED"),
        accumulate=alias("CREATED"),
        restarting=True,
    )


TaskEventType = EventTypeUnion("TaskEventType", BasicTaskEventType, AdvancedTaskEventType)
SubTaskEventType = EventTypeUnion("SubTaskEventType", BasicTaskEventType)


class Task(UnixTimestampEntityMixin, Model):
    __tablename__ = "task"

    description = Column(String)

    discriminator = Column(String, nullable=False)

    __mapper_args__ = dict(
        polymorphic_identity="task",
        polymorphic_on=discriminator,
    )


class SubTask(Task):
    __tablename__ = "sub_task"

    id = Column(
        UUIDType,
        ForeignKey("task.id"),
        primary_key=True,
    )
    priority = Column(Integer)

    __mapper_args__ = dict(
        polymorphic_identity="sub_task",
    )


class TaskEvent(UnixTimestampEntityMixin, metaclass=EventMeta):
    __tablename__ = "task_event"
    __eventtype__ = TaskEventType
    __container__ = Task

    assignee = Column(String)
    deadline = Column(DateTime)


class SubTaskEvent(UnixTimestampEntityMixin, metaclass=EventMeta):
    __tablename__ = "sub_task_event"
    __eventtype__ = SubTaskEventType
    __container__ = SubTask

    assignee = Column(String)
    deadline = Column(DateTime)


@binding("task_store")
class TaskStore(Store):

    def __init__(self, graph):
        super(TaskStore, self).__init__(graph, Task)

    def _order_by(self, query, **kwargs):
        return query.order_by(Task.created_at.desc())


@binding("sub_task_store")
class SubTaskStore(Store):

    def __init__(self, graph):
        super(SubTaskStore, self).__init__(graph, SubTask)

    def _order_by(self, query, **kwargs):
        return query.order_by(
            SubTask.description,
            SubTask.priority,
        )


@binding("task_event_store")
class TaskEventStore(EventStore):

    def __init__(self, graph):
        super(TaskEventStore, self).__init__(graph, TaskEvent)


@binding("sub_task_event_store")
class SubTaskEventStore(EventStore):

    def __init__(self, graph):
        super(SubTaskEventStore, self).__init__(graph, SubTaskEvent)


class NewTaskEventSchema(Schema):
    assignee = fields.String(required=False, allow_none=True)
    deadline = fields.DateTime(required=False, allow_none=True)
    taskId = fields.UUID(attribute="task_id", required=True)
    eventType = EnumField(TaskEventType, attribute="event_type", required=True)


class TaskEventSchema(NewTaskEventSchema, EventSchema):
    pass


class SearchTaskEventSchema(SearchEventSchema):
    task_id = fields.UUID()
    event_type = EnumField(TaskEventType)


@binding("session_factory")
def configure_session_factory(graph):
    return register_session_factory(graph, "db", SessionContext.make)


@binding("task_event_controller")
class TaskEventController(EventController):
    def __init__(self, graph):
        super(TaskEventController, self).__init__(graph, graph.task_event_store)
        self.ns = Namespace(
            subject=TaskEvent,
            version="v1",
        )

    def get_event_factory_kwargs(self):
        return {}

    @property
    def event_factory(self):
        return EventFactory(
            event_store=self.store,
            identifier_key=self.identifier_key,
            **self.get_event_factory_kwargs(),
        )


@binding("task_crud_routes")
def configure_task_crud(graph):
    configure_event_crud(
        graph=graph,
        controller=graph.task_event_controller,
        event_schema=TaskEventSchema(),
        new_event_schema=NewTaskEventSchema(),
        search_event_schema=SearchTaskEventSchema(),
    )


class ActivityEventType(EventType):
    CREATED = event_info(
        follows=nothing(),
    )
    CANCELED = event_info(
        follows=event("CREATED"),
    )


class Activity(UnixTimestampEntityMixin, Model):
    __tablename__ = "activity"

    description = Column(String)


class ActivityEvent(UnixTimestampEntityMixin, metaclass=EventMeta):
    __tablename__ = "activity_event"
    __eventtype__ = ActivityEventType
    __container__ = Activity
    # Supports multiple children per parent
    __unique_parent__ = False

    assignee = Column(String)


@binding("activity_store")
class ActivityStore(Store):

    def __init__(self, graph):
        super(ActivityStore, self).__init__(graph, Activity)


@binding("activity_event_store")
class ActivityEventStore(EventStore):

    def __init__(self, graph):
        super(ActivityEventStore, self).__init__(graph, ActivityEvent)
