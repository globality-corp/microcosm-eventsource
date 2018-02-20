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
from sqlalchemy import Column, DateTime, String

from microcosm_eventsource.accumulation import alias, keep, union
from microcosm_eventsource.controllers import EventController
from microcosm_eventsource.transitioning import all_of, any_of, but_not, event, nothing
from microcosm_eventsource.event_types import event_info, EventType, EventTypeUnion
from microcosm_eventsource.models import EventMeta
from microcosm_eventsource.resources import EventSchema, SearchEventSchema
from microcosm_eventsource.routes import configure_event_crud
from microcosm_eventsource.stores import EventStore


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


class Task(UnixTimestampEntityMixin, Model):
    __tablename__ = "task"

    description = Column(String)


class TaskEvent(UnixTimestampEntityMixin, metaclass=EventMeta):
    __tablename__ = "task_event"
    __eventtype__ = TaskEventType
    __container__ = Task

    assignee = Column(String)
    deadline = Column(DateTime)


@binding("task_store")
class TaskStore(Store):

    def __init__(self, graph):
        super(TaskStore, self).__init__(graph, Task)


@binding("task_event_store")
class TaskEventStore(EventStore):

    def __init__(self, graph):
        super(TaskEventStore, self).__init__(graph, TaskEvent)


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
