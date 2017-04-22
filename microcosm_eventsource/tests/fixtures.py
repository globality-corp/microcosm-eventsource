"""
Test fixtures.

"""
from six import add_metaclass

from marshmallow import fields, Schema
from microcosm.api import binding
from microcosm_flask.fields import EnumField
from microcosm_flask.namespaces import Namespace
from microcosm_flask.session import register_session_factory
from microcosm_postgres.context import SessionContext
from microcosm_postgres.models import Model, UnixTimestampEntityMixin
from microcosm_postgres.store import Store
from sqlalchemy import Column, DateTime, String

from microcosm_eventsource.controllers import EventController
from microcosm_eventsource.event_types import all_of, but_not, info, EventType
from microcosm_eventsource.models import EventMeta
from microcosm_eventsource.resources import EventSchema, SearchEventSchema
from microcosm_eventsource.routes import configure_event_crud
from microcosm_eventsource.stores import EventStore


class TaskEventType(EventType):
    CREATED = info()
    ASSIGNED = info(
        follows=[all_of("CREATED", but_not("ASSIGNED"))],
        accumulating=True,
        requires=["assignee"]
    )
    SCHEDULED = info(
        follows=[all_of("CREATED", but_not("SCHEDULED"))],
        accumulating=True,
        requires=["deadline"],
    )
    STARTED = info(
        follows=[all_of("ASSIGNED", "SCHEDULED")],
    )
    REASSIGNED = info(
        follows=["STARTED"],
        accumulating=True,
        requires=["assignee"],
    )
    RESCHEDULED = info(
        follows=["STARTED"],
        accumulating=True,
        requires=["deadline"],
    )
    REVISED = info(
        follows=["CREATED", "STARTED"],
        # XXX need to support jumping to state CREATED
        restarting=True,
    )
    CANCELED = info(
        follows=["STARTED"],
    )
    COMPLETED = info(
        follows=["STARTED"],
    )


class Task(UnixTimestampEntityMixin, Model):
    __tablename__ = "task"


@add_metaclass(EventMeta)
class TaskEvent(UnixTimestampEntityMixin):
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
