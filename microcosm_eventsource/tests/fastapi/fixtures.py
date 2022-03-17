"""
Test fixtures.

"""
from datetime import datetime

from typing import AsyncContextManager, Optional
from uuid import UUID

from fastapi import Request
from microcosm.api import binding
from microcosm_fastapi.conventions.schemas import BaseSchema, SearchSchema
from microcosm_fastapi.database.store import StoreAsync
from microcosm_fastapi.namespaces import Namespace
from microcosm_postgres.models import EntityMixin, Model, UnixTimestampEntityMixin
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy_utils import UUIDType

from microcosm_eventsource.accumulation import alias, keep, union
from microcosm_eventsource.fastapi.event_types import EventType, EventTypeUnion, event_info
from microcosm_eventsource.fastapi.controllers import EventController
from microcosm_eventsource.fastapi.factory import EventFactoryAsync
from microcosm_eventsource.fastapi.resources import EventSchema, SearchEventSchema
from microcosm_eventsource.fastapi.routes import configure_event_crud
from microcosm_eventsource.models import EventMeta
from microcosm_eventsource.stores.fastapi import EventStoreAsync
from microcosm_eventsource.transitioning import (
    all_of,
    any_of,
    but_not,
    event,
    nothing,
)


class SimpleTestObjectEventType(EventType):
    CREATED = event_info(
        follows=nothing(),
        accumulate=keep(),
    )
    READY = event_info(
        follows=event("CREATED"),
        accumulate=keep(),
    )
    DONE = event_info(
        follows=event("READY"),
        accumulate=keep(),
    )


class SimpleTestObject(Model, EntityMixin):
    __tablename__ = "simple_test_object"


class SimpleTestObjectEvent(EntityMixin, metaclass=EventMeta):
    __tablename__ = "simple_test_object_event"
    __eventtype__ = SimpleTestObjectEventType
    __container__ = SimpleTestObject

    simple_test_object_id = Column(
        UUIDType(),
        ForeignKey("simple_test_object.id"),
        nullable=True,
    )


@binding("simple_test_object_store_async")
class SimpleTestObjectStore(StoreAsync):
    def __init__(self, graph):
        super(SimpleTestObjectStore, self).__init__(graph, SimpleTestObject)


@binding("simple_test_object_event_store")
class SimpleTestObjectEventStore(EventStoreAsync):
    def __init__(self, graph):
        super(SimpleTestObjectEventStore, self).__init__(graph, SimpleTestObjectEvent)


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


class AdvancedTaskEventType(EventType):
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

    __mapper_args__ = {"eager_defaults": True}

    assignee = Column(String)
    deadline = Column(DateTime)


class SubTaskEvent(UnixTimestampEntityMixin, metaclass=EventMeta):
    __tablename__ = "sub_task_event"
    __eventtype__ = SubTaskEventType
    __container__ = SubTask

    assignee = Column(String)
    deadline = Column(DateTime)


@binding("task_store_async")
class TaskStore(StoreAsync):
    def __init__(self, graph):
        super(TaskStore, self).__init__(graph, Task)

    def _order_by(self, query, **kwargs):
        return query.order_by(Task.created_at.desc())


@binding("sub_task_store_async")
class SubTaskStore(StoreAsync):
    def __init__(self, graph):
        super(SubTaskStore, self).__init__(graph, SubTask)

    def _order_by(self, query, **kwargs):
        return query.order_by(
            SubTask.description,
            SubTask.priority,
        )


@binding("task_event_store_async")
class TaskEventStore(EventStoreAsync):
    def __init__(self, graph):
        super(TaskEventStore, self).__init__(graph, TaskEvent)


@binding("sub_task_event_store_async")
class SubTaskEventStore(EventStoreAsync):
    def __init__(self, graph):
        super(SubTaskEventStore, self).__init__(graph, SubTaskEvent)


class NewTaskEventSchema(BaseSchema):
    assignee: Optional[str] = None
    deadline: Optional[datetime] = None
    task_id: UUID
    event_type: TaskEventType


class TaskEventSchema(NewTaskEventSchema, EventSchema):
    pass


# class SearchTaskEventSchema(SearchEventSchema):
#     task_id = fields.UUID()
#     event_type = EnumField(TaskEventType)

# TODO - do we need to grab the async one from microcosm-fastapi
# @binding("session_factory")
# def configure_session_factory(graph):
#     return register_session_factory(graph, "db", SessionContext.make)


@binding("task_event_controller_async")
class TaskEventController(EventController):
    def __init__(self, graph):
        super(TaskEventController, self).__init__(graph, graph.task_event_store_async)
        self.ns = Namespace(
            subject=TaskEvent,
            version="v1",
        )

    def get_event_factory_kwargs(self):
        return {}

    @property
    def event_factory(self):
        return EventFactoryAsync(
            event_store=self.store,
            identifier_key=self.identifier_key,
            **self.get_event_factory_kwargs(),
        )

    async def create(
        self, body: NewTaskEventSchema, request: Request, db_session: AsyncContextManager
    ) -> TaskEventSchema:
        kw_args = {**body.dict()}
        del kw_args["event_type"]

        async with db_session as session:
            result = await super().create(request, session=session, event_type=body.event_type, **kw_args)
            return result

    async def retrieve(
        self, task_event_id: UUID, db_session: AsyncContextManager
    ) -> TaskEventSchema:
        async with db_session as session:
            return await super()._retrieve(task_event_id, session=session)

    async def search(
        self,
        db_session: AsyncContextManager,
        offset: int = 0,
        limit: int = 20,
    ) -> SearchSchema(TaskEventSchema):

        async with db_session as session:
            return await super()._search(
                session=session,
                offset=offset,
                limit=limit,
            )

    async def delete(self, task_event_id: UUID, db_session: AsyncContextManager):
        async with db_session as session:
            return await super()._delete(task_event_id, session)

    async def replace(
            self, task_event_id: UUID, body: NewTaskEventSchema, request: Request, db_session: AsyncContextManager
    ) -> TaskEventSchema:
        instance = TaskEvent(**body.dict())

        async with db_session as session:
            return await super()._replace(identifier=task_event_id, body=instance, session=session)


# TODO - Need to fix this....
# We can't pass in the schemas - they have to be declared beforehand
# We could pass in functions
@binding("task_crud_routes_async")
def configure_task_crud(graph):
    configure_event_crud(
        graph=graph,
        controller=graph.task_event_controller_async,
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


@binding("activity_store_async")
class ActivityStore(StoreAsync):
    def __init__(self, graph):
        super(ActivityStore, self).__init__(graph, Activity)


@binding("activity_event_store_async")
class ActivityEventStore(StoreAsync):
    def __init__(self, graph):
        super(ActivityEventStore, self).__init__(graph, ActivityEvent)
