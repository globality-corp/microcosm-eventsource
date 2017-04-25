# microcosm-eventsource

Event-sourced state machines using `microcosm`.

Manages state changes as an immutable event log using:

 -  [microcosm-postgres](https://github.com/globality-corp/microcosm-postgres)
 -  [microcosm-flask](https://github.com/globality-corp/microcosm-flask)
 -  [microcosm-pubsub](https://github.com/globality-corp/microcosm-pubsub)


## By Example

Imagine a task list with the following rules:

 -  A task is created with a text `description`.

 -  After a task is created, it can be assigned to an `assignee` and scheduled
    for a `deadline`. These operations can happen in any order.

 -  After a task is both assigned and scheduled, it may be started.

 -  After a task is started, it may be reassigned or rescheduled any number of times.

 -  After a task is started, it may either be canceled or completed. Both of these
    states are terminal.

 -  Before a task is canceled or completed, it maybe revised at any time. A revised
    task reverts back to the initial state.


These actions collectively define a state machine, where each transition is triggered
by a new event:

              => ASSIGNED => SCHEDULED =>             => CANCELED
            /                             \         /
    CREATED                                 STARTED
            \                             /         \
              => SCHEDULED => ASSIGNED =>             => COMPLETED

(For simplicity, the `REASSIGNED`, `RESCHEDULED`, and `REVISED` events are not shown.)


### Defining Tasks

Every task will be defined in a `task` table, which needs only the default, auto-generated
primary key and timestamp fields:

    class Task(UnixTimestampEntityMixin, Model):
        __tablename__ = "task"


    @binding("task_store")
    class TaskStore(Store):

        def __init__(self, graph):
            super(TaskStore, self).__init__(graph, Task)


### Defining Task Events

Every action that changes states will be defined in a `task_event` table, which has:

 -  a foreign key reference back to the "container" `task` table
 -  a reference to an event type enumeration (see below)
 -  zero or more additional columns

For example:

    @add_metaclass(EventMeta)
    class TaskEvent(UnixTimestampEntityMixin):
        __tablename__ = "task_event"
        __eventtype__ = TaskEventType
        __container__ = Task

        assignee = Column(String)
        deadline = Column(DateTime)

    @binding("task_event_store")
    class TaskEventStore(EventStore):

        def __init__(self, graph):
            super(TaskEventStore, self).__init__(graph, TaskEvent)


### Defining Task Event Types

It remains to define the legal events and their transitions using an enumeration. This is done
as follows:

 -  Each enumerated value is identified using its (left hand side) `name`.
 -  Each enumerated value is configured using its (right hand side) meta data.
 -  This meta data includes:
     -  "following conditions" for legal state transitions.
     -  a flag controlling whether state accumulates through this transition
     -  a flag controlling whether the state machine is restarted (with a new version) after this state
     -  a list of zero or more required additional columns for this state

Example:

    class TaskEventType(EventType):
        CREATED = event_info()
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
            restarting=True,
        )
        CANCELED = event_info(
            follows=event("STARTED"),
        )
        COMPLETED = event_info(
            follows=event("STARTED"),
        )
