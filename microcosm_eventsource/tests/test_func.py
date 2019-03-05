from os.path import dirname

from hamcrest import assert_that, contains
from microcosm.api import create_object_graph
from microcosm_postgres.context import SessionContext, transaction

from microcosm_eventsource.func import last
from microcosm_eventsource.tests.fixtures import (
    Task,
    TaskEvent,
    TaskEventType,
)


class TestLast:

    def setup(self):
        self.graph = create_object_graph(
            "microcosm_eventsource",
            root_path=dirname(__file__),
            testing=True,
        )
        self.graph.use(
            "task_store",
            "task_event_store",
            "activity_store",
            "activity_event_store",
        )

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

        with transaction():
            self.task = Task().create()
            self.created_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                task_id=self.task.id,
            ).create()
            self.assigned_event = TaskEvent(
                assignee="Alice",
                event_type=TaskEventType.ASSIGNED,
                parent_id=self.created_event.id,
                task_id=self.task.id,
            ).create()
            self.started_event = TaskEvent(
                event_type=TaskEventType.STARTED,
                parent_id=self.assigned_event.id,
                task_id=self.task.id,
            ).create()
            self.reassigned_event = TaskEvent(
                assignee="Bob",
                event_type=TaskEventType.REASSIGNED,
                parent_id=self.started_event.id,
                task_id=self.task.id,
            ).create()
            self.reassigned_event = TaskEvent(
                event_type=TaskEventType.COMPLETED,
                parent_id=self.reassigned_event.id,
                task_id=self.task.id,
            ).create()

    def teardown(self):
        self.context.close()
        self.graph.postgres.dispose()

    def test_last(self):
        rows = self.context.session.query(
            TaskEvent.assignee,
            last.of(TaskEvent.assignee),
        ).order_by(
            TaskEvent.clock.desc(),
        ).all()

        assert_that(rows, contains(
            contains(None, "Bob"),
            contains("Bob", "Bob"),
            contains(None, "Alice"),
            contains("Alice", "Alice"),
            contains(None, None),
        ))

    def test_last_filter_by(self):
        rows = self.context.session.query(
            TaskEvent.assignee,
            last.of(
                TaskEvent.assignee,
                TaskEvent.event_type == TaskEventType.ASSIGNED,
            ),
        ).order_by(
            TaskEvent.clock.desc(),
        ).all()

        assert_that(rows, contains(
            contains(None, "Alice"),
            contains("Bob", "Alice"),
            contains(None, "Alice"),
            contains("Alice", "Alice"),
            contains(None, None),
        ))
