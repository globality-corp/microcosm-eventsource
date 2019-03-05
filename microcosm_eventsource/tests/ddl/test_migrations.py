"""
Persistence tests.

"""
from datetime import datetime
from os import pardir
from os.path import dirname, join
from hamcrest import (
    assert_that,
    calling,
    contains,
    has_length,
    has_properties,
    raises,
)
from microcosm.api import create_object_graph
from microcosm_postgres.context import SessionContext, transaction
from microcosm_postgres.identifiers import new_object_id
from sqlalchemy.exc import ProgrammingError, IntegrityError

from microcosm_eventsource.tests.fixtures import (
    Task,
    TaskEvent,
    TaskEventType,
    Activity,
    ActivityEvent,
    ActivityEventType,
)


class TestMigrations:

    def setup(self):
        self.graph = create_object_graph(
            "microcosm_eventsource",
            root_path=join(dirname(__file__), pardir),
            testing=True,
        )
        self.graph.use(
            "task_store",
            "task_event_store",
            "activity_store",
            "activity_event_store",
        )
        self.store = self.graph.task_event_store
        self.activity_store = self.graph.activity_event_store

        self.context = SessionContext(self.graph)
        self.context.recreate_all()
        self.context.open()

        with transaction():
            self.task = Task().create()
            self.created_event = TaskEvent(
                event_type=TaskEventType.CREATED,
                task_id=self.task.id,
            ).create()
            self.scheduled_event = TaskEvent(
                deadline=datetime.utcnow(),
                event_type=TaskEventType.SCHEDULED,
                parent_id=self.created_event.id,
                state=[TaskEventType.CREATED, TaskEventType.SCHEDULED],
                task_id=self.task.id,
            ).create()
            self.assigned_event = TaskEvent(
                deadline=datetime.utcnow(),
                event_type=TaskEventType.ASSIGNED,
                parent_id=self.scheduled_event.id,
                state=[TaskEventType.ASSIGNED, TaskEventType.CREATED, TaskEventType.SCHEDULED],
                assignee="assignee",
                task_id=self.task.id,
            ).create()
            self.started_event = TaskEvent(
                event_type=TaskEventType.STARTED,
                parent_id=self.assigned_event.id,
                task_id=self.task.id,
            ).create()
            # flush sqlalchemy cache before sql operation
            self.store.session.expire_all()

    def teardown(self):
        self.context.close()
        self.graph.postgres.dispose()

    def test_proc_event_type_delete(self):
        """
        Test that sql proc_event_type_delete function:
        * delete events
        * replaces parent_id
        * updates states

        """
        with transaction():
            self.store.session.execute("SELECT proc_event_type_delete('task_event', 'SCHEDULED', 'task_id');")

        results = self.store.search()
        assert_that(results, has_length(3))
        assert_that(results, contains(
            has_properties(
                event_type=TaskEventType.STARTED,
                state=[TaskEventType.STARTED],
                id=self.started_event.id,
                parent_id=self.assigned_event.id,
            ),
            has_properties(
                event_type=TaskEventType.ASSIGNED,
                state=[TaskEventType.ASSIGNED, TaskEventType.CREATED],
                id=self.assigned_event.id,
                parent_id=self.created_event.id,
            ),
            has_properties(
                event_type=TaskEventType.CREATED,
                state=[TaskEventType.CREATED],
                id=self.created_event.id,
                parent_id=None,
            ),
        ))

    def test_proc_event_type_replace(self):
        """
        Test that proc_event_type_replace sql function:
        * replace event_type
        * replace event_types in state (and sort it)

        """
        with transaction():
            self.store.session.execute("SELECT proc_event_type_replace('task_event', 'SCHEDULED', 'CANCELED');")

        results = self.store.search()
        assert_that(results, has_length(4))
        assert_that(results, contains(
            has_properties(
                event_type=TaskEventType.STARTED,
                state=[TaskEventType.STARTED],
                id=self.started_event.id,
                parent_id=self.assigned_event.id,
            ),
            has_properties(
                event_type=TaskEventType.ASSIGNED,
                state=[TaskEventType.ASSIGNED, TaskEventType.CANCELED, TaskEventType.CREATED],
                id=self.assigned_event.id,
                parent_id=self.scheduled_event.id,
            ),
            has_properties(
                event_type=TaskEventType.CANCELED,
                state=[TaskEventType.CANCELED, TaskEventType.CREATED],
                id=self.scheduled_event.id,
                parent_id=self.created_event.id,
            ),
            has_properties(
                event_type=TaskEventType.CREATED,
                state=[TaskEventType.CREATED],
                id=self.created_event.id,
                parent_id=None,
            ),
        ))

    def test_delete_single_event(self):
        """
        Test that sql proc_events_delete function:
        * delete events
        * replaces parent_id

        """
        with transaction():
            self.store.session.execute("""
                CREATE TEMP TABLE events_to_remove AS (
                    SELECT id FROM task_event WHERE event_type='SCHEDULED'
                );
                SELECT proc_events_delete('task_event', 'events_to_remove', 'task_id');
            """)

        results = self.store.search()
        assert_that(results, has_length(3))
        assert_that(results, contains(
            has_properties(
                event_type=TaskEventType.STARTED,
                state=[TaskEventType.STARTED],
                id=self.started_event.id,
                parent_id=self.assigned_event.id,
            ),
            has_properties(
                event_type=TaskEventType.ASSIGNED,
                state=[TaskEventType.ASSIGNED, TaskEventType.CREATED, TaskEventType.SCHEDULED],
                id=self.assigned_event.id,
                parent_id=self.created_event.id,
            ),
            has_properties(
                event_type=TaskEventType.CREATED,
                state=[TaskEventType.CREATED],
                id=self.created_event.id,
                parent_id=None,
            ),
        ))

    def test_edge_case_proc_event_type_replace_remove_duplicates(self):
        """
        Test that proc_event_type_replace sql function:
        * replace event_type
        * replace event_types in state and remove duplicates (and sort it)

        """
        with transaction():
            self.store.session.execute("SELECT proc_event_type_replace('task_event', 'SCHEDULED', 'CREATED');")

        results = self.store.search()
        assert_that(results, has_length(4))
        assert_that(results, contains(
            has_properties(
                event_type=TaskEventType.STARTED,
                state=[TaskEventType.STARTED],
                id=self.started_event.id,
                parent_id=self.assigned_event.id,
            ),
            has_properties(
                event_type=TaskEventType.ASSIGNED,
                state=[TaskEventType.ASSIGNED, TaskEventType.CREATED],
                id=self.assigned_event.id,
                parent_id=self.scheduled_event.id,
            ),
            has_properties(
                event_type=TaskEventType.CREATED,
                state=[TaskEventType.CREATED],
                id=self.scheduled_event.id,
                parent_id=self.created_event.id,
            ),
            has_properties(
                event_type=TaskEventType.CREATED,
                state=[TaskEventType.CREATED],
                id=self.created_event.id,
                parent_id=None,
            ),
        ))

    def test_edge_case_delete_number_of_events(self):
        """
        Test that sql proc_events_delete function:
        * delete events
        * replaces parent_id (even if the new parent is not directly refrenced by the deleted event)

        """
        with transaction():
            self.store.session.execute("""
                CREATE TEMP TABLE events_to_remove AS (
                    SELECT id FROM task_event WHERE event_type='SCHEDULED' OR event_type='ASSIGNED'
                );
                SELECT proc_events_delete('task_event', 'events_to_remove', 'task_id');
            """)

        results = self.store.search()
        assert_that(results, has_length(2))
        assert_that(results, contains(
            has_properties(
                event_type=TaskEventType.STARTED,
                state=[TaskEventType.STARTED],
                id=self.started_event.id,
                parent_id=self.created_event.id,
            ),
            has_properties(
                event_type=TaskEventType.CREATED,
                state=[TaskEventType.CREATED],
                id=self.created_event.id,
                parent_id=None,
            ),
        ))

    def test_edge_case_delete_last_event(self):
        """
        Test that proc_event_type_replace sql function can delete last event of a model

        """
        with transaction():
            self.store.session.execute("""
                CREATE TEMP TABLE events_to_remove AS (
                    SELECT id FROM task_event WHERE event_type='STARTED'
                );
                SELECT proc_events_delete('task_event', 'events_to_remove', 'task_id');
            """)

        results = self.store.search()
        assert_that(results, has_length(3))
        assert_that(results, contains(
            has_properties(
                event_type=TaskEventType.ASSIGNED,
                state=[TaskEventType.ASSIGNED, TaskEventType.CREATED, TaskEventType.SCHEDULED],
                id=self.assigned_event.id,
                parent_id=self.scheduled_event.id,
            ),
            has_properties(
                event_type=TaskEventType.SCHEDULED,
                state=[TaskEventType.CREATED, TaskEventType.SCHEDULED],
                id=self.scheduled_event.id,
                parent_id=self.created_event.id,
            ),
            has_properties(
                event_type=TaskEventType.CREATED,
                state=[TaskEventType.CREATED],
                id=self.created_event.id,
                parent_id=None,
            ),
        ))

    def test_edge_case_delete_first_event(self):
        """
        Can delete first events (But still have to follow "require_{}_parent_id" constraint)

        """
        with transaction():
            self.activity_store.session.execute(
                """
                    CREATE TEMP TABLE events_to_remove AS (
                        SELECT id FROM task_event WHERE event_type='CREATED'
                    );
                    SELECT proc_event_type_replace('task_event', 'SCHEDULED', 'CREATED');
                    SELECT proc_events_delete('task_event', 'events_to_remove', 'task_id');
                """
            )
            self.store.session.expire_all()
        results = self.store.search()
        assert_that(results, has_length(3))
        assert_that(results, contains(
            has_properties(
                event_type=TaskEventType.STARTED,
                state=[TaskEventType.STARTED],
                id=self.started_event.id,
                parent_id=self.assigned_event.id,
            ),
            has_properties(
                event_type=TaskEventType.ASSIGNED,
                state=[TaskEventType.ASSIGNED, TaskEventType.CREATED],
                id=self.assigned_event.id,
                parent_id=self.scheduled_event.id,
            ),
            has_properties(
                event_type=TaskEventType.CREATED,
                state=[TaskEventType.CREATED],
                id=self.scheduled_event.id,
                parent_id=None,
            ),
        ))

    def test_edge_case_proc_events_delete_with_no_parent_id_constraint(self):
        """
        Some events dont have a parent_id_constraint (If model.__unique_parent__ is set to False)
        In order to delete them, we have to call:
            "proc_events_delete_with_no_parent_id_constraint" instead of "proc_events_delete".
        We cannot call "proc_events_delete_with_no_parent_id_constraint" for events with constraint.

        """
        with transaction():
            activity = Activity().create()
            created_event = ActivityEvent(
                event_type=ActivityEventType.CREATED,
                activity_id=activity.id,
            ).create()
            ActivityEvent(
                event_type=ActivityEventType.CANCELED,
                parent_id=created_event.id,
                activity_id=activity.id,
            ).create()
            self.activity_store.session.expire_all()

        with transaction():
            assert_that(calling(self.activity_store.session.execute).with_args(
                """
                    CREATE TEMP TABLE events_to_remove AS (
                        SELECT id FROM activity_event WHERE event_type='CANCELED'
                    );
                    SELECT proc_events_delete('activity_event', 'events_to_remove', 'activity_id');
                """
                ), raises(ProgrammingError))

        with transaction():
            assert_that(calling(self.activity_store.session.execute).with_args(
                """
                    CREATE TEMP TABLE events_to_remove AS (
                        SELECT id FROM task_event WHERE event_type='SCHEDULED'
                    );
                    SELECT proc_events_delete_with_no_parent_id_constraint('task_event', 'events_to_remove', 'task_id');
                """
                ), raises(IntegrityError))

        with transaction():
            self.activity_store.session.execute("""
                CREATE TEMP TABLE events_to_remove AS (
                    SELECT id FROM activity_event WHERE event_type='CANCELED'
                );
                SELECT proc_events_delete_with_no_parent_id_constraint(
                    'activity_event',
                    'events_to_remove',
                    'activity_id'
                );
            """)

        results = self.activity_store.search()
        assert_that(results, has_length(1))
        assert_that(results, contains(
            has_properties(
                event_type=ActivityEventType.CREATED,
                state=[ActivityEventType.CREATED],
                id=created_event.id,
                parent_id=None,
            ),
        ))

    def test_proc_events_create(self):
        """
        Insert a revised event before the started event and after the assigned event.

        """
        reassigned_event_id = new_object_id()

        events_to_create_string = (
            f"CREATE TEMP TABLE events_to_create AS (\n"
            f"       SELECT\n"
            f"          '{reassigned_event_id}'::uuid as id,\n"
            f"          extract(epoch from now()) as created_at,\n"
            f"          extract(epoch from now()) as updated_at,\n"
            f"          assignee,\n"
            f"          NULL::timestamp without time zone as deadline,\n"
            f"          task_id,\n"
            f"          'REASSIGNED' as event_type,\n"
            f"          id as parent_id,\n"
            f"          state,\n"
            f"          1 as version\n"
            f"       FROM task_event WHERE event_type='ASSIGNED'\n"
            f"    );"
        )

        self.activity_store.session.execute(events_to_create_string)

        self.activity_store.session.execute("""
            SELECT proc_events_create(
                'task_event',
                'events_to_create',
                '(
                    id,
                    created_at,
                    updated_at,
                    assignee,
                    deadline,
                    task_id,
                    event_type,
                    parent_id,
                    state,
                    version
                )'
            );
        """)
        results = self.store.search()
        assert_that(results, has_length(5))

        # NB: The events appear out of order because they are sorted by clock,
        # but the parent id chain is correct. In particular the parent of the
        # STARTED event has been changed by the migration
        assert_that(results, contains(
            has_properties(
                event_type=TaskEventType.REASSIGNED,
                state=[TaskEventType.ASSIGNED, TaskEventType.CREATED, TaskEventType.SCHEDULED],
                parent_id=self.assigned_event.id,
                id=reassigned_event_id,
            ),
            has_properties(
                event_type=TaskEventType.STARTED,
                state=[TaskEventType.STARTED],
                id=self.started_event.id,
                parent_id=reassigned_event_id,
            ),
            has_properties(
                event_type=TaskEventType.ASSIGNED,
                state=[TaskEventType.ASSIGNED, TaskEventType.CREATED, TaskEventType.SCHEDULED],
                id=self.assigned_event.id,
                parent_id=self.scheduled_event.id,
            ),
            has_properties(
                event_type=TaskEventType.SCHEDULED,
                state=[TaskEventType.CREATED, TaskEventType.SCHEDULED],
                id=self.scheduled_event.id,
                parent_id=self.created_event.id,
            ),
            has_properties(
                event_type=TaskEventType.CREATED,
                state=[TaskEventType.CREATED],
                id=self.created_event.id,
                parent_id=None,
            ),
        ))

    def test_proc_events_create_end_event(self):
        """
        Insert a canceled event at the end of the event stream.

        """
        cancelled_event_id = new_object_id()

        events_to_create_string = (
            f"CREATE TEMP TABLE events_to_create AS (\n"
            f"       SELECT\n"
            f"          '{cancelled_event_id}'::uuid as id,\n"
            f"          extract(epoch from now()) as created_at,\n"
            f"          extract(epoch from now()) as updated_at,\n"
            f"          assignee,\n"
            f"          NULL::timestamp without time zone as deadline,\n"
            f"          task_id,\n"
            f"          'CANCELED' as event_type,\n"
            f"          id as parent_id,\n"
            f"          '{{\"CANCELED\"}}'::character varying[] as state,\n"
            f"          1 as version\n"
            f"       FROM task_event WHERE event_type='STARTED'\n"
            f"    );"
        )

        self.activity_store.session.execute(events_to_create_string)

        self.activity_store.session.execute("""
            SELECT proc_events_create(
                'task_event',
                'events_to_create',
                '(
                    id,
                    created_at,
                    updated_at,
                    assignee,
                    deadline,
                    task_id,
                    event_type,
                    parent_id,
                    state,
                    version
                )'
            );
        """)
        results = self.store.search()
        assert_that(results, has_length(5))

        # NB: The events appear out of order because they are sorted by clock,
        # but the parent id chain is correct. In particular the parent of the
        # STARTED event has been changed by the migration
        assert_that(results, contains(
            has_properties(
                event_type=TaskEventType.CANCELED,
                state=[TaskEventType.CANCELED],
                parent_id=self.started_event.id,
                id=cancelled_event_id,
            ),
            has_properties(
                event_type=TaskEventType.STARTED,
                state=[TaskEventType.STARTED],
                id=self.started_event.id,
                parent_id=self.assigned_event.id,
            ),
            has_properties(
                event_type=TaskEventType.ASSIGNED,
                state=[TaskEventType.ASSIGNED, TaskEventType.CREATED, TaskEventType.SCHEDULED],
                id=self.assigned_event.id,
                parent_id=self.scheduled_event.id,
            ),
            has_properties(
                event_type=TaskEventType.SCHEDULED,
                state=[TaskEventType.CREATED, TaskEventType.SCHEDULED],
                id=self.scheduled_event.id,
                parent_id=self.created_event.id,
            ),
            has_properties(
                event_type=TaskEventType.CREATED,
                state=[TaskEventType.CREATED],
                id=self.created_event.id,
                parent_id=None,
            ),
        ))
