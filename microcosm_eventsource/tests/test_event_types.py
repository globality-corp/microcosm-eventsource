"""
Event type tests.

"""
from hamcrest import assert_that, equal_to, is_

from microcosm_eventsource.tests.fixtures import TaskEventType, TaskEvent


def test_is_initial():
    """
    Events with no following information may be initial.

    """
    assert_that(TaskEventType.CREATED.is_initial, is_(equal_to(True)))
    assert_that(TaskEventType.STARTED.is_initial, is_(equal_to(False)))


def test_assign_before_scheduled():
    """
    Assigned can be evaluated before scheduled.

    """
    parent = TaskEvent(
        event_type=TaskEventType.ASSIGNED,
        state=(TaskEventType.CREATED, TaskEventType.ASSIGNED),
    )

    # already created
    assert_that(TaskEventType.CREATED.is_legal_after(parent), is_(equal_to(False)))
    # already assigned
    assert_that(TaskEventType.ASSIGNED.is_legal_after(parent), is_(equal_to(False)))
    # may be scheduled
    assert_that(TaskEventType.SCHEDULED.is_legal_after(parent), is_(equal_to(True)))
    # may not be started until scheduled
    assert_that(TaskEventType.STARTED.is_legal_after(parent), is_(equal_to(False)))


def test_scheduled_before_assigned():
    """
    Scheduled can be evaluated before assigned.

    """
    parent = TaskEvent(
        event_type=TaskEventType.SCHEDULED,
        state=(TaskEventType.CREATED, TaskEventType.SCHEDULED),
    )

    # already created
    assert_that(TaskEventType.CREATED.is_legal_after(parent), is_(equal_to(False)))
    # may be assigned
    assert_that(TaskEventType.ASSIGNED.is_legal_after(parent), is_(equal_to(True)))
    # already scheduled
    assert_that(TaskEventType.SCHEDULED.is_legal_after(parent), is_(equal_to(False)))
    # may not be started until scheduled
    assert_that(TaskEventType.STARTED.is_legal_after(parent), is_(equal_to(False)))


def test_started_after_assigned_and_scheduled():
    """
    Started occurs after both assigned and scheduled.

    """
    parent = TaskEvent(
        event_type=TaskEventType.SCHEDULED,
        state=(TaskEventType.CREATED, TaskEventType.ASSIGNED, TaskEventType.SCHEDULED),
    )

    # already created
    assert_that(TaskEventType.CREATED.is_legal_after(parent), is_(equal_to(False)))
    # already assigned
    assert_that(TaskEventType.ASSIGNED.is_legal_after(parent), is_(equal_to(False)))
    # already scheduled
    assert_that(TaskEventType.SCHEDULED.is_legal_after(parent), is_(equal_to(False)))
    # may be started
    assert_that(TaskEventType.STARTED.is_legal_after(parent), is_(equal_to(True)))


def test_completed_or_canceled_after_started():
    """
    Completed and canceled can occur after starting.

    """
    parent = TaskEvent(
        event_type=TaskEventType.STARTED,
        state=(TaskEventType.STARTED,),
    )

    # already started
    assert_that(TaskEventType.STARTED.is_legal_after(parent), is_(equal_to(False)))
    # may be canceled
    assert_that(TaskEventType.CANCELED.is_legal_after(parent), is_(equal_to(True)))
    # may be completed
    assert_that(TaskEventType.COMPLETED.is_legal_after(parent), is_(equal_to(True)))


def test_repeated_reassign_or_rescheduled():
    """
    Reassign and reschedule can be repeated.

    """
    parent = TaskEvent(
        event_type=TaskEventType.REASSIGNED,
        state=(TaskEventType.STARTED, TaskEventType.REASSIGNED, TaskEventType.RESCHEDULED),
    )

    # already started
    assert_that(TaskEventType.STARTED.is_legal_after(parent), is_(equal_to(False)))
    # may be canceled
    assert_that(TaskEventType.CANCELED.is_legal_after(parent), is_(equal_to(True)))
    # may be completed
    assert_that(TaskEventType.COMPLETED.is_legal_after(parent), is_(equal_to(True)))
    # may be reassigned again
    assert_that(TaskEventType.REASSIGNED.is_legal_after(parent), is_(equal_to(True)))
    # may be reschedule again
    assert_that(TaskEventType.RESCHEDULED.is_legal_after(parent), is_(equal_to(True)))


def test_completed_is_terminal():
    """
    Nothing may happen after completion.

    """
    parent = TaskEvent(
        event_type=TaskEventType.COMPLETED,
        state=(TaskEventType.COMPLETED,),
    )

    # may not be completed again
    assert_that(TaskEventType.COMPLETED.is_legal_after(parent), is_(equal_to(False)))

    # may not be cancelled
    assert_that(TaskEventType.CANCELED.is_legal_after(parent), is_(equal_to(False)))
