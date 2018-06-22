"""
Event type tests.

"""
from hamcrest import assert_that, calling, contains, contains_inanyorder, equal_to, is_, raises

from microcosm_eventsource.tests.fixtures import TaskEventType, FlexibleTaskEventType
from microcosm_eventsource.event_types import event_info, EventType
from microcosm_eventsource.transitioning import event


class IllegalEventType(EventType):
    # Event type with  non valid auto transition event
    CREATED = event_info()
    ASSIGNED = event_info(
        follows=event("CREATED"),
        auto_transition=True,
    )
    SCHEDULED = event_info(
        follows=event("CREATED"),
        auto_transition=True,
    )


def test_accumulate_state():
    """
    State accumulatin is either cummulative or a singleton.

    This is essentially a markov chain.

    """
    # normal condition: accumulate new state
    assert_that(
        TaskEventType.ASSIGNED.accumulate_state({
            TaskEventType.CREATED,
        }),
        contains(TaskEventType.ASSIGNED, TaskEventType.CREATED)
    )
    # do not duplicate state
    assert_that(
        TaskEventType.ASSIGNED.accumulate_state({
            TaskEventType.ASSIGNED,
            TaskEventType.CREATED,
        }),
        contains(TaskEventType.ASSIGNED, TaskEventType.CREATED)
    )
    # do not accumulate state
    assert_that(
        TaskEventType.STARTED.accumulate_state({
            TaskEventType.ASSIGNED,
            TaskEventType.CREATED,
            TaskEventType.SCHEDULED,
        }),
        contains(TaskEventType.STARTED)
    )


def test_next_version():
    """
    Compute next version.

    """
    assert_that(
        TaskEventType.CREATED.next_version(None),
        is_(equal_to(1)),
    )
    assert_that(
        TaskEventType.ASSIGNED.next_version(1),
        is_(equal_to(1)),
    )
    assert_that(
        TaskEventType.REVISED.next_version(1),
        is_(equal_to(2)),
    )


def test_is_initial():
    """
    Events with no following information may be initial.

    """
    assert_that(TaskEventType.CREATED.is_initial, is_(equal_to(True)))
    assert_that(TaskEventType.STARTED.is_initial, is_(equal_to(False)))


def test_is_initial_for_event_that_can_also_be_non_initial():
    """
    Events with no following information may be initial.

    """
    assert_that(FlexibleTaskEventType.CREATED.is_initial, is_(equal_to(True)))


def test_assign_before_scheduled():
    """
    Assigned can be evaluated before scheduled.

    """
    state = {
        TaskEventType.CREATED,
        TaskEventType.ASSIGNED,
    }

    # already created
    assert_that(TaskEventType.CREATED.may_transition(state), is_(equal_to(False)))
    # already assigned
    assert_that(TaskEventType.ASSIGNED.may_transition(state), is_(equal_to(False)))
    # may be scheduled
    assert_that(TaskEventType.SCHEDULED.may_transition(state), is_(equal_to(True)))
    # may not be started until scheduled
    assert_that(TaskEventType.STARTED.may_transition(state), is_(equal_to(False)))


def test_scheduled_before_assigned():
    """
    Scheduled can be evaluated before assigned.

    """
    state = {
        TaskEventType.CREATED,
        TaskEventType.SCHEDULED,
    }

    # already created
    assert_that(TaskEventType.CREATED.may_transition(state), is_(equal_to(False)))
    # may be assigned
    assert_that(TaskEventType.ASSIGNED.may_transition(state), is_(equal_to(True)))
    # already scheduled
    assert_that(TaskEventType.SCHEDULED.may_transition(state), is_(equal_to(False)))
    # may not be started until scheduled
    assert_that(TaskEventType.STARTED.may_transition(state), is_(equal_to(False)))


def test_started_after_assigned_and_scheduled():
    """
    Started occurs after both assigned and scheduled.

    """
    state = {
        TaskEventType.CREATED,
        TaskEventType.ASSIGNED,
        TaskEventType.SCHEDULED,
    }

    # already created
    assert_that(TaskEventType.CREATED.may_transition(state), is_(equal_to(False)))
    # already assigned
    assert_that(TaskEventType.ASSIGNED.may_transition(state), is_(equal_to(False)))
    # already scheduled
    assert_that(TaskEventType.SCHEDULED.may_transition(state), is_(equal_to(False)))
    # may be started
    assert_that(TaskEventType.STARTED.may_transition(state), is_(equal_to(True)))


def test_completed_or_canceled_after_started():
    """
    Completed and canceled can occur after starting.

    """
    state = {
        TaskEventType.STARTED,
    }

    # already started
    assert_that(TaskEventType.STARTED.may_transition(state), is_(equal_to(False)))
    # may be canceled
    assert_that(TaskEventType.CANCELED.may_transition(state), is_(equal_to(True)))
    # may be completed
    assert_that(TaskEventType.COMPLETED.may_transition(state), is_(equal_to(True)))


def test_repeated_reassign_or_rescheduled():
    """
    Reassign and reschedule can be repeated.

    """
    state = {
        TaskEventType.STARTED,
        TaskEventType.REASSIGNED,
        TaskEventType.RESCHEDULED,
    }

    # already started
    assert_that(TaskEventType.STARTED.may_transition(state), is_(equal_to(False)))
    # may be canceled
    assert_that(TaskEventType.CANCELED.may_transition(state), is_(equal_to(True)))
    # may be completed
    assert_that(TaskEventType.COMPLETED.may_transition(state), is_(equal_to(True)))
    # may be reassigned again
    assert_that(TaskEventType.REASSIGNED.may_transition(state), is_(equal_to(True)))
    # may be reschedule again
    assert_that(TaskEventType.RESCHEDULED.may_transition(state), is_(equal_to(True)))


def test_completed_is_terminal():
    """
    Nothing may happen after completion.

    """
    state = {
        TaskEventType.COMPLETED,
    }

    # may not be completed again
    assert_that(TaskEventType.COMPLETED.may_transition(state), is_(equal_to(False)))

    # may not be cancelled
    assert_that(TaskEventType.CANCELED.may_transition(state), is_(equal_to(False)))


def test_initial_states():
    """
    Find all initial states.

    """
    expected_states = [
        {TaskEventType.CREATED},
    ]
    assert_that(list(TaskEventType.intial_states()), contains_inanyorder(*expected_states))


def test_all_states():
    """
    Find all allowed states.

    """
    expected_states = [
        {TaskEventType.CREATED},
        {TaskEventType.STARTED},
        {TaskEventType.CANCELED},
        {TaskEventType.COMPLETED},
        {TaskEventType.ENDED},
        {TaskEventType.CREATED, TaskEventType.ASSIGNED},
        {TaskEventType.CREATED, TaskEventType.SCHEDULED},
        {TaskEventType.CREATED, TaskEventType.SCHEDULED, TaskEventType.ASSIGNED},
    ]
    assert_that(list(TaskEventType.all_states()), contains_inanyorder(*expected_states))


def test_all_states_and_events():
    """
    Find all allowed states.

    """
    states_and_events = [
        (state, event_type)
        for (state, event_type)
        in TaskEventType.all_states_and_events()
        if state in (
            {TaskEventType.CREATED},
            {TaskEventType.CREATED, TaskEventType.ASSIGNED, TaskEventType.SCHEDULED},
        )
    ]
    expected_states_and_events = [
        ({TaskEventType.CREATED}, TaskEventType.CREATED),
        ({TaskEventType.CREATED}, TaskEventType.REVISED),
        ({TaskEventType.CREATED, TaskEventType.ASSIGNED, TaskEventType.SCHEDULED}, TaskEventType.ASSIGNED),
        ({TaskEventType.CREATED, TaskEventType.ASSIGNED, TaskEventType.SCHEDULED}, TaskEventType.SCHEDULED),
    ]
    assert_that(states_and_events, contains_inanyorder(*expected_states_and_events))


def test_all_transitions():
    """
    Find all allowed transitions.

    """
    from_states = [
        {TaskEventType.CREATED},
        {TaskEventType.CREATED, TaskEventType.SCHEDULED},
    ]
    expected_states = [
        ({TaskEventType.CREATED}, {TaskEventType.CREATED}, TaskEventType.REVISED),
        ({TaskEventType.CREATED}, {TaskEventType.CREATED, TaskEventType.ASSIGNED}, TaskEventType.ASSIGNED),
        ({TaskEventType.CREATED}, {TaskEventType.CREATED, TaskEventType.SCHEDULED}, TaskEventType.SCHEDULED),
        ({TaskEventType.CREATED, TaskEventType.SCHEDULED}, {TaskEventType.CREATED}, TaskEventType.REVISED),
        (
            {TaskEventType.CREATED, TaskEventType.SCHEDULED},
            {TaskEventType.CREATED, TaskEventType.SCHEDULED, TaskEventType.ASSIGNED},
            TaskEventType.ASSIGNED,
        ),
    ]
    assert_that(list(TaskEventType.all_transitions(from_states)), contains_inanyorder(*expected_states))


def test_auto_transition_events():
    """
    Find all auto-transition events

    """
    assert_that(TaskEventType.auto_transition_events(), is_(equal_to([
        TaskEventType.ENDED,
    ])))
    assert_that(IllegalEventType.auto_transition_events(), is_(equal_to([
        IllegalEventType.ASSIGNED,
        IllegalEventType.SCHEDULED,
    ])))


def test_assert_only_valid_transitions():
    """
    Test that the state machine supports only valid transitions.

    """
    TaskEventType.assert_only_valid_transitions()
    assert_that(calling(IllegalEventType.assert_only_valid_transitions), raises(AssertionError))
