"""
Test transition functions.

"""
from hamcrest import assert_that, is_, equal_to

from microcosm_eventsource.transitioning import (
    all_of,
    any_of,
    but_not,
    event,
    nothing,
)
from microcosm_eventsource.tests.fixtures import TaskEventType


def test_any_of():
    transition = any_of(
        "CREATED",
        "ASSIGNED",
    )

    assert_that(
        bool(transition),
        is_(equal_to(True)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED, TaskEventType.ASSIGNED]),
        is_(equal_to(True)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED]),
        is_(equal_to(True)),
    )


def test_all_of():
    transition = all_of(
        "CREATED",
        "ASSIGNED",
    )

    assert_that(
        bool(transition),
        is_(equal_to(True)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED, TaskEventType.ASSIGNED]),
        is_(equal_to(True)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED]),
        is_(equal_to(False)),
    )


def test_but_not():
    transition = but_not(
        "ASSIGNED",
    )

    assert_that(
        bool(transition),
        is_(equal_to(True)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED, TaskEventType.ASSIGNED]),
        is_(equal_to(False)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED]),
        is_(equal_to(True)),
    )


def test_event():
    transition = event(
        "ASSIGNED",
    )

    assert_that(
        bool(transition),
        is_(equal_to(True)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED, TaskEventType.ASSIGNED]),
        is_(equal_to(True)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED]),
        is_(equal_to(False)),
    )


def test_nothing():
    transition = nothing()

    assert_that(
        bool(transition),
        is_(equal_to(False)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED, TaskEventType.ASSIGNED]),
        is_(equal_to(False)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED]),
        is_(equal_to(False)),
    )
