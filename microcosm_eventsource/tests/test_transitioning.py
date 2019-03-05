"""
Test transition functions.

"""
from hamcrest import assert_that, equal_to, is_

from microcosm_eventsource.tests.fixtures import TaskEventType
from microcosm_eventsource.transitioning import (
    all_of,
    any_of,
    but_not,
    event,
    nothing,
)


def test_any_of():
    transition = any_of(
        event("CREATED"),
        event("ASSIGNED"),
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
    assert_that(
        transition(TaskEventType, []),
        is_(equal_to(False)),
    )


def test_any_of_nothing():
    transition = any_of(
        nothing(),
        event("CREATED"),
    )

    assert_that(
        bool(transition),
        is_(equal_to(False)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED, TaskEventType.ASSIGNED]),
        is_(equal_to(True)),
    )
    assert_that(
        transition(TaskEventType, [TaskEventType.CREATED]),
        is_(equal_to(True)),
    )
    assert_that(
        transition(TaskEventType, []),
        is_(equal_to(True)),
    )


def test_all_of():
    transition = all_of(
        event("CREATED"),
        event("ASSIGNED"),
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
    assert_that(
        transition(TaskEventType, []),
        is_(equal_to(False)),
    )


def test_all_of_some_nothing():
    transition = all_of(
        nothing(),
        event("CREATED"),
        event("ASSIGNED"),
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
        is_(equal_to(False)),
    )
    assert_that(
        transition(TaskEventType, []),
        is_(equal_to(False)),
    )


def test_all_of_nothing():
    transition = all_of(
        nothing(),
        nothing(),
    )

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
    assert_that(
        transition(TaskEventType, []),
        is_(equal_to(True)),
    )


def test_but_not():
    transition = but_not(
        event("ASSIGNED"),
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


def test_but_not_nothing():
    transition = but_not(nothing())

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
    assert_that(
        transition(TaskEventType, []),
        is_(equal_to(False)),
    )


def test_event():
    transition = event("ASSIGNED")

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
