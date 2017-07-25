"""
Test accumulation.

"""
from hamcrest import assert_that, contains

from microcosm_eventsource.accumulation import (
    addition,
    alias,
    compose,
    current,
    keep,
    difference,
    union,
)
from microcosm_eventsource.tests.fixtures import TaskEventType


def test_alias():
    state = {
        TaskEventType.STARTED,
    }

    assert_that(
        alias(TaskEventType.CREATED)(state, TaskEventType.REVISED),
        contains(TaskEventType.CREATED),
    )


def test_alias_as_string():
    state = {
        TaskEventType.STARTED,
    }

    assert_that(
        alias("CREATED")(state, TaskEventType.REVISED),
        contains(TaskEventType.CREATED),
    )


def test_current():
    state = {
        TaskEventType.ASSIGNED,
    }

    assert_that(
        current()(state, TaskEventType.SCHEDULED),
        contains(TaskEventType.SCHEDULED),
    )


def test_keep():
    state = {
        TaskEventType.ASSIGNED,
    }

    assert_that(
        keep()(state, TaskEventType.SCHEDULED),
        contains(TaskEventType.ASSIGNED),
    )


def test_addition():
    state = {
        TaskEventType.CREATED,
    }

    assert_that(
        addition(TaskEventType.ASSIGNED)(state, TaskEventType.ASSIGNED),
        contains(TaskEventType.ASSIGNED, TaskEventType.CREATED),
    )


def test_addition_as_string():
    state = {
        TaskEventType.CREATED,
    }

    assert_that(
        addition("ASSIGNED")(state, TaskEventType.ASSIGNED),
        contains(TaskEventType.ASSIGNED, TaskEventType.CREATED),
    )


def test_difference():
    state = {
        TaskEventType.CREATED,
        TaskEventType.ASSIGNED,
    }

    assert_that(
        difference(TaskEventType.ASSIGNED)(state, TaskEventType.CANCELED),
        contains(TaskEventType.CREATED),
    )


def test_difference_as_string():
    state = {
        TaskEventType.CREATED,
        TaskEventType.ASSIGNED,
    }

    assert_that(
        difference("ASSIGNED")(state, TaskEventType.CANCELED),
        contains(TaskEventType.CREATED),
    )


def test_union():
    state = {
        TaskEventType.CREATED,
    }

    assert_that(
        union()(state, TaskEventType.ASSIGNED),
        contains(TaskEventType.ASSIGNED, TaskEventType.CREATED),
    )


def test_compose():
    state = {
        TaskEventType.STARTED,
        TaskEventType.REASSIGNED,
    }

    assert_that(
        compose(
            difference("STARTED"),
            addition("COMPLETED"),
        )(state, TaskEventType.COMPLETED),
        contains(TaskEventType.COMPLETED, TaskEventType.REASSIGNED),
    )
