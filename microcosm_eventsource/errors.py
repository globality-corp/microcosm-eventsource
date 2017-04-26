"""
State machine errors.

"""


class ConcurrentStateConflictError(Exception):
    @property
    def status_code(self):
        return 409


class IllegalStateTransitionError(Exception):
    @property
    def status_code(self):
        return 403


class IllegalInitialStateError(IllegalStateTransitionError):
    pass
