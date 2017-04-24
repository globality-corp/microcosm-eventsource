"""
State machine errors.

"""


class ConcurrentStateConflictError(Exception):
    @property
    def status_code(self):
        return 409


class IllegalInitialStateError(Exception):
    @property
    def status_code(self):
        return 403


class IllegalStateTransitionError(Exception):
    @property
    def status_code(self):
        return 403
