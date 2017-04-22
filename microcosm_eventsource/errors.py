"""
State machine errors.

"""


class IllegalInitialStateError(Exception):
    @property
    def status_code(self):
        return 403


class IllegalStateTransitionError(Exception):
    @property
    def status_code(self):
        return 403
