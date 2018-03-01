"""
Transition mini-language.

"""
from abc import ABCMeta


def normalize(value):
    """
    Normalize string values into id functions.

    """
    if callable(value):
        return value
    return event(value)


class Transition(metaclass=ABCMeta):

    def __call__(self, cls, state):
        """
        Is this `state` allowed to transition (follow) from the given event type `cls`?

        :returns: boolean

        """
        return False

    def __bool__(self):
        """
        Is this state non-initial?

        :returns: boolean

        """
        return True


class Nothing(Transition):

    def __call__(self, cls, state):
        return not(state)

    def __bool__(self):
        return False

    __nonzero__ = __bool__


class AllOf(Transition):

    def __init__(self, *args):
        self.args = args

    def __call__(self, cls, state):
        return all(normalize(arg)(cls, state) for arg in self.args)

    def __bool__(self):
        return any(arg for arg in self.args)

    __nonzero__ = __bool__


class AnyOf(Transition):

    def __init__(self, *args):
        self.args = args

    def __call__(self, cls, state):
        return any(normalize(arg)(cls, state) for arg in self.args)

    def __bool__(self):
        return all(arg for arg in self.args)

    __nonzero__ = __bool__


class ButNot(Transition):

    def __init__(self, arg):
        self.arg = arg

    def __call__(self, cls, state):
        return not normalize(self.arg)(cls, state)

    def __bool__(self):
        return True

    __nonzero__ = __bool__


class Event(Transition):

    def __init__(self, name):
        self.name = name

    def __call__(self, cls, state):
        return cls[self.name] in state

    def __bool__(self):
        return True

    __nonzero__ = __bool__


def event(name):
    """
    Mini grammar to match a specific event type.

    """
    return Event(name)


def any_of(*args):
    """
    Mini grammar to match a list of event types.

    """
    return AnyOf(*args)


def all_of(*args):
    """
    Mini grammar to match a list of event types.

    """
    return AllOf(*args)


def but_not(arg):
    """
    Mini grammar to not match a specific event type.

    """
    return ButNot(arg)


def nothing():
    """
    Mini grammar that matches nothing.

    """
    return Nothing()
