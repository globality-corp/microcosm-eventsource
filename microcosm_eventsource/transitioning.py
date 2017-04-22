"""
Transition mini-language.

"""


class Nothing(object):

    def __call__(self, cls, state):
        return False

    def __bool__(self):
        return False

    __nonzero__ = __bool__


def event(name):
    """
    Mini grammar to match a specific event type.

    """
    return lambda cls, state: cls[name] in state


def normalize(value):
    """
    Normalize string values into id functions.

    """
    if callable(value):
        return value
    return event(value)


def any_of(*args):
    """
    Mini grammar to match a list of event types.

    """
    return lambda cls, state: any(normalize(item)(cls, state) for item in args)


def all_of(*args):
    """
    Mini grammar to match a list of event types.

    """
    return lambda cls, state: all(normalize(item)(cls, state) for item in args)


def but_not(func):
    """
    Mini grammar to not match a specific event type.

    """
    return lambda cls, state: not normalize(func)(cls, state)


def nothing():
    """
    Mini grammar that matches nothing.

    """
    return Nothing()
