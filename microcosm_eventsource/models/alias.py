"""
Column aliasing.

"""


class ColumnAlias:
    """
    Descriptor to reference a column by a well known alias.

    """
    def __init__(self, name):
        self.name = name

    def __get__(self, cls, owner):
        return getattr(cls or owner, self.name)

    def __set__(self, cls, value):
        return setattr(cls, self.name, value)
