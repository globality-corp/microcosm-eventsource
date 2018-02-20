"""
Event resources.

"""
from marshmallow import fields, Schema

from microcosm_flask.paging import PageSchema


class EventSchema(Schema):
    clock = fields.Integer(allow_none=True)
    createdTimestamp = fields.Float(
        attribute="created_timestamp",
        required=True,
        allow_none=True,
    )
    id = fields.UUID(
        required=True,
    )
    version = fields.Integer(
        required=True,
    )
    parentId = fields.UUID(
        attribute="parent_id",
        required=False,
        allow_none=True,
    )
    updatedTimestamp = fields.Float(
        attribute="created_timestamp",
        required=True,
        allow_none=True,
    )


class SearchEventSchema(PageSchema):
    clock = fields.Integer()
    min_clock = fields.Integer()
    max_clock = fields.Integer()
    parent_id = fields.UUID()
    sort_by_clock = fields.Boolean()
    version = fields.Integer()
