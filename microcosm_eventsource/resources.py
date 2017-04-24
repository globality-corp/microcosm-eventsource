"""
Event resources.

"""
from marshmallow import fields, Schema

from microcosm_flask.paging import PageSchema


class EventSchema(Schema):
    clock = fields.Integer(
        required=True,
    )
    createdTimestamp = fields.Float(
        attribute="created_timestamp",
        required=True,
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
    )
    updatedTimestamp = fields.Float(
        attribute="created_timestamp",
        required=True,
    )


class SearchEventSchema(PageSchema):
    clock = fields.Integer()
    min_clock = fields.Integer()
    max_clock = fields.Integer()
    parent_id = fields.UUID()
    version = fields.Integer()
