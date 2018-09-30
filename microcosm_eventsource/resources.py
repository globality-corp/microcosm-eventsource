"""
Event resources.

"""
from marshmallow import fields, Schema, validates_schema, ValidationError
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
    sort_clock_in_ascending_order = fields.Boolean()
    version = fields.Integer()

    @validates_schema
    def validate(self, obj):
        if obj.get("sort_clock_in_ascending_order") and not obj.get("sort_by_clock"):
            raise ValidationError(
                "sort_by_clock must be set if sort_clock_in_ascending_order is set",
                field_names=[
                    "sort_by_clock",
                    "sort_clock_in_ascending_order",
                ],
            )
