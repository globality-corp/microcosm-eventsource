"""
Event resources.

"""
from typing import Optional
from uuid import UUID

from microcosm_fastapi.conventions.schemas import BaseSchema
from pydantic import validator, ValidationError


class EventSchema(BaseSchema):
    clock: Optional[int] = None
    created_timestamp: float
    id: UUID
    version: int
    parent_id: Optional[bool] = None


class SearchEventSchema(BaseSchema):
    clock: Optional[int]
    min_clock: Optional[int]
    max_clock: Optional[int]
    parent_id: Optional[UUID]
    sort_by_clock: Optional[bool]
    sort_clock_in_ascending_order: Optional[bool]
    version: Optional[int]

    @validator("sort_clock_in_ascending_order")
    def validate_sort_clock_in_ascending_order(cls, field_value, values, field, config):
        if field_value and not values.get("sort_by_clock"):
            raise ValidationError("sort_by_clock must be set if sort_clock_in_ascending_order is set")

