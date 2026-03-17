from __future__ import annotations
from typing import Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T")


class PageMeta(BaseModel):
    page:      int
    page_size: int
    total:     int
    has_next:  bool


class PaginatedResponse(BaseModel, Generic[T]):
    total:     int
    page:      int
    page_size: int
    has_next:  bool
    items:     list[T]
