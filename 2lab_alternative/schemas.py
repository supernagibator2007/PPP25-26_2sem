from pydantic import BaseModel
from typing import Optional


class PostBase(BaseModel):
    title: str
    body: str
    author: str


class PostUpdatePATCH(BaseModel):
    title: Optional[str] = None
    body: Optional[str] = None
    author: Optional[str] = None


class PostResponse(BaseModel):
    id: int
    title: str
    body: str
    author: str
    extracted_time: str

    class Config:
        from_attributes = True
