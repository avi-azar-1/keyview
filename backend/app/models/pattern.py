from pydantic import BaseModel


class PatternCreate(BaseModel):
    pattern: str


class Pattern(BaseModel):
    id: str
    pattern: str
