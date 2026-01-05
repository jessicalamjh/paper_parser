"""Pydantic schemas for PubMed/PMC article data structures."""

from datetime import date
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import Any

class ArticleIDs(BaseModel):
    """Article IDs.
    
    This model fills in specific article IDs of interest but also allows for 
    other IDs to be present. The IDs named specifically must be strings or 
    None. Other IDs can be of any type.
    """
    pmc: str | None = Field(None, pattern=r"^PMC\d+$")
    pmid: str | None = Field(None, pattern=r"^\d+$")
    doi: str | None = None
    publisher_id: str | None = None

    model_config = ConfigDict(
        extra='allow',
        str_strip_whitespace=True,
    )

    @model_validator(mode="before")
    @classmethod
    def normalise_id_types(cls, data: Any):
        if "publisher-id" in data:
            data["publisher_id"] = data["publisher-id"]
            del data["publisher-id"]
        if "pmcid" in data:
            data["pmc"] = data["pmcid"]
            del data["pmcid"]
        return data


    @field_validator("pmc", mode="before")
    @classmethod
    def ensure_pmc_pattern(cls, v: Any):
        digits = ''.join(c for c in v if c.isdigit())
        return f"PMC{digits}"


class Date(BaseModel):
    """Publication date.
    
    The date is composed of year, month, and day fields, but all three are 
    optional. 
    """
    year: str | None = None
    month: str | None = None
    day: str | None = None
    
    @model_validator(mode="after")
    def validate_date(cls, values):
        y = "2000" if values.year is None else values.year
        m = "01" if values.month is None else values.month
        d = "01" if values.day is None else values.day

        try:
            date(int(y), int(m), int(d))
        except ValueError:
            raise ValueError(f"Invalid date combination: {y}-{m}-{d}")

        if values.year:
            values.year = f"{int(values.year):04d}"
        if values.month:
            values.month = f"{int(values.month):02d}"
        if values.day:
            values.day = f"{int(values.day):02d}"
        return values


class Reference(BaseModel):
    """Reference information."""
    referenced_ids: ArticleIDs | None = None

    ref_idx: int
    ref_id: str | None = None
    label: str | None = None

    model_config = ConfigDict(
        extra='allow',
        str_strip_whitespace=True,
    )


class Article(BaseModel):
    """Complete article metadata extracted from PMC XML."""
    schema_version: str = "1.0.0"

    article_type: str | None = None
    article_ids: ArticleIDs | None = None
    pub_date: Date | None = None
    subjects: list[str] = None

    title: str | None = None
    subtitle: str | None = None
    abstract: str | None = None

    references: list[Reference] = None
    
    model_config = ConfigDict(
        extra='allow',
        str_strip_whitespace=True,
    )
