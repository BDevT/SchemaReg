"""
Database models and Pydantic schemas for JSON Schema Registry
"""

from typing import Optional
from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB
from pydantic import BaseModel
import json

Base = declarative_base()


class JSONSchemaDB(Base):
    """SQLAlchemy model for JSON schemas"""

    __tablename__ = "json_schemas"

    id = Column(Integer, primary_key=True, index=True)
    schema_uuid = Column(String(36), nullable=False, unique=True, index=True)
    name = Column(String(255), nullable=False, unique=True, index=True)
    description = Column(Text)
    schema_content = Column(JSONB, nullable=False)
    created_at = Column(DateTime, default=func.now())


class SchemaCreate(BaseModel):
    """Pydantic model for creating a new schema"""

    name: str
    description: Optional[str] = None
    schema_content: str


class SchemaUpdate(BaseModel):
    """Pydantic model for updating an existing schema"""

    name: Optional[str] = None
    description: Optional[str] = None
    schema_content: Optional[str] = None


class SchemaResponse(BaseModel):
    """Pydantic model for schema responses"""

    schema_uuid: str
    name: str
    description: Optional[str]
    schema_content: str

    class Config:
        from_attributes = True


class JSONDatasetDB(Base):
    """SQLAlchemy model for JSON datasets"""

    __tablename__ = "json_datasets"

    id = Column(Integer, primary_key=True, index=True)
    dataset_uuid = Column(String(36), nullable=False, unique=True, index=True)
    schema_uuid = Column(String(36), nullable=False, index=True)
    name = Column(String(255), nullable=False, unique=True, index=True)
    description = Column(Text)
    dataset_content = Column(JSONB, nullable=False)
    created_at = Column(DateTime, default=func.now())


class DatasetCreate(BaseModel):
    """Pydantic model for creating a new dataset"""

    name: str
    schema_uuid: str
    description: Optional[str] = None
    dataset_content: str


class DatasetUpdate(BaseModel):
    """Pydantic model for updating an existing dataset"""

    name: Optional[str] = None
    schema_uuid: Optional[str] = None
    description: Optional[str] = None
    dataset_content: Optional[str] = None


class DatasetResponse(BaseModel):
    """Pydantic model for dataset responses"""

    dataset_uuid: str
    schema_uuid: str
    name: str
    description: Optional[str]
    dataset_content: str

    class Config:
        from_attributes = True


def json_schema_db_to_response(db_schema: JSONSchemaDB) -> SchemaResponse:
    """Convert database model to response model"""
    return SchemaResponse(
        schema_uuid=db_schema.schema_uuid,
        name=db_schema.name,
        description=db_schema.description,
        schema_content=json.dumps(db_schema.schema_content),
    )


def json_dataset_db_to_response(db_dataset: JSONDatasetDB) -> DatasetResponse:
    """Convert dataset database model to response model"""
    return DatasetResponse(
        dataset_uuid=db_dataset.dataset_uuid,
        schema_uuid=db_dataset.schema_uuid,
        name=db_dataset.name,
        description=db_dataset.description,
        dataset_content=json.dumps(db_dataset.dataset_content),
    )
