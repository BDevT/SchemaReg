"""
Database models and Pydantic schemas for JSON Schema Registry
"""

from datetime import datetime
from typing import Optional, Dict, Any
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
    schema_content: Dict[str, Any]


class SchemaUpdate(BaseModel):
    """Pydantic model for updating an existing schema"""
    name: Optional[str] = None
    description: Optional[str] = None
    schema_content: Optional[Dict[str, Any]] = None


class SchemaResponse(BaseModel):
    """Pydantic model for schema responses"""
    schema_uuid: str
    name: str
    description: Optional[str]
    schema_content: Dict[str, Any]
    
    class Config:
        from_attributes = True


def db_to_response(db_schema: JSONSchemaDB) -> SchemaResponse:
    """Convert database model to response model"""
    return SchemaResponse(
        schema_uuid=db_schema.schema_uuid,
        name=db_schema.name,
        description=db_schema.description,
        schema_content=db_schema.schema_content
    )