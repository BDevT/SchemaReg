#!/usr/bin/env python3
"""
Simple JSON Schema Registry API
A FastAPI application for managing JSON schemas with database storage.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
import json
import os
import argparse
import sys
from pathlib import Path
import uuid

from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import func
import uvicorn

# Parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="JSON Schema Registry API")
    parser.add_argument(
        "--db-type", 
        choices=["sqlite", "postgres"], 
        default="sqlite",
        help="Database type to use (default: postgres)"
    )
    parser.add_argument(
        "--db-file", 
        type=str, 
        default="data/schemas.db",
        help="SQLite database file path (default: schemas.db)"
    )
    parser.add_argument(
        "--db-url", 
        type=str, 
        help="PostgreSQL database URL (overrides default)"
    )
    parser.add_argument(
        "--host", 
        type=str, 
        default="0.0.0.0",
        help="Host to bind the server (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=8000,
        help="Port to bind the server (default: 8000)"
    )


    return parser.parse_args()


args = None
engine = None
SessionLocal = None
Base = declarative_base()

def setup_database():
    global engine, SessionLocal
    
    if args.db_type == "sqlite":
        db_path = Path(args.db_file)
        
        if db_path.exists():
            print(f"Using existing SQLite database: {db_path}")
        else:
            print(f"Creating new SQLite database: {db_path}")
            if not db_path.parent.exists():
                db_path.parent.mkdir(parents=True, exist_ok=True)
        
        database_url = f"sqlite:///{args.db_file}"
        engine = create_engine(
            database_url, 
            connect_args={"check_same_thread": False}
        )
        
    else:
        if args.db_url:
            database_url = args.db_url
        else:
            database_url = os.getenv(
                "DATABASE_URL", 
                "postgresql://username:password@localhost:5432/json_schemas"
            )
        engine = create_engine(database_url)
        print(f"Using PostgreSQL database: {database_url}")
    
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class JSONSchemaDB(Base):
    __tablename__ = "json_schemas"
    
    id = Column(Integer, primary_key=True, index=True)
    schema_uuid = Column(String(36), nullable=False, unique=True, index=True)  # UUID as string
    name = Column(String(255), nullable=False, unique=True, index=True)
    description = Column(Text)
    schema_content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=func.now())

class SchemaCreate(BaseModel):
    name: str
    description: Optional[str] = None
    schema_content: str

class SchemaUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    schema_content: Optional[str] = None

class SchemaResponse(BaseModel):
    schema_uuid: str  # UUID as string
    name: str
    description: Optional[str]
    schema_content: str
    
    class Config:
        from_attributes = True


app = None

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



def create_app():
    global app
    
    app = FastAPI(
        title="JSON Schema Registry",
        description="A REST API for managing JSON schemas",
        version="1.0.0"
    )

    @app.on_event("startup")
    async def startup():
        """Create database tables and setup"""
        try:
            print("Creating database tables...")
            Base.metadata.create_all(bind=engine)
            print("Database tables created successfully")
            
            if args.db_type == "sqlite":
                print(f"SQLite database ready at: {Path(args.db_file).absolute()}")
            
        except Exception as e:
            print(f"Error during startup: {e}")
            sys.exit(1)
    
    return app

def db_to_response(db_schema: JSONSchemaDB) -> SchemaResponse:
    return SchemaResponse(
        schema_uuid=db_schema.schema_uuid,
        name=db_schema.name,
        description=db_schema.description,
        schema_content=db_schema.schema_content  # Return as string directly
    )

def main():
    global args, app
    args = parse_args()
    
    setup_database()
    app = create_app()
    add_routes(app)
    
    print(f"Starting JSON Schema Registry API...")
    print(f"Server will run on: http://{args.host}:{args.port}")
    print(f"API Documentation: http://{args.host}:{args.port}/docs")
    print(f"Database type: {args.db_type}")
    
    uvicorn.run(
        app, 
        host=args.host, 
        port=args.port
    )

def add_routes(app):
    """Add all API routes to the app"""
    
    @app.get("/schemas", response_model=List[SchemaResponse])
    async def list_schemas(db: Session = Depends(get_db)):
        """List all JSON schemas"""
        schemas = db.query(JSONSchemaDB).all()
        return [db_to_response(schema) for schema in schemas]

    @app.get("/schemas/{schema_name}", response_model=SchemaResponse)
    async def get_schema(schema_name: str, db: Session = Depends(get_db)):
        """Get a specific JSON schema by UUID"""
        schema = db.query(JSONSchemaDB).filter(JSONSchemaDB.name == schema_name).first()
        if not schema:
            raise HTTPException(status_code=404, detail="Schema not found")
        return db_to_response(schema)

    @app.post("/schemas", response_model=SchemaResponse, status_code=201)
    async def add_schema(schema: SchemaCreate, db: Session = Depends(get_db)):
        """Add a new JSON schema"""
        # Check if name already exists
        existing = db.query(JSONSchemaDB).filter(JSONSchemaDB.name == schema.name).first()
        if existing:
            raise HTTPException(status_code=400, detail="Schema name already exists")
        
        # # Validate that schema_content is a valid JSON string
        # try:
        #     json.loads(schema.schema_content)  # Validate JSON string
        # except json.JSONDecodeError:
        #     raise HTTPException(status_code=400, detail="Invalid JSON schema format")
        
        # Generate a new UUID for the schema
        schema_uuid = str(uuid.uuid4())
        
        db_schema = JSONSchemaDB(
            schema_uuid=schema_uuid,
            name=schema.name,
            description=schema.description,
            schema_content=schema.schema_content  # Store as string directly
        )
        db.add(db_schema)
        db.commit()
        db.refresh(db_schema)

        print(f"Schema added: {db_schema.name} (UUID: {db_schema.schema_uuid})")
        
        return db_to_response(db_schema)

    @app.put("/schemas/{schema_uuid}", response_model=SchemaResponse)
    async def update_schema(schema_uuid: str, schema_update: SchemaUpdate, db: Session = Depends(get_db)):
        """Update an existing JSON schema by UUID"""
        # Find the schema by UUID
        schema = db.query(JSONSchemaDB).filter(JSONSchemaDB.schema_uuid == schema_uuid).first()
        if not schema:
            raise HTTPException(status_code=404, detail="Schema not found")
        
        # Check if name is being updated and if it would conflict
        if schema_update.name and schema_update.name != schema.name:
            existing_name = db.query(JSONSchemaDB).filter(
                JSONSchemaDB.name == schema_update.name,
                JSONSchemaDB.schema_uuid != schema_uuid
            ).first()
            if existing_name:
                raise HTTPException(status_code=400, detail="Schema name already exists")
        
        # Update fields if provided
        if schema_update.name is not None:
            schema.name = schema_update.name
        if schema_update.description is not None:
            schema.description = schema_update.description
        if schema_update.schema_content is not None:
            schema.schema_content = schema_update.schema_content
        
        db.commit()
        db.refresh(schema)
        
        print(f"Schema updated: {schema.name} (UUID: {schema.schema_uuid})")
        
        return db_to_response(schema)

    @app.delete("/schemas/{schema_uuid}")
    async def remove_schema(schema_uuid: str, db: Session = Depends(get_db)):
        """Remove a JSON schema by UUID"""
        schema = db.query(JSONSchemaDB).filter(JSONSchemaDB.schema_uuid == schema_uuid).first()
        if not schema:
            raise HTTPException(status_code=404, detail="Schema not found")
        
        db.delete(schema)
        db.commit()
        return {"message": "Schema deleted successfully"}

    @app.get("/search", response_model=List[SchemaResponse])
    async def search_schemas(
        q: str = Query(..., description="Search schema names"),
        db: Session = Depends(get_db)
    ):
        """Search JSON schemas by name"""
        schemas = db.query(JSONSchemaDB).filter(
            JSONSchemaDB.name.ilike(f"%{q}%")
        ).all()
        
        return [db_to_response(schema) for schema in schemas]

if __name__ == "__main__":
    main()
