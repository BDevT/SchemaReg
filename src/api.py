import json
import uuid
from typing import List
from fastapi import FastAPI, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from models import JSONSchemaDB, SchemaCreate, SchemaUpdate, SchemaResponse, db_to_response
from database import DatabaseManager


class SchemaRegistryAPI:
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.app = self._create_app()
        self._add_routes()
    
    def _create_app(self) -> FastAPI:
        app = FastAPI(
            title="JSON Schema Registry",
            description="A REST API for managing JSON schemas",
            version="1.0.0"
        )
        
        @app.on_event("startup")
        async def startup():
            self.db_manager.create_tables()
        
        return app
    
    def _add_routes(self):
        
        @self.app.get("/schemas", response_model=List[SchemaResponse])
        async def list_schemas(db: Session = Depends(self.db_manager.get_db)):
            schemas = db.query(JSONSchemaDB).all()
            return [db_to_response(schema) for schema in schemas]
        
        @self.app.get("/schemas/{schema_uuid}", response_model=SchemaResponse)
        async def get_schema_by_uuid(schema_uuid: str, db: Session = Depends(self.db_manager.get_db)):
            schema = db.query(JSONSchemaDB).filter(JSONSchemaDB.schema_uuid == schema_uuid).first()
            if not schema:
                raise HTTPException(status_code=404, detail="Schema not found")
            return db_to_response(schema)
        
        @self.app.get("/schemas/name/{schema_name}", response_model=SchemaResponse)
        async def get_schema_by_name(schema_name: str, db: Session = Depends(self.db_manager.get_db)):
            schema = db.query(JSONSchemaDB).filter(JSONSchemaDB.name == schema_name).first()
            if not schema:
                raise HTTPException(status_code=404, detail="Schema not found")
            return db_to_response(schema)
        
        @self.app.get("/search", response_model=List[SchemaResponse])
        async def search_schemas(
            q: str = Query(..., description="Search schema names"),
            db: Session = Depends(self.db_manager.get_db)
        ):
            schemas = db.query(JSONSchemaDB).filter(
                JSONSchemaDB.name.ilike(f"%{q}%")
            ).all()
            
            return [db_to_response(schema) for schema in schemas]
        
        @self.app.get("/search/content", response_model=List[SchemaResponse])
        async def search_schema_content(
            q: str = Query(..., description="Search within schema content"),
            db: Session = Depends(self.db_manager.get_db)
        ):
            schemas = db.query(JSONSchemaDB).filter(
                JSONSchemaDB.schema_content.op("@>")(text(f"'\"{q}\"'"))
            ).all()
            
            return [db_to_response(schema) for schema in schemas]
        
        @self.app.get("/schemas/by-property/{property_name}", response_model=List[SchemaResponse])
        async def get_schemas_by_property(
            property_name: str,
            db: Session = Depends(self.db_manager.get_db)
        ):
            schemas = db.query(JSONSchemaDB).filter(
                JSONSchemaDB.schema_content.op("?")(property_name)
            ).all()
            
            return [db_to_response(schema) for schema in schemas]        
        
        @self.app.post("/schemas", response_model=SchemaResponse, status_code=201)
        async def add_schema(schema: SchemaCreate, db: Session = Depends(self.db_manager.get_db)):
            existing = db.query(JSONSchemaDB).filter(JSONSchemaDB.name == schema.name).first()
            if existing:
                raise HTTPException(status_code=400, detail="Schema name already exists")
            
            schema_uuid = str(uuid.uuid4())
            
            db_schema = JSONSchemaDB(
                schema_uuid=schema_uuid,
                name=schema.name,
                description=schema.description,
                schema_content=schema.schema_content
            )
            db.add(db_schema)
            db.commit()
            db.refresh(db_schema)
            
            print(f"Schema added: {db_schema.name} (UUID: {db_schema.schema_uuid})")
            
            return db_to_response(db_schema)
        
        @self.app.put("/schemas/{schema_uuid}", response_model=SchemaResponse)
        async def update_schema(
            schema_uuid: str, 
            schema_update: SchemaUpdate, 
            db: Session = Depends(self.db_manager.get_db)
        ):
            schema = db.query(JSONSchemaDB).filter(JSONSchemaDB.schema_uuid == schema_uuid).first()
            if not schema:
                raise HTTPException(status_code=404, detail="Schema not found")
            
            if schema_update.name and schema_update.name != schema.name:
                existing_name = db.query(JSONSchemaDB).filter(
                    JSONSchemaDB.name == schema_update.name,
                    JSONSchemaDB.schema_uuid != schema_uuid
                ).first()
                if existing_name:
                    raise HTTPException(status_code=400, detail="Schema name already exists")
            
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
        
        @self.app.delete("/schemas/{schema_uuid}")
        async def remove_schema(schema_uuid: str, db: Session = Depends(self.db_manager.get_db)):
            schema = db.query(JSONSchemaDB).filter(JSONSchemaDB.schema_uuid == schema_uuid).first()
            if not schema:
                raise HTTPException(status_code=404, detail="Schema not found")
            
            db.delete(schema)
            db.commit()
            
            print(f"Schema deleted: {schema.name} (UUID: {schema.schema_uuid})")
            
            return {"message": "Schema deleted successfully"}
 
    def get_app(self) -> FastAPI:
        return self.app