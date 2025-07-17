import json
import uuid
from typing import List
from fastapi import FastAPI, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import String, cast, text
from models import DatasetCreate, DatasetResponse, DatasetUpdate, JSONDatasetDB, JSONSchemaDB, SchemaCreate, SchemaUpdate, SchemaResponse, json_dataset_db_to_response, json_schema_db_to_response
from database import DatabaseManager
from sqlalchemy import text, or_

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
        
        tags_metadata = [
            {
                "name": "JSON Schemas",
                "description": "Operations with JSON schemas.",
            },
            {
                "name": "JSON Datasets",
                "description": "Operations with JSON datasets.",
            },
        ]
        
        @self.app.get("/schemas", response_model=List[SchemaResponse], tags=["JSON Schemas"])
        async def list_schemas(db: Session = Depends(self.db_manager.get_db)):
            schemas = db.query(JSONSchemaDB).all()
            return [json_schema_db_to_response(schema) for schema in schemas]
        
        @self.app.get("/schemas/search", response_model=List[SchemaResponse], tags=["JSON Schemas"])
        async def search_schemas(
            q: str = Query(..., description="Search term to find in schema content"),
            db: Session = Depends(self.db_manager.get_db)
        ):
            conditions = [
                JSONSchemaDB.name.ilike(f'%{q}%'),
                JSONSchemaDB.description.ilike(f'%{q}%'),
                cast(JSONSchemaDB.schema_content, String).op('~*')(q)
            ]

            schemas = db.query(JSONSchemaDB).filter(or_(*conditions)).all()
            return [json_schema_db_to_response(schema) for schema in schemas]        

        @self.app.post("/schemas", response_model=SchemaResponse, status_code=201, tags=["JSON Schemas"])
        async def add_schema(schema: SchemaCreate, db: Session = Depends(self.db_manager.get_db)):
            existing = db.query(JSONSchemaDB).filter(JSONSchemaDB.name == schema.name).first()
            if existing:
                raise HTTPException(status_code=400, detail="Schema name already exists")
            
            schema_uuid = str(uuid.uuid4())

            db_schema = JSONSchemaDB(
                schema_uuid=schema_uuid,
                name=schema.name,
                description=schema.description,
                schema_content= json.loads(schema.schema_content)
            )
            db.add(db_schema)
            db.commit()
            db.refresh(db_schema)
            
            print(f"Schema added: {db_schema.name} (UUID: {db_schema.schema_uuid})")
            
            return json_schema_db_to_response(db_schema)
        
        @self.app.get("/schemas/{schema_uuid}", response_model=SchemaResponse, tags=["JSON Schemas"])
        async def get_schema_by_uuid(schema_uuid: str, db: Session = Depends(self.db_manager.get_db)):
            schema = db.query(JSONSchemaDB).filter(JSONSchemaDB.schema_uuid == schema_uuid).first()
            if not schema:
                raise HTTPException(status_code=404, detail="Schema not found")
            return json_schema_db_to_response(schema)
        
        @self.app.put("/schemas/{schema_uuid}", response_model=SchemaResponse, tags=["JSON Schemas"])
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
                schema.schema_content = json.loads(schema_update.schema_content)

            db.commit()
            db.refresh(schema)
            
            print(f"Schema updated: {schema.name} (UUID: {schema.schema_uuid})")
            
            return json_schema_db_to_response(schema)
        
        @self.app.delete("/schemas/{schema_uuid}", tags=["JSON Schemas"])
        async def remove_schema(schema_uuid: str, db: Session = Depends(self.db_manager.get_db)):
            schema = db.query(JSONSchemaDB).filter(JSONSchemaDB.schema_uuid == schema_uuid).first()
            if not schema:
                raise HTTPException(status_code=404, detail="Schema not found")
            
            db.delete(schema)
            db.commit()
            
            print(f"Schema deleted: {schema.name} (UUID: {schema.schema_uuid})")
            
            return {"message": "Schema deleted successfully"}
        
        @self.app.get("/schemas/name/{schema_name}", response_model=SchemaResponse, tags=["JSON Schemas"])
        async def get_schema_by_name(schema_name: str, db: Session = Depends(self.db_manager.get_db)):
            schema = db.query(JSONSchemaDB).filter(JSONSchemaDB.name == schema_name).first()
            if not schema:
                raise HTTPException(status_code=404, detail="Schema not found")
            return json_schema_db_to_response(schema)
        

        @self.app.get("/datasets", response_model=List[DatasetResponse], tags=["JSON Datasets"])
        async def list_datasets(db: Session = Depends(self.db_manager.get_db)):
            datasets = db.query(JSONDatasetDB).all()
            return [json_dataset_db_to_response(dataset) for dataset in datasets]
        
        @self.app.get("/datasets/search", response_model=List[DatasetResponse], tags=["JSON Datasets"])
        async def search_datasets(
        q: str = Query(..., description="Search term to find in dataset content"),
        db: Session = Depends(self.db_manager.get_db)
        ):
            conditions = [
                JSONDatasetDB.name.ilike(f'%{q}%'),
                JSONDatasetDB.description.ilike(f'%{q}%'),
                cast(JSONDatasetDB.dataset_content, String).op('~*')(q)
            ]
            
            datasets = db.query(JSONDatasetDB).filter(or_(*conditions)).all()
            return [json_dataset_db_to_response(dataset) for dataset in datasets]

        @self.app.post("/datasets", response_model=DatasetResponse, status_code=201, tags=["JSON Datasets"])
        async def add_dataset(dataset: DatasetCreate, db: Session = Depends(self.db_manager.get_db)):
            existing = db.query(JSONDatasetDB).filter(JSONDatasetDB.name == dataset.name).first()
            if existing:
                raise HTTPException(status_code=400, detail="Dataset name already exists")
            
            dataset_uuid = str(uuid.uuid4())
            db_dataset = JSONDatasetDB(
                dataset_uuid=dataset_uuid,
                schema_uuid=dataset.schema_uuid,
                name=dataset.name,
                description=dataset.description,
                dataset_content=json.loads(dataset.dataset_content)
            )

            db.add(db_dataset)
            db.commit()
            db.refresh(db_dataset)

            print(f"Dataset created: {db_dataset.name} (UUID: {db_dataset.dataset_uuid})")
            return json_dataset_db_to_response(db_dataset)
                
        @self.app.get("/datasets/{dataset_uuid}", response_model=DatasetResponse, tags=["JSON Datasets"])
        async def get_dataset_by_uuid(dataset_uuid: str, db: Session = Depends(self.db_manager.get_db)):
            dataset = db.query(JSONDatasetDB).filter(JSONDatasetDB.dataset_uuid == dataset_uuid).first()
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")
            return json_dataset_db_to_response(dataset)
        
        @self.app.get("/datasets/name/{dataset_name}", response_model=DatasetResponse, tags=["JSON Datasets"])
        async def get_dataset_by_name(dataset_name: str, db: Session = Depends(self.db_manager.get_db)):
            dataset = db.query(JSONDatasetDB).filter(JSONDatasetDB.name == dataset_name).first()
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")
            return json_dataset_db_to_response(dataset)
        
        @self.app.put("/datasets/{dataset_uuid}", response_model=DatasetResponse, tags=["JSON Datasets"])
        async def update_dataset(
            dataset_uuid: str, 
            dataset_update: DatasetUpdate, 
            db: Session = Depends(self.db_manager.get_db)
        ):
            dataset = db.query(JSONDatasetDB).filter(JSONDatasetDB.dataset_uuid == dataset_uuid).first()
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")
            
            if dataset_update.name and dataset_update.name != dataset.name:
                existing_name = db.query(JSONDatasetDB).filter(
                    JSONDatasetDB.name == dataset_update.name,
                    JSONDatasetDB.dataset_uuid != dataset_uuid
                ).first()
                if existing_name:
                    raise HTTPException(status_code=400, detail="Dataset name already exists")
            
            if dataset_update.name is not None:
                dataset.name = dataset_update.name
            if dataset_update.description is not None:
                dataset.description = dataset_update.description
            if dataset_update.schema_uuid is not None:
                dataset.schema_uuid = dataset_update.schema_uuid
            if dataset_update.dataset_content is not None:
                dataset.dataset_content = json.loads(dataset_update.dataset_content)

            db.commit()
            db.refresh(dataset)

            print(f"Dataset updated: {dataset.name} (UUID: {dataset.dataset_uuid})")
            return json_dataset_db_to_response(dataset)
        
        @self.app.delete("/datasets/{dataset_uuid}", tags=["JSON Datasets"])
        async def remove_dataset(dataset_uuid: str, db: Session = Depends(self.db_manager.get_db)):
            dataset = db.query(JSONDatasetDB).filter(JSONDatasetDB.dataset_uuid == dataset_uuid).first()
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")
            
            db.delete(dataset)
            db.commit()

            print(f"Dataset deleted: {dataset.name} (UUID: {dataset.dataset_uuid})")
            return {"message": "Dataset deleted successfully"}


 
    def get_app(self) -> FastAPI:
        return self.app