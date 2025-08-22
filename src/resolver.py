import json
import uuid
from fastapi import HTTPException
from sqlalchemy.orm import Session
from models import (
    JSONSchemaDB,
)

def _find_all_refs(obj, refs=None):
    """Find all $ref values in the schema object"""
    if refs is None:
        refs = set()
    
    if isinstance(obj, dict):
        if "$ref" in obj:
            refs.add(obj["$ref"])
        for value in obj.values():
            _find_all_refs(value, refs)
    elif isinstance(obj, list):
        for item in obj:
            _find_all_refs(item, refs)
    
    return refs

def _replace_refs(obj, ref_cache):
    """Replace all $ref values with resolved schemas"""
    if isinstance(obj, dict):
        if "$ref" in obj:
            ref_uuid = obj["$ref"]
            if ref_uuid in ref_cache:
                return _replace_refs(ref_cache[ref_uuid], ref_cache)
            else:
                raise HTTPException(
                    status_code=404,
                    detail=f"Reference not found in cache: {ref_uuid}"
                )
        else:
            resolved_dict = {}
            for key, value in obj.items():
                resolved_dict[key] = _replace_refs(value, ref_cache)
            return resolved_dict
    elif isinstance(obj, list):
        resolved_list = []
        for item in obj:
            resolved_list.append(_replace_refs(item, ref_cache))
        return resolved_list
    else:
        return obj

async def _resolve_schema_refs(schema_obj, db: Session, schema_chain=None):
    """
    Recursively resolve all $ref references in a JSON schema.
    Disallows recursive references (e.g., Person -> Children -> Person).
    
    Args:
        schema_obj: The schema object to resolve
        db: Database session
        schema_chain: List of schema UUIDs in the current resolution chain
    
    Returns:
        The schema with all $ref references resolved
    """
    if schema_chain is None:
        schema_chain = []
    
    all_refs = _find_all_refs(schema_obj)
    
    if not all_refs:
        return schema_obj
    
    # Check for recursive references
    for ref_uuid in all_refs:
        try:
            uuid.UUID(ref_uuid)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid UUID format in $ref: {ref_uuid}"
            )
        
        if ref_uuid in schema_chain:
            raise HTTPException(
                status_code=400, 
                detail=f"Recursive reference detected: {' -> '.join(schema_chain)} -> {ref_uuid}"
            )
    
    ref_cache = {}
    for ref_uuid in all_refs:
        referenced_schema = (
            db.query(JSONSchemaDB)
            .filter(JSONSchemaDB.schema_uuid == ref_uuid)
            .first()
        )
        
        if not referenced_schema:
            raise HTTPException(
                status_code=404, 
                detail=f"Referenced schema not found: {ref_uuid}"
            )
        
        try:
            referenced_content = (
                json.loads(referenced_schema.schema_content) 
                if isinstance(referenced_schema.schema_content, str) 
                else referenced_schema.schema_content
            )
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid JSON in referenced schema {ref_uuid}: {str(e)}"
            )
    
        new_schema_chain = schema_chain + [ref_uuid]
        resolved_content = await _resolve_schema_refs(referenced_content, db, new_schema_chain)
        ref_cache[ref_uuid] = resolved_content
    
    return _replace_refs(schema_obj, ref_cache)