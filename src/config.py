"""
Configuration management for JSON Schema Registry
"""

import argparse
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuration settings for the application"""
    db_type: str
    db_file: str
    db_url: Optional[str]
    host: str
    port: int


def parse_args() -> Config:
    """Parse command line arguments and return configuration"""
    parser = argparse.ArgumentParser(description="JSON Schema Registry API")
    
    parser.add_argument(
        "--db-type", 
        choices=["sqlite", "postgres"], 
        help="Database type to use (default: sqlite)"
    )
    
    parser.add_argument(
        "--db-file", 
        type=str, 
        help="SQLite database file path (default: data/schemas.db)"
    )
    
    parser.add_argument(
        "--db-url", 
        type=str, 
        help="PostgreSQL database URL (overrides default)"
    )
    
    parser.add_argument(
        "--host", 
        type=str, 
        help="Host to bind the server (default: 0.0.0.0)"
    )
    
    parser.add_argument(
        "--port", 
        type=int, 
        help="Port to bind the server (default: 8000)"
    )
    
    args = parser.parse_args()
    
    db_type = args.db_type or os.getenv("DB_TYPE", "sqlite")
    db_file = args.db_file or os.getenv("DB_FILE", "data/schemas.db")
    db_url = args.db_url or os.getenv("DB_URL")
    host = args.host or os.getenv("HOST", "0.0.0.0")
    port = args.port or int(os.getenv("PORT", "8000"))
    
    return Config(
        db_type=db_type,
        db_file=db_file,
        db_url=db_url,
        host=host,
        port=port
    )