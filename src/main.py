#!/usr/bin/env python3
"""
Business Metadata API
A FastAPI application for managing business metadata with database storage.
"""

import sys
import uvicorn
from config import parse_args
from database import DatabaseManager
from api import BusinessMetadataAPI


def main():
    """Main application entry point"""
    try:
        config = parse_args()

        db_manager = DatabaseManager(
            db_type=config.db_type, db_file=config.db_file, db_url=config.db_url
        )

        api = BusinessMetadataAPI(db_manager)
        app = api.get_app()

        print("Starting Business Metadata API...")
        print(f"Server will run on: http://{config.host}:{config.port}")
        print(f"API Documentation: http://{config.host}:{config.port}/docs")
        print(f"Database type: {config.db_type}")

        uvicorn.run(app, host=config.host, port=config.port)

    except KeyboardInterrupt:
        print("\nShutting down server...")
        sys.exit(0)
    except Exception as e:
        print(f"Failed to start server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
