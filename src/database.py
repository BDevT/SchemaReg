"""
Database configuration and session management
"""

from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from models import Base


class DatabaseManager:
    """Manages database connection and session lifecycle"""

    def __init__(self, db_type: str, db_file: str = None, db_url: str = None):
        self.db_type = db_type
        self.db_file = db_file
        self.db_url = db_url
        self.engine = None
        self.SessionLocal = None
        self._setup_database()

    def _setup_database(self):
        """Initialize database connection based on type"""
        if self.db_type == "sqlite":
            self._setup_sqlite()
        elif self.db_type == "postgres":
            self._setup_postgresql()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

    def _setup_sqlite(self):
        """Setup SQLite database"""
        db_path = Path(self.db_file)

        if db_path.exists():
            print(f"Using existing SQLite database: {db_path}")
        else:
            print(f"Creating new SQLite database: {db_path}")
            if not db_path.parent.exists():
                db_path.parent.mkdir(parents=True, exist_ok=True)

        database_url = f"sqlite:///{self.db_file}"
        self.engine = create_engine(
            database_url, connect_args={"check_same_thread": False}
        )

    def _setup_postgresql(self):
        """Setup PostgreSQL database"""
        if self.db_url:
            database_url = self.db_url

        self.engine = create_engine(database_url)
        print(f"Using PostgreSQL database: {database_url}")

    def create_tables(self):
        """Create all database tables"""
        try:
            print("Creating database tables...")
            Base.metadata.create_all(bind=self.engine)
            print("Database tables created successfully")

            if self.db_type == "sqlite":
                print(f"SQLite database ready at: {Path(self.db_file).absolute()}")
            elif self.db_type == "postgres":
                print(f"PostgreSQL database ready at: {self.db_url}")
            else:
                print("Unknown database type, tables created but check configuration")

        except Exception as e:
            print(f"Error creating database tables: {e}")
            raise

    def get_session(self) -> Session:
        """Get a new database session"""
        return self.SessionLocal()

    def get_db(self):
        """Dependency for FastAPI to get database session"""
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()
