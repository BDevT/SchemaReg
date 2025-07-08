SchemaReg# Schema Reg

A FastAPI-based JSON Schema Registry for storing and serving JSON schemas.

## Quick Start

### Using Docker (Recommended)

```bash
# SQLite (default)
docker run -p 8000:8000 -v $(pwd)/data:/app/data ghcr.io/bdevt/schemareg:latest
```

### Local Development

```bash
# Install dependencies with uv
uv pip install -e .

# Run with SQLite
python main.py
```

## API Documentation

Once running, visit:
- Interactive docs: http://localhost:8000/docs

## CLI Options

```bash
python main.py [OPTIONS]

Options:
  --db-type {sqlite,postgres}    Database type (default: sqlite)
  --db-file TEXT                 SQLite database file path (default: data/schemas.db)
  --db-url TEXT                  PostgreSQL database URL
  --host TEXT                    Host to bind (default: 0.0.0.0)
  --port INTEGER                 Port to bind (default: 8000)
```

## Examples

```bash
# SQLite with custom file
python main.py --db-file /path/to/schemas.db

# Custom host and port
python main.py --host 127.0.0.1 --port 9000
```
