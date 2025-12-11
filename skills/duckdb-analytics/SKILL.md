---
name: duckdb-analytics
description: >
  Run DuckDB SQL queries over CSV, Parquet, JSON/NDJSON, Excel (.xlsx), and database sources
  (Postgres, MySQL, SQLite, S3) for data exploration and analytics. Use when the user wants to:
  (1) Explore, filter, or aggregate data from tabular files, (2) Join multiple datasets,
  (3) Perform analytical SQL over local files or databases, (4) Get precise numeric/statistical
  results from large datasets. Prefer this over in-context reasoning for datasets with thousands
  of rows or complex joins/aggregations.
---

# DuckDB Analytics

Run analytical SQL queries over tabular data using DuckDB.

## Usage

**IMPORTANT**: On Windows, always change to the skill directory first to avoid path escaping issues:

```bash
cd "<skill_directory>" && echo '<json_request>' | uv run scripts/query_duckdb.py
```

Where `<skill_directory>` is the directory containing `scripts/query_duckdb.py`.

## Query Modes

### Direct File Queries (Simple)

Query files directly by path - DuckDB auto-detects format:

```json
{"query": "SELECT * FROM 'data.csv' LIMIT 10"}
```

```json
{"query": "SELECT * FROM read_parquet('s3://bucket/data/*.parquet') WHERE date > '2024-01-01'"}
```

### Aliased Sources (Multi-file joins)

Define sources to create named views for cleaner SQL:

```json
{
  "query": "SELECT s.*, p.category FROM sales s JOIN products p ON s.product_id = p.id",
  "sources": [
    {"type": "file", "alias": "sales", "path": "/data/sales.parquet"},
    {"type": "file", "alias": "products", "path": "/data/products.csv"}
  ]
}
```

## Request/Response Format

**Request:**
```json
{
  "query": "SQL statement",
  "sources": [...],
  "options": {
    "max_rows": 200,
    "max_bytes": 200000,
    "format": "json"
  }
}
```

**Options:**
- `max_rows`: Maximum rows to return (default: 200)
- `max_bytes`: Maximum response size in bytes (default: 200000)
- `format`: Output format - `json` (schema+rows), `records` (list of dicts), or `csv`

**Response (json format - default):**
```json
{
  "schema": [{"name": "col1", "type": "INTEGER"}],
  "rows": [[...]],
  "truncated": false,
  "warnings": [],
  "error": null
}
```

**Response (records format):**
```json
{
  "data": [{"col1": 1, "col2": "value"}, ...],
  "truncated": false,
  "warnings": [],
  "error": null
}
```

**Response (csv format):**
```json
{
  "csv": "col1,col2\n1,value\n...",
  "truncated": false,
  "warnings": [],
  "error": null
}
```

## File Types

DuckDB reads these directly (auto-detects format from extension):

| Format | Direct Query | Notes |
|--------|-------------|-------|
| CSV | `SELECT * FROM 'file.csv'` | Auto-detects delimiters, headers |
| TSV | `SELECT * FROM 'file.tsv'` | Tab-separated (auto-detected) |
| Parquet | `SELECT * FROM 'file.parquet'` | Projection/filter pushdown |
| JSON | `SELECT * FROM 'file.json'` | Array or newline-delimited |
| Excel | `SELECT * FROM 'file.xlsx'` | .xls not supported |

Glob patterns supported: `'logs/*.parquet'`, `'data/**/*.csv'`

## Source Types (for aliases)

**File:**
```json
{"type": "file", "alias": "data", "path": "/path/to/file.csv"}
```

**File with glob pattern:**
```json
{"type": "file", "alias": "logs", "path": "/logs/**/*.parquet"}
```

**TSV or custom delimiter:**
```json
{"type": "file", "alias": "data", "path": "/path/to/file.tsv"}
```
```json
{"type": "file", "alias": "data", "path": "/path/to/file.csv", "delimiter": "|"}
```

**Postgres:**
```json
{
  "type": "postgres", "alias": "events",
  "host": "host", "port": 5432, "database": "db",
  "user": "user", "password": "pass",
  "schema": "public", "table": "events"
}
```

**MySQL:**
```json
{
  "type": "mysql", "alias": "orders",
  "host": "host", "port": 3306, "database": "db",
  "user": "user", "password": "pass", "table": "orders"
}
```

**SQLite:**
```json
{"type": "sqlite", "alias": "data", "path": "/path/to/db.sqlite", "table": "tablename"}
```

**S3:**
```json
{
  "type": "s3", "alias": "logs",
  "url": "s3://bucket/path/*.parquet",
  "aws_region": "us-east-1",
  "aws_access_key_id": "...", "aws_secret_access_key": "..."
}
```

## Workflow

1. **Inspect schema**: `DESCRIBE SELECT * FROM 'file.csv'` or `SUMMARIZE SELECT * FROM 'file.csv'`
2. **Preview data**: `SELECT * FROM 'file.csv' LIMIT 5`
3. **Query with filters**: Use `WHERE`, `GROUP BY`, `LIMIT` to keep results compact
4. **Use aggregations for large data**: DuckDB handles millions of rows efficiently

## Utility Statements

These commands run directly without wrapping (useful for schema inspection):

- `DESCRIBE SELECT * FROM 'file.csv'` - Column names and types
- `SUMMARIZE SELECT * FROM 'file.csv'` - Statistics (min, max, avg, nulls, unique)
- `SHOW TABLES` - List available tables/views
- `EXPLAIN query` - Query execution plan

## Error Handling

If `error` is non-null: check column names, verify paths, propose corrected SQL.

If `truncated` is true: suggest more aggregated queries for full coverage.
