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

**IMPORTANT - Windows Shell Escaping:**

1. Always `cd` to the skill directory first
2. Use **double quotes** for the echo command with escaped inner quotes (`\"`)
3. Use **forward slashes** in file paths (DuckDB accepts them on Windows)

```bash
cd "<skill_directory>" && echo "{\"query\": \"SELECT * FROM 'D:/path/to/file.csv'\"}" | uv run scripts/query_duckdb.py
```

**WRONG** (causes Invalid JSON escape errors):
```bash
echo '{"query": "SELECT * FROM 'D:\path\file.csv'"}' | ...
```

**CORRECT**:
```bash
echo "{\"query\": \"SELECT * FROM 'D:/path/file.csv'\"}" | ...
```

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
- `format`: Output format - `markdown` (default, LLM-friendly table), `json` (schema+rows), `records` (list of dicts), or `csv`

**Response (markdown format - default):**
```
| column1 | column2 | column3 |
|---|---|---|
| value1 | value2 | value3 |
| value4 | value5 | value6 |
```

**Response (json format):**
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

## Secrets Management

Credentials can be managed securely using a YAML secrets file instead of embedding them in requests.

### Quick Start

1. Create `secrets.yaml`:
```yaml
secrets:
  my_postgres:
    type: postgres
    host: db.example.com
    port: 5432
    user: analyst
    password: "${PGPASSWORD}"  # Uses environment variable
    database: analytics
    schema: public
```

2. Set environment variables for sensitive values:
```bash
export PGPASSWORD="your_secure_password"
```

3. Use in request:
```json
{
  "query": "SELECT * FROM customers LIMIT 10",
  "secrets_file": "D:/path/to/secrets.yaml",
  "sources": [{
    "type": "postgres",
    "alias": "customers",
    "secret": "my_postgres",
    "table": "customers"
  }]
}
```

### Secrets File Format

```yaml
secrets:
  # PostgreSQL
  postgres_prod:
    type: postgres
    host: prod-db.example.com
    port: 5432
    user: analytics_user
    password: "${PGPASSWORD}"
    database: analytics_db
    schema: public

  # MySQL
  mysql_warehouse:
    type: mysql
    host: mysql.example.com
    port: 3306
    user: warehouse_user
    password: "${MYSQL_PASSWORD}"
    database: warehouse

  # AWS S3
  s3_data_lake:
    type: s3
    key_id: "${AWS_ACCESS_KEY_ID}"
    secret: "${AWS_SECRET_ACCESS_KEY}"
    region: us-east-1
    scope: "s3://my-data-lake/*"

  # Azure Blob Storage
  azure_lake:
    type: azure
    account_name: mystorageaccount
    account_key: "${AZURE_STORAGE_KEY}"

  # Google Cloud Storage
  gcs_backup:
    type: gcs
    key_id: "${GCS_KEY_ID}"
    secret: "${GCS_SECRET}"
    region: us-central1

  # Cloudflare R2
  r2_storage:
    type: r2
    key_id: "${R2_KEY_ID}"
    secret: "${R2_SECRET}"
    account_id: "${R2_ACCOUNT_ID}"

  # HTTP/HTTPS with Bearer Token
  api_secret:
    type: http
    bearer_token: "${API_TOKEN}"

  # HuggingFace
  hf_secret:
    type: huggingface
    token: "${HF_TOKEN}"
```

### Supported Secret Types

| Type | Required Fields | Optional Fields |
|------|-----------------|-----------------|
| `postgres` | host, user, password, database | port (5432), schema (public) |
| `mysql` | host, user, password, database | port (3306) |
| `s3` | key_id, secret, region | scope, endpoint, use_ssl |
| `gcs` | key_id, secret | region, scope |
| `azure` | (varies by auth method) | account_name, account_key, connection_string, tenant_id, client_id, client_secret, provider |
| `r2` | key_id, secret, account_id | region, scope |
| `http` | (none required) | bearer_token, extra_http_headers, http_proxy |
| `huggingface` | (none required) | token, provider |
| `iceberg` | (none required) | token, client_id, client_secret, oauth2_server_uri |
| `ducklake` | metadata_path, data_path | metadata_parameters |

### Environment Variable Substitution

Use `${VAR_NAME}` syntax to reference environment variables:

```yaml
secrets:
  my_db:
    type: postgres
    host: "${DB_HOST}"
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"
    database: "${DB_NAME}"
```

This keeps sensitive credentials out of the YAML file and allows different values per environment.

### Multi-Database Example

Join data across multiple databases:

```json
{
  "query": "SELECT a.*, o.status FROM analytics a JOIN ops o ON a.id = o.order_id",
  "secrets_file": "D:/config/secrets.yaml",
  "sources": [
    {
      "type": "postgres",
      "alias": "analytics",
      "secret": "analytics_postgres",
      "table": "transactions"
    },
    {
      "type": "mysql",
      "alias": "ops",
      "secret": "operational_mysql",
      "table": "orders"
    }
  ]
}
```

### Security Best Practices

1. **Keep secrets.yaml in .gitignore** - Never commit credentials to version control
2. **Use environment variables** - Store actual passwords in env vars, not the YAML file
3. **Restrict file permissions** - Use `chmod 600 secrets.yaml` on Unix systems
4. **Use scoped secrets** - Limit S3/GCS/Azure secrets to specific bucket prefixes
