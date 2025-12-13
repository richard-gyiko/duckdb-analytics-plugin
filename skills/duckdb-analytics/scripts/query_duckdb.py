#!/usr/bin/env python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb>=1.4.3",
#   "polars[pyarrow]>=1.36.1",
#   "pydantic>=2.0",
#   "pyyaml>=6.0",
# ]
# ///
"""
DuckDB Analytics Engine for Claude Skill

Executes DuckDB SQL queries over various data sources (CSV, Parquet, JSON, Excel, etc.)
with safe limits and returns results as JSON.

Supports two modes:
1. Direct file queries: Use paths directly in SQL (e.g., SELECT * FROM 'data.csv')
2. Aliased sources: Define sources array to create named views for cleaner SQL

Secrets Management:
Sources can reference named secrets from a YAML file instead of embedding credentials.
Create a secrets.yaml file with credentials and reference them in sources.

Usage:
    echo '{"query": "SELECT * FROM '\''data.csv'\''"}' | uv run scripts/query_duckdb.py
    echo '{"query": "SELECT * FROM sales", "sources": [{"type": "file", "alias": "sales", "path": "data.csv"}]}' | uv run scripts/query_duckdb.py
    echo '{"query": "SELECT * FROM db", "secrets_file": "secrets.yaml", "sources": [{"type": "postgres", "alias": "db", "secret": "my_postgres", "table": "users"}]}' | uv run scripts/query_duckdb.py
"""

from __future__ import annotations

import json
import sys
import os
import re
from pathlib import Path
from typing import Optional, Dict, Any, Literal, Union

import duckdb
import polars as pl
import yaml
from pydantic import BaseModel, Field, model_validator, ConfigDict

DEFAULT_MAX_ROWS = 200
DEFAULT_MAX_BYTES = 200_000

# Patterns for utility statements that cannot be wrapped in SELECT * FROM (...)
UTILITY_PATTERNS = [
    re.compile(r"^\s*DESCRIBE\b", re.IGNORECASE),
    re.compile(r"^\s*SUMMARIZE\b", re.IGNORECASE),
    re.compile(r"^\s*SHOW\b", re.IGNORECASE),
    re.compile(r"^\s*PRAGMA\b", re.IGNORECASE),
    re.compile(r"^\s*EXPLAIN\b", re.IGNORECASE),
]

# Environment variable pattern for ${VAR_NAME} substitution
ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


# =============================================================================
# Pydantic Secret Models
# =============================================================================


class SecretBase(BaseModel):
    """Base model for all secret types with common validation."""

    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class PostgresSecret(SecretBase):
    """PostgreSQL database credentials."""

    type: Literal["postgres"]
    host: str
    port: int = 5432
    user: str
    password: str
    database: str
    schema_: str = Field(default="public", alias="schema")


class MySQLSecret(SecretBase):
    """MySQL database credentials."""

    type: Literal["mysql"]
    host: str
    port: int = 3306
    user: str
    password: str
    database: str


class S3Secret(SecretBase):
    """AWS S3 credentials."""

    type: Literal["s3"]
    key_id: str
    secret: str
    region: str
    scope: Optional[str] = None
    endpoint: Optional[str] = None
    use_ssl: bool = True


class GCSSecret(SecretBase):
    """Google Cloud Storage credentials."""

    type: Literal["gcs"]
    key_id: str
    secret: str
    region: Optional[str] = None
    scope: Optional[str] = None


class AzureSecret(SecretBase):
    """Azure Blob Storage credentials."""

    type: Literal["azure"]
    account_name: Optional[str] = None
    # CONFIG provider options
    account_key: Optional[str] = None
    connection_string: Optional[str] = None
    # SERVICE_PRINCIPAL provider
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    client_certificate_path: Optional[str] = None
    # CREDENTIAL_CHAIN options
    chain: Optional[str] = None
    provider: Optional[str] = None


class R2Secret(SecretBase):
    """Cloudflare R2 credentials."""

    type: Literal["r2"]
    key_id: str
    secret: str
    account_id: str
    region: Optional[str] = None
    scope: Optional[str] = None


class HTTPSecret(SecretBase):
    """HTTP/HTTPS authentication credentials."""

    type: Literal["http"]
    bearer_token: Optional[str] = None
    extra_http_headers: Optional[Dict[str, str]] = None
    http_proxy: Optional[str] = None
    http_proxy_username: Optional[str] = None
    http_proxy_password: Optional[str] = None


class IcebergSecret(SecretBase):
    """Iceberg REST Catalog credentials."""

    type: Literal["iceberg"]
    token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    oauth2_server_uri: Optional[str] = None
    oauth2_scope: Optional[str] = None


class DuckLakeSecret(SecretBase):
    """DuckLake lakehouse credentials."""

    type: Literal["ducklake"]
    metadata_path: str
    data_path: str
    metadata_parameters: Optional[Dict[str, str]] = None


class HuggingFaceSecret(SecretBase):
    """HuggingFace Hub credentials."""

    type: Literal["huggingface"]
    token: Optional[str] = None
    provider: Optional[str] = None


# Union type for all secret types
AnySecret = Union[
    PostgresSecret,
    MySQLSecret,
    S3Secret,
    GCSSecret,
    AzureSecret,
    R2Secret,
    HTTPSecret,
    IcebergSecret,
    DuckLakeSecret,
    HuggingFaceSecret,
]

# Map type name to secret class
SECRET_TYPE_MAP: Dict[str, type[SecretBase]] = {
    "postgres": PostgresSecret,
    "mysql": MySQLSecret,
    "s3": S3Secret,
    "gcs": GCSSecret,
    "azure": AzureSecret,
    "r2": R2Secret,
    "http": HTTPSecret,
    "iceberg": IcebergSecret,
    "ducklake": DuckLakeSecret,
    "huggingface": HuggingFaceSecret,
}


class SecretsConfig(BaseModel):
    """Top-level secrets configuration file structure."""

    secrets: Dict[str, AnySecret]
    options: Optional[Dict[str, Any]] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def parse_secret_types(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Parse secrets dict and dispatch to correct model based on 'type' field."""
        secrets_raw = values.get("secrets", {})
        parsed_secrets = {}

        for name, secret_data in secrets_raw.items():
            if isinstance(secret_data, SecretBase):
                # Already parsed
                parsed_secrets[name] = secret_data
                continue

            secret_type = secret_data.get("type")
            if secret_type not in SECRET_TYPE_MAP:
                raise ValueError(f"Unknown secret type: {secret_type}")

            secret_class = SECRET_TYPE_MAP[secret_type]
            parsed_secrets[name] = secret_class(**secret_data)

        values["secrets"] = parsed_secrets
        return values


# =============================================================================
# Secrets Helper Functions
# =============================================================================


def expand_env_vars(value: Any) -> Any:
    """
    Recursively expand ${VAR_NAME} patterns in strings.

    Raises ValueError if referenced env var doesn't exist.
    """
    if isinstance(value, str):
        matches = ENV_VAR_PATTERN.findall(value)
        result = value
        for var_name in matches:
            env_value = os.environ.get(var_name)
            if env_value is None:
                raise ValueError(f"Environment variable not set: {var_name}")
            result = result.replace(f"${{{var_name}}}", env_value)
        return result
    elif isinstance(value, dict):
        return {k: expand_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [expand_env_vars(item) for item in value]
    else:
        return value


def load_secrets_from_yaml(file_path: str) -> SecretsConfig:
    """
    Load and validate secrets from a YAML file using Pydantic.

    Supports environment variable substitution with ${VAR_NAME} syntax.

    Raises:
        FileNotFoundError: If secrets file doesn't exist
        yaml.YAMLError: If YAML is malformed
        ValidationError: If secrets don't match schema
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Secrets file not found: {file_path}")

    with open(path, "r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f)

    if raw_data is None:
        raise ValueError("Secrets file is empty")

    # Expand environment variables
    expanded_data = expand_env_vars(raw_data)

    # Parse and validate with Pydantic
    return SecretsConfig(**expanded_data)


def create_secret_sql(secret_name: str, secret: AnySecret) -> str:
    """Generate CREATE OR REPLACE SECRET SQL for a Pydantic secret model."""
    escaped_name = escape_identifier(secret_name)

    if isinstance(secret, PostgresSecret):
        # Build libpq connection string
        conn_str = (
            f"host={secret.host} port={secret.port} "
            f"user={secret.user} password={secret.password} "
            f"dbname={secret.database}"
        )
        return f"CREATE OR REPLACE SECRET {escaped_name} (TYPE postgres, CONNECTION_STRING {escape_string(conn_str)})"

    elif isinstance(secret, MySQLSecret):
        # MySQL uses a similar connection string format
        conn_str = (
            f"host={secret.host} port={secret.port} "
            f"user={secret.user} password={secret.password} "
            f"database={secret.database}"
        )
        return f"CREATE OR REPLACE SECRET {escaped_name} (TYPE mysql, CONNECTION_STRING {escape_string(conn_str)})"

    elif isinstance(secret, S3Secret):
        parts = [
            "TYPE s3",
            f"KEY_ID {escape_string(secret.key_id)}",
            f"SECRET {escape_string(secret.secret)}",
            f"REGION {escape_string(secret.region)}",
        ]
        if secret.endpoint:
            parts.append(f"ENDPOINT {escape_string(secret.endpoint)}")
        if not secret.use_ssl:
            parts.append("USE_SSL false")
        if secret.scope:
            parts.append(f"SCOPE {escape_string(secret.scope)}")
        return f"CREATE OR REPLACE SECRET {escaped_name} ({', '.join(parts)})"

    elif isinstance(secret, GCSSecret):
        parts = [
            "TYPE gcs",
            f"KEY_ID {escape_string(secret.key_id)}",
            f"SECRET {escape_string(secret.secret)}",
        ]
        if secret.region:
            parts.append(f"REGION {escape_string(secret.region)}")
        if secret.scope:
            parts.append(f"SCOPE {escape_string(secret.scope)}")
        return f"CREATE OR REPLACE SECRET {escaped_name} ({', '.join(parts)})"

    elif isinstance(secret, AzureSecret):
        parts = ["TYPE azure"]
        if secret.provider:
            parts.append(f"PROVIDER {secret.provider}")
        if secret.account_name:
            parts.append(f"ACCOUNT_NAME {escape_string(secret.account_name)}")
        if secret.account_key:
            parts.append(f"ACCOUNT_KEY {escape_string(secret.account_key)}")
        if secret.connection_string:
            parts.append(f"CONNECTION_STRING {escape_string(secret.connection_string)}")
        if secret.tenant_id:
            parts.append(f"TENANT_ID {escape_string(secret.tenant_id)}")
        if secret.client_id:
            parts.append(f"CLIENT_ID {escape_string(secret.client_id)}")
        if secret.client_secret:
            parts.append(f"CLIENT_SECRET {escape_string(secret.client_secret)}")
        if secret.client_certificate_path:
            parts.append(
                f"CLIENT_CERTIFICATE_PATH {escape_string(secret.client_certificate_path)}"
            )
        if secret.chain:
            parts.append(f"CHAIN {escape_string(secret.chain)}")
        return f"CREATE OR REPLACE SECRET {escaped_name} ({', '.join(parts)})"

    elif isinstance(secret, R2Secret):
        parts = [
            "TYPE r2",
            f"KEY_ID {escape_string(secret.key_id)}",
            f"SECRET {escape_string(secret.secret)}",
            f"ACCOUNT_ID {escape_string(secret.account_id)}",
        ]
        if secret.region:
            parts.append(f"REGION {escape_string(secret.region)}")
        if secret.scope:
            parts.append(f"SCOPE {escape_string(secret.scope)}")
        return f"CREATE OR REPLACE SECRET {escaped_name} ({', '.join(parts)})"

    elif isinstance(secret, HTTPSecret):
        parts = ["TYPE http"]
        if secret.bearer_token:
            parts.append(f"BEARER_TOKEN {escape_string(secret.bearer_token)}")
        if secret.extra_http_headers:
            # Format as MAP for DuckDB
            headers_str = ", ".join(
                f"{escape_string(k)}: {escape_string(v)}"
                for k, v in secret.extra_http_headers.items()
            )
            parts.append(f"EXTRA_HTTP_HEADERS MAP {{{headers_str}}}")
        if secret.http_proxy:
            parts.append(f"HTTP_PROXY {escape_string(secret.http_proxy)}")
        if secret.http_proxy_username:
            parts.append(
                f"HTTP_PROXY_USERNAME {escape_string(secret.http_proxy_username)}"
            )
        if secret.http_proxy_password:
            parts.append(
                f"HTTP_PROXY_PASSWORD {escape_string(secret.http_proxy_password)}"
            )
        return f"CREATE OR REPLACE SECRET {escaped_name} ({', '.join(parts)})"

    elif isinstance(secret, IcebergSecret):
        parts = ["TYPE iceberg"]
        if secret.token:
            parts.append(f"TOKEN {escape_string(secret.token)}")
        if secret.client_id:
            parts.append(f"CLIENT_ID {escape_string(secret.client_id)}")
        if secret.client_secret:
            parts.append(f"CLIENT_SECRET {escape_string(secret.client_secret)}")
        if secret.oauth2_server_uri:
            parts.append(f"OAUTH2_SERVER_URI {escape_string(secret.oauth2_server_uri)}")
        if secret.oauth2_scope:
            parts.append(f"OAUTH2_SCOPE {escape_string(secret.oauth2_scope)}")
        return f"CREATE OR REPLACE SECRET {escaped_name} ({', '.join(parts)})"

    elif isinstance(secret, DuckLakeSecret):
        parts = [
            "TYPE ducklake",
            f"METADATA_PATH {escape_string(secret.metadata_path)}",
            f"DATA_PATH {escape_string(secret.data_path)}",
        ]
        if secret.metadata_parameters:
            params_str = ", ".join(
                f"{escape_string(k)}: {escape_string(v)}"
                for k, v in secret.metadata_parameters.items()
            )
            parts.append(f"METADATA_PARAMETERS MAP {{{params_str}}}")
        return f"CREATE OR REPLACE SECRET {escaped_name} ({', '.join(parts)})"

    elif isinstance(secret, HuggingFaceSecret):
        parts = ["TYPE huggingface"]
        if secret.provider:
            parts.append(f"PROVIDER {secret.provider}")
        if secret.token:
            parts.append(f"TOKEN {escape_string(secret.token)}")
        return f"CREATE OR REPLACE SECRET {escaped_name} ({', '.join(parts)})"

    else:
        raise ValueError(f"Unknown secret type: {type(secret)}")


def register_duckdb_secret(
    con: duckdb.DuckDBPyConnection, secret_name: str, secret: AnySecret
) -> None:
    """Register a secret in DuckDB using CREATE SECRET."""
    sql = create_secret_sql(secret_name, secret)
    con.execute(sql)


def register_all_secrets(
    con: duckdb.DuckDBPyConnection, secrets_config: SecretsConfig
) -> None:
    """Register all secrets from a SecretsConfig in DuckDB."""
    for name, secret in secrets_config.secrets.items():
        register_duckdb_secret(con, name, secret)


# =============================================================================
# Utility Functions
# =============================================================================


def is_utility_statement(query: str) -> bool:
    """Check if query is a utility statement that shouldn't be wrapped."""
    return any(pattern.match(query) for pattern in UTILITY_PATTERNS)


def escape_identifier(name: str) -> str:
    """Escape a SQL identifier by quoting it."""
    return '"' + name.replace('"', '""') + '"'


def escape_string(s: str) -> str:
    """Escape a string literal for SQL."""
    return "'" + s.replace("'", "''") + "'"


def load_source(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    secrets: Optional[Dict[str, AnySecret]] = None,
) -> None:
    """
    Register a data source as a DuckDB view.

    This creates named views for data sources, allowing users to write
    cleaner SQL with aliases instead of full paths.

    If src contains a 'secret' field, it references a named secret from
    the secrets dict to get connection credentials.

    Note: DuckDB supports direct file queries (SELECT * FROM 'file.csv')
    without creating views. The view approach is used here to:
    - Provide simple aliases for complex paths
    - Enable multi-file joins with readable names
    - Keep user queries path-agnostic
    """
    stype = src.get("type")
    alias = src.get("alias")
    secret_name = src.get("secret")

    if not alias:
        raise ValueError("source missing 'alias'")

    escaped_alias = escape_identifier(alias)

    # If source references a secret, merge secret credentials into src
    if secret_name:
        if secrets is None:
            raise ValueError(
                f"Source '{alias}' references secret '{secret_name}' but no secrets file was provided"
            )
        if secret_name not in secrets:
            raise ValueError(
                f"Secret '{secret_name}' not found in secrets file"
            )
        secret = secrets[secret_name]
        # Merge secret fields into src (secret provides defaults, src can override)
        secret_dict = secret.model_dump(by_alias=True, exclude_none=True)
        # For sources that reference secrets, use secret credentials
        src = {**secret_dict, **src}

    if stype == "file":
        path = src["path"]
        escaped_path = escape_string(path)
        # Handle glob patterns - extract extension from pattern
        clean_path = path.rstrip("*").rstrip("/")
        ext = os.path.splitext(clean_path)[1].lower()
        
        # Get optional CSV parameters
        delimiter = src.get("delimiter") or src.get("sep")
        header = src.get("header")

        # DuckDB can auto-detect file types for common extensions
        # Using explicit functions for clarity and to support all extensions
        if ext in (".csv", ".tsv"):
            csv_opts = []
            if delimiter:
                csv_opts.append(f"sep={escape_string(delimiter)}")
            elif ext == ".tsv":
                csv_opts.append("sep='\\t'")
            if header is not None:
                csv_opts.append(f"header={str(header).lower()}")
            opts_str = ", " + ", ".join(csv_opts) if csv_opts else ""
            con.execute(
                f"CREATE OR REPLACE VIEW {escaped_alias} AS "
                f"SELECT * FROM read_csv({escaped_path}{opts_str})"
            )
        elif ext == ".parquet":
            # Parquet supports projection/filter pushdown automatically
            con.execute(
                f"CREATE OR REPLACE VIEW {escaped_alias} AS "
                f"SELECT * FROM read_parquet({escaped_path})"
            )
        elif ext in (".json", ".ndjson"):
            # read_json auto-detects array vs newline-delimited format
            con.execute(
                f"CREATE OR REPLACE VIEW {escaped_alias} AS "
                f"SELECT * FROM read_json({escaped_path})"
            )
        elif ext == ".xlsx":
            # Excel extension required; .xls files not supported
            con.execute("INSTALL excel; LOAD excel;")
            con.execute(
                f"CREATE OR REPLACE VIEW {escaped_alias} AS "
                f"SELECT * FROM read_xlsx({escaped_path})"
            )
        else:
            raise ValueError(f"Unsupported file extension: {ext}")

    elif stype == "postgres":
        con.execute("INSTALL postgres; LOAD postgres;")
        schema = src.get("schema", "public")
        conn_str = (
            f"host={src['host']} port={src.get('port', 5432)} "
            f"dbname={src['database']} user={src['user']} password={src['password']}"
        )
        table = src["table"]
        con.execute(
            f"CREATE OR REPLACE VIEW {escaped_alias} AS "
            f"SELECT * FROM postgres_scan({escape_string(conn_str)}, {escape_string(schema)}, {escape_string(table)})"
        )

    elif stype == "mysql":
        con.execute("INSTALL mysql; LOAD mysql;")
        conn_str = (
            f"host={src['host']} port={src.get('port', 3306)} "
            f"database={src['database']} user={src['user']} password={src['password']}"
        )
        table = src["table"]
        # Use ATTACH for MySQL (mysql_scan is deprecated)
        con.execute(f"ATTACH {escape_string(conn_str)} AS mysql_tmp (TYPE mysql, READ_ONLY)")
        con.execute(
            f"CREATE OR REPLACE VIEW {escaped_alias} AS "
            f"SELECT * FROM mysql_tmp.{escape_identifier(table)}"
        )
        con.execute("DETACH mysql_tmp")

    elif stype == "sqlite":
        con.execute("INSTALL sqlite; LOAD sqlite;")
        path = src["path"]
        table = src["table"]
        con.execute(
            f"CREATE OR REPLACE VIEW {escaped_alias} AS "
            f"SELECT * FROM sqlite_scan({escape_string(path)}, {escape_string(table)})"
        )

    elif stype == "s3":
        con.execute("INSTALL httpfs; LOAD httpfs;")
        if "aws_access_key_id" in src:
            con.execute(f"SET s3_access_key_id={escape_string(src['aws_access_key_id'])}")
            con.execute(f"SET s3_secret_access_key={escape_string(src['aws_secret_access_key'])}")
        if "aws_region" in src:
            con.execute(f"SET s3_region={escape_string(src['aws_region'])}")
        url = src["url"]
        # S3 URLs can point to parquet, csv, or json - infer from extension
        ext = os.path.splitext(url.split('?')[0].rstrip('*/'))[1].lower()
        if ext == ".csv":
            reader = "read_csv"
        elif ext == ".json" or ext == ".ndjson":
            reader = "read_json"
        else:
            reader = "read_parquet"  # default for S3
        con.execute(
            f"CREATE OR REPLACE VIEW {escaped_alias} AS "
            f"SELECT * FROM {reader}({escape_string(url)})"
        )

    else:
        raise ValueError(f"Unknown source type: {stype!r}")


def main() -> None:
    raw = sys.stdin.read()
    try:
        req = json.loads(raw)
    except Exception as e:
        print(json.dumps({"error": f"Invalid JSON input: {e}"}))
        return

    query = req.get("query")
    if not query:
        print(json.dumps({"error": "Missing 'query'"}))
        return

    sources = req.get("sources", [])
    options = req.get("options", {})
    secrets_file = req.get("secrets_file")
    max_rows = int(options.get("max_rows", DEFAULT_MAX_ROWS))
    max_bytes = int(options.get("max_bytes", DEFAULT_MAX_BYTES))
    output_format = options.get("format", "markdown")  # markdown, json, records, csv

    # Load secrets from YAML file if provided
    secrets_config: Optional[SecretsConfig] = None
    secrets_dict: Optional[Dict[str, AnySecret]] = None
    
    if secrets_file:
        try:
            secrets_config = load_secrets_from_yaml(secrets_file)
            secrets_dict = secrets_config.secrets
        except FileNotFoundError as e:
            print(json.dumps({"error": str(e)}))
            return
        except yaml.YAMLError as e:
            print(json.dumps({"error": f"Invalid YAML in secrets file: {e}"}))
            return
        except ValueError as e:
            print(json.dumps({"error": f"Secret validation error: {e}"}))
            return
        except Exception as e:
            # Pydantic validation errors
            error_msg = str(e)
            if hasattr(e, "errors"):
                # Format Pydantic validation errors nicely
                errors = e.errors()  # type: ignore
                error_details = []
                for err in errors:
                    loc = " -> ".join(str(x) for x in err.get("loc", []))
                    msg = err.get("msg", "")
                    error_details.append(f"{loc}: {msg}")
                error_msg = "Secret validation failed: " + "; ".join(error_details)
            print(json.dumps({"error": error_msg}))
            return

    try:
        con = duckdb.connect(database=":memory:")
        con.execute("PRAGMA memory_limit='1GB';")
        con.execute("PRAGMA threads=4;")

        # Register secrets with DuckDB if provided
        if secrets_config:
            register_all_secrets(con, secrets_config)

        # Register aliased sources as views
        for src in sources:
            load_source(con, src, secrets_dict)

        # Detect utility statements (DESCRIBE, SUMMARIZE, etc.) that can't be wrapped
        is_utility = is_utility_statement(query)
        
        if is_utility:
            # Execute utility statements directly with row limit
            res = con.execute(query)
        else:
            # Wrap regular queries for limit control
            wrapped_query = f"SELECT * FROM ({query}) AS q LIMIT {max_rows + 1}"
            res = con.execute(wrapped_query)
        
        # Convert to Polars DataFrame using .pl() for efficient formatting
        df = res.pl()
        
        truncated = False
        if len(df) > max_rows:
            df = df.head(max_rows)
            truncated = True

        # Format output based on requested format
        if output_format == "markdown":
            # Use Polars Config for clean markdown table output
            with pl.Config(
                tbl_formatting="MARKDOWN",
                tbl_hide_dataframe_shape=True,
                tbl_hide_column_data_types=True,
                set_tbl_width_chars=1000,
                tbl_rows=max_rows,
            ):
                table = str(df)
            
            result_parts = [table]
            if truncated:
                result_parts.append(f"\n*Results truncated to {len(df)} rows*")
            
            print("\n".join(result_parts))
            return
        elif output_format == "records":
            # Return as list of dicts (JSON records)
            out_obj = {
                "data": df.to_dicts(),
                "truncated": truncated,
                "warnings": [],
                "error": None,
            }
        elif output_format == "csv":
            # Return as CSV string
            out_obj = {
                "csv": df.write_csv(),
                "truncated": truncated,
                "warnings": [],
                "error": None,
            }
        else:
            # json format with schema + rows
            schema = [{"name": col, "type": str(dtype)} for col, dtype in zip(df.columns, df.dtypes)]
            rows = df.rows()
            out_obj = {
                "schema": schema,
                "rows": rows,
                "truncated": truncated,
                "warnings": [],
                "error": None,
            }

        encoded = json.dumps(out_obj, default=str)
        if len(encoded.encode("utf-8")) > max_bytes:
            # Progressively trim rows to fit size limit
            while len(df) > 0 and len(encoded.encode("utf-8")) > max_bytes:
                df = df.head(max(1, len(df) * 3 // 4))
                truncated = True
                if output_format == "records":
                    out_obj["data"] = df.to_dicts()
                elif output_format == "csv":
                    out_obj["csv"] = df.write_csv()
                else:
                    out_obj["rows"] = df.rows()
                out_obj["truncated"] = truncated
                encoded = json.dumps(out_obj, default=str)
            out_obj["warnings"].append(
                "Output truncated to respect max_bytes; try more aggregation or filters."
            )
            encoded = json.dumps(out_obj, default=str)

        print(encoded)

    except Exception as e:
        print(json.dumps({"error": str(e)}))


if __name__ == "__main__":
    main()
