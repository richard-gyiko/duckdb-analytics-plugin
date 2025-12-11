#!/usr/bin/env python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb>=1.4.3",
#   "polars[pyarrow]>=1.36.1",
# ]
# ///
"""
DuckDB Analytics Engine for Claude Skill

Executes DuckDB SQL queries over various data sources (CSV, Parquet, JSON, Excel, etc.)
with safe limits and returns results as JSON.

Supports two modes:
1. Direct file queries: Use paths directly in SQL (e.g., SELECT * FROM 'data.csv')
2. Aliased sources: Define sources array to create named views for cleaner SQL

Usage:
    echo '{"query": "SELECT * FROM '\''data.csv'\''"}' | uv run scripts/query_duckdb.py
    echo '{"query": "SELECT * FROM sales", "sources": [{"type": "file", "alias": "sales", "path": "data.csv"}]}' | uv run scripts/query_duckdb.py
"""

import json
import sys
import os
import re
import duckdb
import polars as pl

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


def is_utility_statement(query: str) -> bool:
    """Check if query is a utility statement that shouldn't be wrapped."""
    return any(pattern.match(query) for pattern in UTILITY_PATTERNS)


def escape_identifier(name: str) -> str:
    """Escape a SQL identifier by quoting it."""
    return '"' + name.replace('"', '""') + '"'


def escape_string(s: str) -> str:
    """Escape a string literal for SQL."""
    return "'" + s.replace("'", "''") + "'"


def load_source(con: duckdb.DuckDBPyConnection, src: dict) -> None:
    """
    Register a data source as a DuckDB view.

    This creates named views for data sources, allowing users to write
    cleaner SQL with aliases instead of full paths.

    Note: DuckDB supports direct file queries (SELECT * FROM 'file.csv')
    without creating views. The view approach is used here to:
    - Provide simple aliases for complex paths
    - Enable multi-file joins with readable names
    - Keep user queries path-agnostic
    """
    stype = src.get("type")
    alias = src.get("alias")
    if not alias:
        raise ValueError("source missing 'alias'")

    escaped_alias = escape_identifier(alias)

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
    max_rows = int(options.get("max_rows", DEFAULT_MAX_ROWS))
    max_bytes = int(options.get("max_bytes", DEFAULT_MAX_BYTES))
    output_format = options.get("format", "markdown")  # markdown, json, records, csv

    try:
        con = duckdb.connect(database=":memory:")
        con.execute("PRAGMA memory_limit='1GB';")
        con.execute("PRAGMA threads=4;")

        # Register aliased sources as views
        for src in sources:
            load_source(con, src)

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
