"""
Unit tests for DuckDB Analytics secrets management feature.

Tests focus on OUR custom logic:
- Environment variable expansion
- YAML loading integration
- SQL generation for CREATE SECRET statements
- String escaping for security

We don't test Pydantic validation - that's Pydantic's job.
"""

import os
import pytest
import tempfile
from pathlib import Path

# Import from the main script
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "skills" / "duckdb-analytics" / "scripts"))

from query_duckdb import (
    # Pydantic Models (only what we need for SQL generation tests)
    PostgresSecret,
    MySQLSecret,
    S3Secret,
    AzureSecret,
    HTTPSecret,
    HuggingFaceSecret,
    SecretsConfig,
    # Functions - these are what we're actually testing
    expand_env_vars,
    load_secrets_from_yaml,
    create_secret_sql,
    escape_string,
    escape_identifier,
)


# =============================================================================
# Environment Variable Expansion Tests (our custom logic)
# =============================================================================


class TestExpandEnvVars:
    def test_single_var(self):
        """Test expanding single environment variable."""
        os.environ["TEST_VAR"] = "test_value"
        result = expand_env_vars("${TEST_VAR}")
        assert result == "test_value"

    def test_multiple_vars_in_string(self):
        """Test expanding multiple environment variables in one string."""
        os.environ["VAR1"] = "value1"
        os.environ["VAR2"] = "value2"
        result = expand_env_vars("${VAR1}-${VAR2}")
        assert result == "value1-value2"

    def test_nested_dict(self):
        """Test expanding env vars recursively in nested dict."""
        os.environ["HOST"] = "db.example.com"
        os.environ["PASS"] = "secret"
        data = {
            "host": "${HOST}",
            "credentials": {"password": "${PASS}"},
        }
        result = expand_env_vars(data)
        assert result["host"] == "db.example.com"
        assert result["credentials"]["password"] == "secret"

    def test_list_expansion(self):
        """Test expanding env vars in list."""
        os.environ["ITEM1"] = "a"
        os.environ["ITEM2"] = "b"
        result = expand_env_vars(["${ITEM1}", "${ITEM2}"])
        assert result == ["a", "b"]

    def test_missing_var_raises(self):
        """Test that missing env var raises ValueError."""
        if "NONEXISTENT_VAR_XYZ" in os.environ:
            del os.environ["NONEXISTENT_VAR_XYZ"]

        with pytest.raises(ValueError, match="Environment variable not set"):
            expand_env_vars("${NONEXISTENT_VAR_XYZ}")

    def test_no_expansion_needed(self):
        """Test that strings without ${} are unchanged."""
        result = expand_env_vars("plain string")
        assert result == "plain string"

    def test_non_string_passthrough(self):
        """Test that non-string values pass through unchanged."""
        assert expand_env_vars(123) == 123
        assert expand_env_vars(True) is True
        assert expand_env_vars(None) is None


# =============================================================================
# YAML Loading Integration Tests (our integration logic)
# =============================================================================


class TestLoadSecretsFromYaml:
    def test_valid_yaml_loads_and_validates(self):
        """Test the full pipeline: YAML -> env expansion -> Pydantic."""
        yaml_content = """
secrets:
  my_postgres:
    type: postgres
    host: localhost
    user: analyst
    password: secret
    database: mydb
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            f.flush()
            config = load_secrets_from_yaml(f.name)

        assert "my_postgres" in config.secrets
        assert config.secrets["my_postgres"].host == "localhost"
        os.unlink(f.name)

    def test_file_not_found(self):
        """Test that missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_secrets_from_yaml("/nonexistent/path/secrets.yaml")

    def test_env_var_expansion_integrated(self):
        """Test environment variable expansion during YAML loading."""
        os.environ["TEST_DB_PASSWORD"] = "secret123"
        yaml_content = """
secrets:
  test_db:
    type: postgres
    host: localhost
    user: user
    password: "${TEST_DB_PASSWORD}"
    database: db
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            f.flush()
            config = load_secrets_from_yaml(f.name)

        assert config.secrets["test_db"].password == "secret123"
        os.unlink(f.name)

    def test_empty_yaml_raises(self):
        """Test that empty YAML file raises ValueError."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write("")
            f.flush()
            with pytest.raises(ValueError, match="empty"):
                load_secrets_from_yaml(f.name)
        os.unlink(f.name)


# =============================================================================
# SQL Generation Tests (our SQL building logic - critical)
# =============================================================================


class TestCreateSecretSql:
    def test_postgres_connection_string_format(self):
        """Test PostgreSQL generates correct libpq connection string."""
        secret = PostgresSecret(
            type="postgres",
            host="db.example.com",
            port=5432,
            user="analyst",
            password="secret",
            database="mydb",
        )
        sql = create_secret_sql("pg_secret", secret)
        
        assert "CREATE OR REPLACE SECRET" in sql
        assert "TYPE postgres" in sql
        assert "CONNECTION_STRING" in sql
        assert "host=db.example.com" in sql
        assert "port=5432" in sql
        assert "user=analyst" in sql
        assert "dbname=mydb" in sql

    def test_s3_includes_all_fields(self):
        """Test S3 SQL includes key_id, secret, region."""
        secret = S3Secret(
            type="s3",
            key_id="AKIAIOSFODNN7EXAMPLE",
            secret="wJalrXUtnFEMI",
            region="us-east-1",
        )
        sql = create_secret_sql("s3_secret", secret)
        
        assert "TYPE s3" in sql
        assert "KEY_ID" in sql
        assert "SECRET" in sql
        assert "REGION" in sql
        assert "us-east-1" in sql

    def test_s3_with_scope(self):
        """Test S3 SQL includes SCOPE when specified."""
        secret = S3Secret(
            type="s3",
            key_id="key",
            secret="secret",
            region="us-west-2",
            scope="s3://bucket/*",
        )
        sql = create_secret_sql("s3_scoped", secret)
        assert "SCOPE" in sql
        assert "s3://bucket/*" in sql

    def test_azure_with_account_key(self):
        """Test Azure SQL with account key authentication."""
        secret = AzureSecret(
            type="azure",
            account_name="mystorageaccount",
            account_key="key123",
        )
        sql = create_secret_sql("azure_secret", secret)
        
        assert "TYPE azure" in sql
        assert "ACCOUNT_NAME" in sql
        assert "ACCOUNT_KEY" in sql

    def test_http_with_bearer_token(self):
        """Test HTTP SQL with bearer token."""
        secret = HTTPSecret(type="http", bearer_token="token123")
        sql = create_secret_sql("http_secret", secret)
        
        assert "TYPE http" in sql
        assert "BEARER_TOKEN" in sql

    def test_huggingface_sql(self):
        """Test HuggingFace SQL generation."""
        secret = HuggingFaceSecret(type="huggingface", token="hf_token")
        sql = create_secret_sql("hf_secret", secret)
        
        assert "TYPE huggingface" in sql
        assert "TOKEN" in sql


# =============================================================================
# SQL Escaping Tests (security-critical)
# =============================================================================


class TestSqlEscaping:
    def test_single_quote_in_password(self):
        """Test that single quotes in passwords are escaped (SQL injection prevention)."""
        secret = PostgresSecret(
            type="postgres",
            host="localhost",
            user="user",
            password="pass'word",
            database="db",
        )
        sql = create_secret_sql("test", secret)
        # Single quote should be escaped as ''
        assert "pass''word" in sql

    def test_escape_string_function(self):
        """Test SQL string literal escaping."""
        assert escape_string("value") == "'value'"
        assert escape_string("it's") == "'it''s'"
        assert escape_string("O'Brien's") == "'O''Brien''s'"

    def test_escape_identifier_function(self):
        """Test SQL identifier escaping."""
        assert escape_identifier("table") == '"table"'
        assert escape_identifier('table"name') == '"table""name"'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

