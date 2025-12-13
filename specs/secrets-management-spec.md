# DuckDB Analytics Skill - Secrets Management Feature Specification

**Date**: December 13, 2025  
**Status**: Specification Draft  
**Target Version**: v2.0.0

---

## 1. Executive Summary

This specification outlines the design and implementation of a **Secrets Management** feature for the DuckDB Analytics Claude Skill. The feature enables users to securely manage credentials for protected data sources (PostgreSQL, MySQL, Azure, S3, GCS, etc.) through a simple YAML configuration file that is parsed and registered in DuckDB's native Secrets Manager for the duration of a session.

### Key Benefits

- **Security**: Credentials are never hardcoded in SQL queries or logged to shell history
- **Convenience**: Single YAML file manages credentials for multiple data sources
- **Compliance**: Leverages DuckDB's built-in Secrets Manager for enterprise-grade credential handling
- **Flexibility**: Supports scoped secrets (e.g., different credentials for different S3 buckets)
- **Claude Skill Integration**: Works seamlessly with the existing Claude Code skill architecture

---

## 2. Problem Statement

### Current Limitations

Currently, users connecting to protected data sources must:

1. **Embed credentials directly in SQL or JSON**:
   ```json
   {
     "query": "SELECT * FROM ...",
     "sources": [{
       "type": "postgres",
       "host": "db.example.com",
       "user": "myuser",
       "password": "mypassword",  // ❌ Exposed
       "database": "mydb"
     }]
   }
   ```

2. **Pass credentials as environment variables** - fragile and hard to manage multiple credentials
3. **No support for scoped secrets** - can't have different credentials for different resources
4. **No persistent secret storage** - credentials must be re-specified for each session

### Use Cases That Require Secrets

1. **Cross-database analytics** - Join data from multiple PostgreSQL/MySQL databases
2. **Hybrid cloud queries** - Combine on-premise database with S3/Azure data lake
3. **Sensitive data access** - Query production databases with restricted credentials
4. **Multi-tenant scenarios** - Different credentials per customer/region
5. **Enterprise deployments** - Where credentials are managed by DevOps/Security teams

---

## 3. Architecture Overview

### 3.1 Component Design

```
User Request
    ↓
Claude Code Skill (duckdb-analytics)
    ├─ Load YAML secrets file
    ├─ Parse secrets configuration
    ├─ Create DuckDB connection
    ├─ Register secrets via CREATE SECRET statements
    ├─ Load data sources (using registered secrets)
    ├─ Execute query
    └─ Return results
```

### 3.2 DuckDB Secrets Manager Integration

The feature leverages DuckDB's **Secrets Manager** (available since DuckDB 0.10):

- **Temporary Secrets**: In-memory secrets for the current session (default)
- **Persistent Secrets**: Stored unencrypted in `~/.duckdb/stored_secrets/` (optional)
- **Scoped Secrets**: Support for path-based secret scoping
- **Secret Types**: `S3`, `GCS`, `AZURE`, `R2`, `POSTGRES`, `MYSQL`, `HUGGINGFACE`

---

## 4. User-Facing API

### 4.1 YAML Secrets File Format

Users create a `secrets.yaml` file (or custom name) with the following structure:

```yaml
# secrets.yaml - Credentials for protected data sources

secrets:
  # PostgreSQL Database Credentials
  postgres_prod:
    type: postgres
    host: prod-db.example.com
    port: 5432
    user: analytics_user
    password: "${PGPASSWORD}"  # Can reference env vars
    database: analytics_db
    schema: public

  # MySQL Database
  mysql_warehouse:
    type: mysql
    host: mysql.example.com
    port: 3306
    user: warehouse_user
    password: "${MYSQL_PASSWORD}"
    database: warehouse

  # AWS S3 Credentials
  s3_data_lake:
    type: s3
    key_id: "${AWS_ACCESS_KEY_ID}"
    secret: "${AWS_SECRET_ACCESS_KEY}"
    region: us-east-1
    scope: "s3://my-data-lake/*"  # Optional: restrict to specific buckets

  # Azure Blob Storage
  azure_lake:
    type: azure
    account_name: mystorageaccount
    account_key: "${AZURE_STORAGE_KEY}"
    scope: "az://container/*"

  # Google Cloud Storage
  gcs_backup:
    type: gcs
    key_id: "${GCS_KEY_ID}"
    secret: "${GCS_SECRET}"
    region: us-central1

options:
  # Global secret options
  persist: false  # Whether to persist secrets (default: false for security)
  provider: config  # Secret provider: 'config' or 'credential_chain'
```

### 4.2 Extended JSON Request Format

The script accepts an optional `secrets_file` parameter in the JSON request:

```json
{
  "query": "SELECT * FROM prod_sales JOIN warehouse_products USING (product_id)",
  "secrets_file": "D:/path/to/secrets.yaml",
  "sources": [
    {
      "type": "postgres",
      "alias": "prod_sales",
      "secret": "postgres_prod",
      "table": "sales"
    },
    {
      "type": "mysql",
      "alias": "warehouse_products",
      "secret": "mysql_warehouse",
      "table": "products"
    }
  ],
  "options": {
    "max_rows": 500,
    "format": "json"
  }
}
```

### 4.3 Source Reference to Secrets

When a source has a `secret` field, it references a secret name from `secrets.yaml`:

```json
{
  "type": "postgres",
  "alias": "main_db",
  "secret": "postgres_prod",
  "table": "customers",
  "schema": "public"
}
```

The script automatically:
1. Loads the named secret from `secrets.yaml`
2. Creates a DuckDB SECRET using `CREATE SECRET`
3. Uses the secret when creating the data source view

---

## 5. Detailed Implementation Design

### 5.1 Python Script Changes

#### New Imports

```python
import yaml
import os
from pathlib import Path
from typing import Optional, Dict, Any, Literal, Union
from pydantic import BaseModel, Field, validator, root_validator
```

#### Pydantic Secret Models

```python
class SecretBase(BaseModel):
    """Base model for all secret types with common validation."""
    
    class Config:
        extra = "forbid"  # Disallow unknown fields
        use_enum_values = True

class PostgresSecret(SecretBase):
    type: Literal["postgres"]
    host: str
    port: int = 5432
    user: str
    password: str
    database: str
    schema: str = "public"

class MySQLSecret(SecretBase):
    type: Literal["mysql"]
    host: str
    port: int = 3306
    user: str
    password: str
    database: str

class S3Secret(SecretBase):
    type: Literal["s3"]
    key_id: str
    secret: str
    region: str
    scope: Optional[str] = None
    endpoint: Optional[str] = None
    use_ssl: bool = True

class GCSSecret(SecretBase):
    type: Literal["gcs"]
    key_id: str
    secret: str
    region: Optional[str] = None
    scope: Optional[str] = None

class AzureSecret(SecretBase):
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
    # MANAGED_IDENTITY provider
    # client_id already defined above
    # CREDENTIAL_CHAIN options
    chain: Optional[str] = None  # e.g., "cli;env"
    provider: Optional[str] = None  # "config", "service_principal", "credential_chain", "managed_identity"

class R2Secret(SecretBase):
    type: Literal["r2"]
    key_id: str
    secret: str
    account_id: str
    region: Optional[str] = None
    scope: Optional[str] = None

class HTTPSecret(SecretBase):
    type: Literal["http"]
    # Authentication options
    bearer_token: Optional[str] = None
    extra_http_headers: Optional[Dict[str, str]] = None  # Map of custom headers
    # HTTP Proxy configuration
    http_proxy: Optional[str] = None  # Proxy URL
    http_proxy_username: Optional[str] = None
    http_proxy_password: Optional[str] = None

class IcebergSecret(SecretBase):
    type: Literal["iceberg"]
    # Authentication methods (choose one):
    token: Optional[str] = None  # Bearer token
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    # OAuth2 configuration (with client credentials)
    oauth2_server_uri: Optional[str] = None
    oauth2_scope: Optional[str] = None

class DuckLakeSecret(SecretBase):
    type: Literal["ducklake"]
    metadata_path: str  # Connection string for metadata catalog (e.g., "metadata.db", "", "postgres:dbname=...")
    data_path: str  # Storage location for data files (e.g., "s3://bucket/path/", "data/")
    metadata_parameters: Optional[Dict[str, str]] = None  # Parameters to pass to catalog server

class HuggingFaceSecret(SecretBase):
    type: Literal["huggingface"]
    token: Optional[str] = None  # Hugging Face token (use with CONFIG provider)
    provider: Optional[str] = None  # "config" or "credential_chain"

# Union type for all secret types
AnySecret = Union[PostgresSecret, MySQLSecret, S3Secret, GCSSecret, 
                  AzureSecret, R2Secret, HTTPSecret, 
                  IcebergSecret, DuckLakeSecret, HuggingFaceSecret]

class SecretsConfig(BaseModel):
    """Top-level secrets configuration file structure."""
    
    secrets: Dict[str, AnySecret]
    
    options: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Global options (persist, provider, etc.)"
    )
    
    class Config:
        extra = "forbid"
    
    @root_validator(pre=True)
    def parse_secret_types(cls, values):
        """
        Parse secrets dict and dispatch to correct model based on 'type' field.
        """
        secrets_raw = values.get("secrets", {})
        parsed_secrets = {}
        
        for name, secret_data in secrets_raw.items():
            secret_type = secret_data.get("type")
            
            if secret_type == "postgres":
                parsed_secrets[name] = PostgresSecret(**secret_data)
            elif secret_type == "mysql":
                parsed_secrets[name] = MySQLSecret(**secret_data)
            elif secret_type == "s3":
                parsed_secrets[name] = S3Secret(**secret_data)
            elif secret_type == "gcs":
                parsed_secrets[name] = GCSSecret(**secret_data)
            elif secret_type == "azure":
                parsed_secrets[name] = AzureSecret(**secret_data)
            elif secret_type == "r2":
                parsed_secrets[name] = R2Secret(**secret_data)
            elif secret_type == "http":
                parsed_secrets[name] = HTTPSecret(**secret_data)
            elif secret_type == "iceberg":
                parsed_secrets[name] = IcebergSecret(**secret_data)
            elif secret_type == "ducklake":
                parsed_secrets[name] = DuckLakeSecret(**secret_data)
            elif secret_type == "huggingface":
                parsed_secrets[name] = HuggingFaceSecret(**secret_data)
            else:
                raise ValueError(f"Unknown secret type: {secret_type}")
        
        values["secrets"] = parsed_secrets
        return values
```

#### New Functions

```python
def load_secrets_from_yaml(file_path: str) -> SecretsConfig:
    """
    Load and validate secrets from a YAML file using Pydantic.
    
    Supports environment variable substitution with ${VAR_NAME} syntax.
    
    Raises:
        FileNotFoundError: If secrets file doesn't exist
        yaml.YAMLError: If YAML is malformed
        ValidationError: If secrets don't match schema or validation fails
    """

def expand_env_vars(value: Any) -> Any:
    """
    Recursively expand ${VAR_NAME} patterns in strings.
    Raises ValueError if referenced env var doesn't exist.
    """

def register_duckdb_secret(con: duckdb.DuckDBPyConnection, 
                         secret_name: str, 
                         secret: AnySecret) -> None:
    """
    Register a secret in DuckDB using CREATE SECRET.
    
    Dispatches to type-specific SQL generation based on secret type.
    """

def create_secret_sql(secret_name: str, secret: AnySecret) -> str:
    """
    Generate CREATE [OR REPLACE] SECRET SQL for a Pydantic secret model.
    """
```

#### Modified Functions

```python
def load_source(con: duckdb.DuckDBPyConnection, 
               src: dict, 
               secrets: Optional[Dict[str, Dict]] = None) -> None:
    """
    Enhanced to support 'secret' field that references pre-registered secrets.
    
    When src["secret"] is specified, retrieves connection info from secrets dict
    instead of from src itself.
    """

def main() -> None:
    """
    Enhanced to:
    1. Extract 'secrets_file' from request
    2. Load and validate YAML secrets
    3. Register secrets with DuckDB before loading sources
    4. Pass secrets dict to load_source()
    """
```

### 5.2 Supported Secret Types

#### PostgreSQL

```yaml
postgres_secret:
  type: postgres
  host: localhost
  port: 5432
  user: postgres_user
  password: secure_password
  database: mydb
  schema: public  # Optional
```

**DuckDB Equivalent**:
```sql
CREATE SECRET postgres_secret (
  TYPE postgres,
  CONNECTION_STRING 'host=localhost port=5432 user=postgres_user password=secure_password dbname=mydb'
);
```

#### MySQL

```yaml
mysql_secret:
  type: mysql
  host: localhost
  port: 3306
  user: mysql_user
  password: secure_password
  database: mydb
```

#### S3 (AWS)

```yaml
s3_secret:
  type: s3
  key_id: AKIA...
  secret: wJalrX...
  region: us-east-1
  scope: "s3://my-bucket/*"  # Optional
```

#### Azure Blob Storage

```yaml
# Option 1: Storage account key
azure_secret_key:
  type: azure
  account_name: mystorageaccount
  account_key: DefaultEndpointsProtocol=https...

# Option 2: Connection string
azure_secret_connstr:
  type: azure
  connection_string: "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"

# Option 3: Service Principal (requires Azure AD)
azure_secret_sp:
  type: azure
  account_name: mystorageaccount
  tenant_id: "12345678-1234-1234-1234-123456789012"
  client_id: "abcdef12-3456-7890-abcd-ef1234567890"
  client_secret: "${AZURE_CLIENT_SECRET}"
  provider: service_principal

# Option 4: Managed Identity (for Azure-hosted applications)
azure_secret_managed:
  type: azure
  account_name: mystorageaccount
  client_id: "abcdef12-3456-7890-abcd-ef1234567890"  # Optional
  provider: managed_identity
```

#### Google Cloud Storage

```yaml
gcs_secret:
  type: gcs
  key_id: ABC123...
  secret: secret_key_content
  region: us-central1
  scope: "gs://my-bucket/*"  # Optional
```

#### HTTP/HTTPS

```yaml
http_api_secret:
  type: http
  # Option 1: Bearer token authentication
  bearer_token: "${HTTP_TOKEN}"
  
  # Option 2: Custom headers
  extra_http_headers:
    Authorization: "Bearer token123"
    Custom-Header: value
  
  # Option 3: HTTP Proxy configuration (optional)
  http_proxy: "http://proxy.example.com:8080"
  http_proxy_username: proxy_user
  http_proxy_password: proxy_password
```

#### HuggingFace

```yaml
huggingface_secret:
  type: huggingface
  # Option 1: Direct token
  token: "${HF_TOKEN}"
  
  # Option 2: Use system credentials
  provider: credential_chain
```

#### Iceberg REST Catalog

```yaml
iceberg_catalog_secret:
  type: iceberg
  # Option 1: Bearer Token authentication
  token: "${ICEBERG_TOKEN}"
  
  # Option 2: OAuth2 with Client Credentials
  # client_id: admin
  # client_secret: "${ICEBERG_CLIENT_SECRET}"
  # oauth2_server_uri: "https://catalog.example.com/oauth/tokens"
  # oauth2_scope: "iceberg"
```

#### DuckLake

```yaml
ducklake_secret:
  type: ducklake
  # Metadata path: connection string for catalog database
  # Examples: "metadata.db" (DuckDB), "" (inline), "postgres:dbname=mydb" (PostgreSQL)
  metadata_path: "metadata.db"
  
  # Data path: storage location for data files
  data_path: "s3://my-datalake/data/"
  
  # Optional: parameters to pass to metadata catalog server
  metadata_parameters:
    TYPE: postgres
    SECRET: postgres_secret
```

### 5.3 Secret Creation SQL Generation

For each secret type, the script generates appropriate `CREATE SECRET` SQL:

**PostgreSQL**:
```sql
CREATE OR REPLACE SECRET postgres_secret (
  TYPE postgres,
  CONNECTION_STRING 'host=... user=... password=... dbname=...'
);
```

**S3 with CONFIG Provider**:
```sql
CREATE OR REPLACE SECRET s3_secret (
  TYPE s3,
  KEY_ID 'AKIA...',
  SECRET 'wJalrX...',
  REGION 'us-east-1'
);
```

**S3 with CREDENTIAL_CHAIN Provider**:
```sql
CREATE OR REPLACE SECRET s3_secret (
  TYPE s3,
  PROVIDER credential_chain,
  REGION 'us-east-1'
);
```

### 5.4 Pydantic-Based Validation Flow

```
User YAML File
    ↓
YAML Parser (pyyaml)
    ↓
Raw Dict
    ↓
Pydantic SecretsConfig Model
├─ Root validator converts each secret to specific type model
├─ Type validation (postgres → PostgresSecret)
├─ Required field checking
├─ Type coercion (string → int for ports)
├─ Custom validators (regex, constraints)
└─ Environment variable expansion
    ↓
Validated SecretsConfig object
    ↓
Generate CREATE SECRET SQL
    ↓
Register with DuckDB
```

#### Validation Examples

**Valid PostgreSQL Secret**:
```python
secret = PostgresSecret(
    type="postgres",
    host="db.example.com",
    port=5432,  # Can be string or int, Pydantic coerces
    user="analyst",
    password="${PGPASSWORD}",
    database="analytics"
    # schema defaults to "public"
)
```

**Invalid S3 Secret (Missing key_id)**:
```python
# Pydantic raises ValidationError:
# "field required" for "key_id" in S3Secret
```

**Type Mismatch (Port as string that can't convert)**:
```python
# Pydantic raises ValidationError:
# "value is not a valid integer" for port="abc"
```

**Unknown Field (Typo)**:
```python
secret_data = {
    "type": "postgres",
    "host": "db.com",
    "user": "user",
    "passwor": "pwd",  # Typo!
    "database": "db"
}
# Pydantic raises ValidationError:
# "extra fields not permitted" for "passwor"
```

---

## 5.5 Pydantic Model Design Rationale

Using Pydantic for secrets management provides several advantages over raw dict parsing:

### Type Safety & Validation

```python
# ✅ Invalid type automatically caught
secret_data = {
    "type": "postgres",
    "port": "not_a_number",  # String instead of int
    ...
}
secrets = SecretsConfig(secrets={"db": secret_data})
# ValidationError: port must be a valid integer
```

### Smart Defaults

```python
# ✅ Omit default values, they're auto-set
secret_data = {
    "type": "postgres",
    "host": "localhost",
    "user": "analyst",
    "password": "secret",
    "database": "mydb"
    # port defaults to 5432
    # schema defaults to "public"
}

postgres_secret = PostgresSecret(**secret_data)
assert postgres_secret.port == 5432
assert postgres_secret.schema == "public"
```

### Field-Level Validation with Validators

```python
class S3Secret(SecretBase):
    type: Literal["s3"]
    region: str
    
    @validator("region")
    def validate_region(cls, v):
        valid_regions = ["us-east-1", "us-west-2", "eu-west-1", ...]
        if v not in valid_regions:
            raise ValueError(f"Invalid AWS region: {v}")
        return v
```

### Clear Error Messages

Raw dict parsing:
```
❌ Error: config['secret1']['passwor'] - missing password field
```

Pydantic validation:
```
✅ ValidationError: 1 validation error for SecretsConfig
secrets -> secret1 -> PostgresSecret
  extra fields not permitted (type=value_error.extra)
```

### Extensibility

Easy to add new secret types by extending `SecretBase`:

```python
class HuggingFaceSecret(SecretBase):
    type: Literal["huggingface"]
    token: str
    
    @validator("token")
    def validate_token_format(cls, v):
        # Can add custom validation logic
        if not v.startswith("hf_"):
            raise ValueError("HF token must start with 'hf_'")
        return v
```

### Serialization & Debugging

```python
# Convert back to dict for inspection
secret_dict = postgres_secret.dict()

# Partial dict (exclude sensitive fields)
safe_dict = postgres_secret.dict(exclude={"password"})
# {'type': 'postgres', 'host': '...', 'port': 5432, ...}
```

---

## 6. Security Considerations

### 6.1 Credential Handling

✅ **Best Practices Implemented**:
- Environment variable expansion (`${VAR_NAME}`) encourages external credential management
- No credentials logged to stdout/stderr (only file paths)
- Secrets not included in query results or error messages
- In-memory only by default (not persisted to disk)

⚠️ **User Responsibilities**:
- Keep `secrets.yaml` file in `.gitignore`
- Use environment variables for sensitive values, not literal passwords
- Restrict file permissions on `secrets.yaml` (e.g., `chmod 600 secrets.yaml`)
- Consider using secret management tools (Vault, AWS Secrets Manager, etc.) to source env vars

### 6.2 DuckDB Security Model

DuckDB's Secrets Manager provides:
- Sensitive information redaction in `duckdb_secrets()` output
- Scope-based access control (different secrets for different resource prefixes)
- Support for multiple authentication providers (CONFIG, CREDENTIAL_CHAIN)

### 6.3 Claude Skill Context

As a Claude Code skill executing in a sandboxed environment:
- File operations are restricted to designated directories
- Environment variables are available but cannot be manipulated
- Secret contents are never echoed back to Claude's context

---

## 7. Error Handling

### 7.1 Error Scenarios

| Scenario | Handling |
|----------|----------|
| `secrets_file` not found | Return JSON error: `"Secret file not found: {path}"` |
| Invalid YAML syntax | Return JSON error: `"Invalid YAML in secrets file: {error}"` |
| Validation error (missing required field) | Return JSON error: `"Secret validation error: {field} required for {secret_name}"` |
| Invalid field type | Return JSON error: `"Secret validation error: {field} must be {type} for {secret_name}"` |
| Missing env var (e.g., `${UNKNOWN}`) | Return JSON error: `"Environment variable not set: UNKNOWN"` |
| Unknown secret type | Return JSON error: `"Unknown secret type: {type}"` |
| Unknown field in secret | Return JSON error: `"Unexpected field '{field}' in secret '{name}'. Allowed: {allowed_fields}"` |
| Source references non-existent secret | Return JSON error: `"Secret '{name}' not found in secrets file"` |
| DuckDB CREATE SECRET fails | Return JSON error with DuckDB exception message |
| Connection test fails | Return JSON error with connection details (without password) |

### 7.2 Pydantic Validation Benefits

Pydantic models provide:
- **Type validation**: Ensures fields are correct types (int, str, bool)
- **Required field checking**: Detects missing required credentials
- **Default values**: Sets sensible defaults (e.g., port 5432 for Postgres)
- **Field constraints**: Can enforce patterns (e.g., valid region names)
- **Clear error messages**: Detailed validation errors with field paths
- **Extra field detection**: Catches typos in field names (e.g., `passwor` vs `password`)
- **Custom validators**: Can add domain-specific validation logic

Example validation error output:
```json
{
  "error": "Secret validation failed",
  "details": [
    {
      "loc": ["secrets", "my_postgres", "port"],
      "msg": "value is not a valid integer",
      "type": "type_error.integer"
    },
    {
      "loc": ["secrets", "my_s3", "key_id"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

### 7.3 Error Response Format

```json
{
  "error": "Failed to load secrets file",
  "error_type": "ValidationError",
  "context": {
    "secrets_file": "D:/secrets.yaml",
    "details": [...]
  }
}
```

---

## 8. Integration with Existing Skill

### 8.1 Backward Compatibility

✅ Fully backward compatible:
- Existing requests without `secrets_file` work unchanged
- Sources without `secret` field work as before (inline credentials)
- No breaking changes to JSON API

### 8.2 Enhanced SKILL.md Documentation

Update [SKILL.md](SKILL.md) with new sections:

```markdown
## Using Secrets for Protected Data Sources

Credentials can be managed securely using a YAML secrets file instead of 
embedding them in requests.

### Quick Start

1. Create `secrets.yaml`:
```yaml
secrets:
  my_postgres:
    type: postgres
    host: db.example.com
    user: analyst
    password: "${PGPASSWORD}"
    database: analytics
```

2. Use in request:
```json
{
  "query": "SELECT * FROM customers",
  "secrets_file": "/path/to/secrets.yaml",
  "sources": [{
    "type": "postgres",
    "alias": "customers",
    "secret": "my_postgres",
    "table": "customers"
  }]
}
```

See [Secrets Management Guide](#secrets-management-guide) for details.
```

---

## 9. Implementation Roadmap

### Phase 1: Core Implementation (v2.0.0)

- [ ] Add Pydantic models for all secret types (Postgres, MySQL, S3, GCS, Azure, R2, SQLite)
- [ ] Create SecretsConfig root model with validation
- [ ] Implement environment variable substitution with Pydantic validators
- [ ] Add YAML parsing with Pydantic integration
- [ ] Implement `register_duckdb_secret()` for all secret types
- [ ] Update `load_source()` to support `secret` field
- [ ] Update `main()` to load and register secrets
- [ ] Add comprehensive error handling with Pydantic ValidationError mapping
- [ ] Write unit tests for Pydantic models and validation
- [ ] Write unit tests for secret loading and SQL generation
- [ ] Update SKILL.md with secrets documentation
- [ ] Create `examples/secrets.yaml` template

**Dependencies**: `pydantic` (v2.0+) and `pyyaml`

### Phase 2: Advanced Features (v2.1.0)

- [ ] Support `provider: credential_chain` for AWS/GCS credential auto-discovery
- [ ] Add field-level Pydantic validators for region/endpoint constraints
- [ ] Implement `validate_secret()` function to test connections before querying
- [ ] Support `.env` file loading as alternative to env vars
- [ ] Add secret rotation support (update existing secrets)
- [ ] Implement secret scope validation with Pydantic
- [ ] Add `which_secret()` function integration for debugging
- [ ] Create custom Pydantic validator for AWS region names, Azure account formats, etc.

### Phase 3: Enterprise Features (v2.2.0)

- [ ] Optional persistent secret storage with encryption
- [ ] Integration with HashiCorp Vault / AWS Secrets Manager
- [ ] Audit logging of secret usage
- [ ] Secret expiration and refresh handling

---

## A. Pydantic Configuration & Customization

### A.1 Custom Validators

Extend secret models with domain-specific validation:

```python
from pydantic import validator

class S3Secret(SecretBase):
    type: Literal["s3"]
    key_id: str
    secret: str
    region: str
    scope: Optional[str] = None
    
    @validator("key_id")
    def validate_key_format(cls, v):
        """Ensure AWS key IDs start with AKIA or ASIA"""
        if not (v.startswith("AKIA") or v.startswith("ASIA")):
            raise ValueError("AWS key ID must start with AKIA or ASIA")
        return v
    
    @validator("region")
    def validate_region(cls, v):
        """Validate against known AWS regions"""
        valid_regions = [
            "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", ...
        ]
        if v not in valid_regions:
            raise ValueError(f"Unknown AWS region: {v}")
        return v
    
    @validator("scope")
    def validate_scope(cls, v):
        """Ensure scope starts with s3://"""
        if v and not v.startswith("s3://"):
            raise ValueError("S3 scope must start with 's3://'")
        return v
```

### A.2 Sensitive Field Handling

Exclude passwords from serialization:

```python
class PostgresSecret(SecretBase):
    type: Literal["postgres"]
    host: str
    port: int = 5432
    user: str
    password: str  # ← Don't include in logs
    database: str
    
    class Config:
        # Hide sensitive fields from __repr__
        fields = {
            "password": {"exclude": True}
        }
    
    def __repr__(self):
        return f"PostgresSecret(host={self.host}, user={self.user}, ...)"
    
    def dict_safe(self):
        """Return dict without sensitive fields"""
        return self.dict(exclude={"password"})
```

### A.3 Conditional Field Requirements

Use root validators for conditional logic:

```python
class AzureSecret(SecretBase):
    type: Literal["azure"]
    account_name: str
    account_key: Optional[str] = None
    account_sas_token: Optional[str] = None
    
    @root_validator
    def check_auth_fields(cls, values):
        """Ensure either account_key OR account_sas_token provided"""
        has_key = values.get("account_key") is not None
        has_token = values.get("account_sas_token") is not None
        
        if not (has_key or has_token):
            raise ValueError("Provide either 'account_key' or 'account_sas_token'")
        if has_key and has_token:
            raise ValueError("Provide either 'account_key' or 'account_sas_token', not both")
        
        return values
```

---

## 10. Configuration Examples

### Example 1: Multi-Database Analytics

```yaml
# secrets.yaml
secrets:
  analytics_postgres:
    type: postgres
    host: analytics.internal.company.com
    port: 5432
    user: analytics_reader
    password: "${ANALYTICS_DB_PASSWORD}"
    database: warehouse
    schema: analytics

  operational_mysql:
    type: mysql
    host: operational.internal.company.com
    port: 3306
    user: ops_reader
    password: "${OPS_DB_PASSWORD}"
    database: production

  s3_data_lake:
    type: s3
    key_id: "${AWS_ACCESS_KEY_ID}"
    secret: "${AWS_SECRET_ACCESS_KEY}"
    region: us-west-2
    scope: "s3://data-lake/*"
```

**Query Request**:
```json
{
  "query": "SELECT a.*, o.status FROM analytics_table a JOIN ops_table o ON a.id = o.id",
  "secrets_file": "/config/secrets.yaml",
  "sources": [
    {
      "type": "postgres",
      "alias": "analytics_table",
      "secret": "analytics_postgres",
      "table": "transactions"
    },
    {
      "type": "mysql",
      "alias": "ops_table",
      "secret": "operational_mysql",
      "table": "orders"
    }
  ]
}
```

### Example 2: S3 Data Lake + Postgres

```yaml
secrets:
  s3_lake:
    type: s3
    key_id: "${AWS_ACCESS_KEY_ID}"
    secret: "${AWS_SECRET_ACCESS_KEY}"
    region: us-west-2
    scope: "s3://company-datalake/*"

  source_db:
    type: postgres
    host: source.db.company.com
    user: etl_user
    password: "${SOURCE_DB_PASSWORD}"
    database: source_db
```

**Query**:
```json
{
  "query": "SELECT s3_files.year, COUNT(*) as count FROM s3_files INNER JOIN source_sales ON s3_files.id = source_sales.id GROUP BY year",
  "secrets_file": "/secure/secrets.yaml",
  "sources": [
    {
      "type": "file",
      "alias": "s3_files",
      "path": "s3://company-datalake/exports/*.parquet",
      "secret": "s3_lake"
    },
    {
      "type": "postgres",
      "alias": "source_sales",
      "secret": "source_db",
      "table": "sales"
    }
  ]
}
```

### Example 3: Using Environment Variables Only (No Secrets File)

For deployments where all credentials come from environment:

```bash
export PGPASSWORD="secure_password"
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="wJalrX..."
```

```json
{
  "query": "SELECT * FROM data",
  "sources": [
    {
      "type": "postgres",
      "alias": "data",
      "host": "localhost",
      "user": "analyst",
      "password": "${PGPASSWORD}",
      "database": "mydb",
      "table": "table"
    }
  ]
}
```

---

## 11. Testing Strategy

### Unit Tests

```python
# test_secrets_models.py - Pydantic model validation tests

def test_postgres_secret_valid():
    """Test creating valid PostgreSQL secret"""
    secret = PostgresSecret(
        type="postgres",
        host="localhost",
        user="user",
        password="pwd",
        database="db"
    )
    assert secret.port == 5432  # default
    assert secret.schema == "public"  # default

def test_postgres_secret_missing_required():
    """Test that missing required field raises ValidationError"""
    with pytest.raises(ValidationError) as exc:
        PostgresSecret(
            type="postgres",
            host="localhost",
            # Missing user, password, database
        )
    errors = exc.value.errors()
    assert len(errors) == 3
    assert all(e["type"] == "value_error.missing" for e in errors)

def test_postgres_secret_invalid_port_type():
    """Test that invalid port type raises ValidationError"""
    with pytest.raises(ValidationError):
        PostgresSecret(
            type="postgres",
            host="localhost",
            user="user",
            password="pwd",
            database="db",
            port="not_a_number"
        )

def test_s3_secret_valid():
    """Test creating valid S3 secret"""
    secret = S3Secret(
        type="s3",
        key_id="AKIA...",
        secret="wJalrX...",
        region="us-east-1"
    )
    assert secret.use_ssl is True  # default
    assert secret.scope is None

def test_s3_secret_with_scope():
    """Test S3 secret with optional scope"""
    secret = S3Secret(
        type="s3",
        key_id="AKIA...",
        secret="wJalrX...",
        region="us-west-2",
        scope="s3://my-bucket/*"
    )
    assert secret.scope == "s3://my-bucket/*"

def test_http_secret_with_bearer_token():
    """Test HTTP secret with bearer token authentication"""
    secret = HTTPSecret(
        type="http",
        bearer_token="token123"
    )
    assert secret.bearer_token == "token123"
    assert secret.extra_http_headers is None

def test_http_secret_with_custom_headers():
    """Test HTTP secret with custom headers"""
    secret = HTTPSecret(
        type="http",
        extra_http_headers={"Authorization": "Bearer token", "Custom": "header"}
    )
    assert secret.extra_http_headers is not None
    assert secret.extra_http_headers["Authorization"] == "Bearer token"

def test_http_secret_with_proxy():
    """Test HTTP secret with proxy configuration"""
    secret = HTTPSecret(
        type="http",
        http_proxy="http://proxy.example.com:8080",
        http_proxy_username="user",
        http_proxy_password="pass"
    )
    assert secret.http_proxy == "http://proxy.example.com:8080"
    assert secret.http_proxy_username == "user"

def test_iceberg_secret():
    """Test Iceberg REST Catalog secret"""
    # Bearer token authentication
    secret = IcebergSecret(
        type="iceberg",
        token="my_bearer_token"
    )
    assert secret.token == "my_bearer_token"
    assert secret.client_id is None

def test_iceberg_secret_oauth2():
    """Test Iceberg secret with OAuth2 credentials"""
    secret = IcebergSecret(
        type="iceberg",
        client_id="admin",
        client_secret="password",
        oauth2_server_uri="https://catalog.example.com/oauth/tokens"
    )
    assert secret.client_id == "admin"
    assert secret.oauth2_server_uri is not None

def test_ducklake_secret():
    """Test DuckLake secret"""
    secret = DuckLakeSecret(
        type="ducklake",
        metadata_path="metadata.db",
        data_path="s3://my-datalake/"
    )
    assert secret.metadata_path == "metadata.db"
    assert secret.data_path == "s3://my-datalake/"

def test_ducklake_secret_with_metadata_params():
    """Test DuckLake secret with metadata parameters"""
    secret = DuckLakeSecret(
        type="ducklake",
        metadata_path="",
        data_path="s3://datalake/",
        metadata_parameters={
            "TYPE": "postgres",
            "SECRET": "postgres_secret"
        }
    )
    assert secret.metadata_parameters is not None
    assert secret.metadata_parameters["TYPE"] == "postgres"

def test_azure_secret_with_account_key():
    """Test Azure secret with account key authentication"""
    secret = AzureSecret(
        type="azure",
        account_name="mystorageaccount",
        account_key="DefaultEndpointsProtocol=https..."
    )
    assert secret.account_name == "mystorageaccount"
    assert secret.account_key is not None

def test_azure_secret_with_service_principal():
    """Test Azure secret with service principal authentication"""
    secret = AzureSecret(
        type="azure",
        account_name="mystorageaccount",
        tenant_id="12345678-1234-1234-1234-123456789012",
        client_id="abcdef12-3456-7890-abcd-ef1234567890",
        client_secret="secret123",
        provider="service_principal"
    )
    assert secret.provider == "service_principal"
    assert secret.tenant_id is not None

def test_azure_secret_with_managed_identity():
    """Test Azure secret with managed identity authentication"""
    secret = AzureSecret(
        type="azure",
        account_name="mystorageaccount",
        provider="managed_identity"
    )
    assert secret.provider == "managed_identity"

def test_huggingface_secret_with_token():
    """Test HuggingFace secret with token"""
    secret = HuggingFaceSecret(
        type="huggingface",
        token="hf_token123"
    )
    assert secret.token == "hf_token123"

def test_huggingface_secret_with_credential_chain():
    """Test HuggingFace secret using credential chain"""
    secret = HuggingFaceSecret(
        type="huggingface",
        provider="credential_chain"
    )
    assert secret.provider == "credential_chain"

def test_secrets_config_multiple_types():
    """Test SecretsConfig with mixed secret types"""
    config = SecretsConfig(
        secrets={
            "my_postgres": {
                "type": "postgres",
                "host": "localhost",
                "user": "user",
                "password": "pwd",
                "database": "db"
            },
            "my_s3": {
                "type": "s3",
                "key_id": "AKIA...",
                "secret": "secret",
                "region": "us-east-1"
            }
        }
    )
    assert isinstance(config.secrets["my_postgres"], PostgresSecret)
    assert isinstance(config.secrets["my_s3"], S3Secret)

def test_secrets_config_unknown_secret_type():
    """Test that unknown secret type raises ValidationError"""
    with pytest.raises(ValueError, match="Unknown secret type"):
        SecretsConfig(
            secrets={
                "invalid": {
                    "type": "unknown_db",
                    "host": "localhost"
                }
            }
        )

def test_http_secret_valid():
    """Test creating valid HTTP secret"""
    secret = HTTPSecret(
        type="http",
        bearer_token="token123"
    )
    assert secret.bearer_token == "token123"
    assert secret.extra_http_headers is None  # optional
    assert secret.http_proxy is None  # optional

def test_iceberg_secret_valid():
    """Test creating valid Iceberg secret"""
    secret = IcebergSecret(
        type="iceberg",
        token="bearer_token_xyz"
    )
    assert secret.token == "bearer_token_xyz"
    assert secret.client_id is None  # optional

def test_ducklake_secret_valid():
    """Test creating valid DuckLake secret"""
    secret = DuckLakeSecret(
        type="ducklake",
        metadata_path="metadata.db",
        data_path="data/"
    )
    assert secret.metadata_parameters is None  # optional
    assert secret.metadata_path == "metadata.db"

def test_secrets_config_extra_field_forbidden():
    """Test that extra/unknown fields raise ValidationError"""
    with pytest.raises(ValidationError) as exc:
        PostgresSecret(
            type="postgres",
            host="localhost",
            user="user",
            password="pwd",
            database="db",
            invalid_field="value"  # Unknown field
        )
    errors = exc.value.errors()
    assert any("extra fields not permitted" in str(e) for e in errors)

# test_secrets.py - Integration with YAML and env vars

def test_load_secrets_yaml_valid():
    """Test loading valid YAML secrets file"""
    yaml_content = """
secrets:
  my_postgres:
    type: postgres
    host: localhost
    user: analyst
    password: secret
    database: mydb
"""
    # Mock file read
    config = SecretsConfig.parse_yaml(yaml_content)
    assert "my_postgres" in config.secrets
    assert isinstance(config.secrets["my_postgres"], PostgresSecret)

def test_env_var_expansion():
    """Test environment variable expansion in secrets"""
    os.environ["PGPASSWORD"] = "secret123"
    
    secret_data = {
        "type": "postgres",
        "host": "localhost",
        "user": "user",
        "password": "${PGPASSWORD}",
        "database": "db"
    }
    # After expansion
    secret = PostgresSecret(**expand_env_vars(secret_data))
    assert secret.password == "secret123"

def test_missing_env_var_raises():
    """Test that missing env var raises ValueError"""
    secret_data = {
        "password": "${NONEXISTENT_VAR}"
    }
    with pytest.raises(ValueError, match="Environment variable not set"):
        expand_env_vars(secret_data)

def test_load_secret_file_not_found():
    """Test that missing secrets file raises FileNotFoundError"""
    with pytest.raises(FileNotFoundError):
        load_secrets_from_yaml("/nonexistent/path.yaml")

def test_load_secret_invalid_yaml():
    """Test that malformed YAML raises yaml.YAMLError"""
    invalid_yaml = "secrets: [invalid: yaml: syntax:"
    with pytest.raises(yaml.YAMLError):
        yaml.safe_load(invalid_yaml)
```

### Integration Tests

```python
# test_integration.py

def test_postgres_connection_with_secret():
    """Test actual Postgres connection using secret"""
    
def test_s3_query_with_secret():
    """Test S3 file read using CREATE SECRET"""
    
def test_multiple_secrets_same_type():
    """Test multiple postgres secrets with different scopes"""
    
def test_query_with_missing_secret_reference():
    """Test error when source references non-existent secret"""

def test_pydantic_validation_error_formatting():
    """Test that Pydantic validation errors are formatted nicely for JSON response"""
```

---

## 12. Migration Guide

### For Existing Users

If you're currently using inline credentials:

**Before**:
```json
{
  "sources": [{
    "type": "postgres",
    "host": "db.example.com",
    "user": "analyst",
    "password": "my_password",
    "database": "mydb",
    "table": "sales"
  }]
}
```

**After (Recommended)**:

1. Create `secrets.yaml`:
```yaml
secrets:
  my_postgres:
    type: postgres
    host: db.example.com
    user: analyst
    password: "${PGPASSWORD}"
    database: mydb
```

2. Set environment:
```bash
export PGPASSWORD="my_password"
```

3. Update request:
```json
{
  "secrets_file": "/path/to/secrets.yaml",
  "sources": [{
    "type": "postgres",
    "alias": "sales",
    "secret": "my_postgres",
    "table": "sales"
  }]
}
```

---

## 13. Alternative Approaches Considered

### 1. Environment Variables Only
- ❌ Doesn't scale well with multiple credentials
- ❌ Hard to track which env var maps to which service
- ✅ Good fallback for simple scenarios

### 2. External Secret Service (Vault, etc.)
- ✅ Enterprise-grade security
- ✅ Credential rotation support
- ❌ Adds external dependency
- ❌ Complexity for simple use cases
- **Conclusion**: Phase 3 enhancement, not v2.0

### 3. Python-only Secrets Library
- ❌ Reinventing DuckDB's existing wheel
- ❌ Less battle-tested than DuckDB's Secrets Manager
- ✅ Simpler to implement
- **Conclusion**: Better to leverage DuckDB's native support

### 4. JSON Secrets File
- ✅ No new dependency
- ❌ Less readable for credentials
- ❌ No native comment support
- **Conclusion**: YAML chosen for readability and comments

---

## 14. Appendix: DuckDB Secrets Manager Reference

### Supported Secret Types

| Type | Extension | Use Case |
|------|-----------|----------|
| `POSTGRES` | postgres | PostgreSQL databases |
| `MYSQL` | mysql | MySQL/MariaDB databases |
| `S3` | httpfs | AWS S3 buckets |
| `GCS` | httpfs | Google Cloud Storage |
| `AZURE` | azure | Azure Blob Storage |
| `R2` | httpfs | Cloudflare R2 |
| `HUGGINGFACE` | httpfs | Hugging Face Hub |
| `HTTP` | httpfs | HTTP/HTTPS endpoints |
| `ICEBERG` | httpfs, iceberg | Iceberg REST Catalogs |
| `DUCKLAKE` | ducklake | DuckLake lakehouse format |

### Secret Providers

- **`CONFIG`** (default): Manually specify all credentials
- **`CREDENTIAL_CHAIN`**: Auto-discover credentials from:
  - Environment variables
  - AWS IAM roles / GCS service accounts
  - Local credential files

### Useful DuckDB Secrets Functions

```sql
-- List all secrets (sensitive info redacted)
SELECT * FROM duckdb_secrets();

-- Determine which secret is used for a path
SELECT which_secret('s3://bucket/file.parquet', 's3');

-- Clear a secret
DROP SECRET secret_name;
```

---

## 15. Glossary

- **Credential**: Username, password, API key, or token used for authentication
- **Secret**: A named, typed credential stored in DuckDB's Secrets Manager
- **Secret Provider**: Mechanism for how a secret is created (CONFIG or CREDENTIAL_CHAIN)
- **Scope**: Optional path prefix that restricts a secret to specific resources
- **Persistent Secret**: Secret stored on disk and loaded automatically on startup
- **Temporary Secret**: In-memory secret that expires when DuckDB instance closes
- **Secrets File**: YAML file containing multiple secret definitions

---

**End of Specification**
