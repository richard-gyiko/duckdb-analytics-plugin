# Secrets Management

Manage credentials securely using a YAML secrets file instead of embedding them in requests.

## Quick Start

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

## Secrets File Format

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

## Supported Secret Types

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

## Environment Variable Substitution

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

## Multi-Database Example

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

## Security Best Practices

1. **Keep secrets.yaml in .gitignore** - Never commit credentials to version control
2. **Use environment variables** - Store actual passwords in env vars, not the YAML file
3. **Restrict file permissions** - Use `chmod 600 secrets.yaml` on Unix systems
4. **Use scoped secrets** - Limit S3/GCS/Azure secrets to specific bucket prefixes
