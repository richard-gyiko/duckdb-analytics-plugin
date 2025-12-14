"""Tests for query mode functionality."""

import json
import subprocess
from pathlib import Path


SCRIPT_PATH = Path(__file__).parent.parent / "skills" / "data-wrangler" / "scripts" / "query_duckdb.py"


def run_script(request_obj: dict) -> str | dict:
    """Run the query_duckdb.py script and return output."""
    result = subprocess.run(
        ["uv", "run", str(SCRIPT_PATH)],
        input=json.dumps(request_obj),
        capture_output=True,
        text=True,
        cwd=str(SCRIPT_PATH.parent.parent.parent.parent),
    )
    return result.stdout, result.stderr


def run_script_json(request_obj: dict) -> dict:
    """Run script and parse JSON response."""
    stdout, stderr = run_script(request_obj)
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {"raw_stdout": stdout, "raw_stderr": stderr}


class TestQueryModeMarkdown:
    """Test default markdown output format."""

    def test_markdown_output_format(self):
        """Test that default output is markdown table."""
        req = {"query": "SELECT 1 as id, 'hello' as msg"}
        stdout, _ = run_script(req)
        
        # Should be markdown table format
        assert "|" in stdout
        assert "id" in stdout
        assert "msg" in stdout
        assert "hello" in stdout

    def test_markdown_multiple_rows(self):
        """Test markdown with multiple rows."""
        req = {"query": "SELECT * FROM range(5) t(n)"}
        stdout, _ = run_script(req)
        
        assert "| n |" in stdout or "|n|" in stdout.replace(" ", "")
        # Should have header separator
        assert "---" in stdout or "─" in stdout

    def test_markdown_truncation_message(self):
        """Test truncation message appears when results exceed limit."""
        req = {
            "query": "SELECT * FROM range(300) t(n)",
            "options": {"max_rows": 10}
        }
        stdout, _ = run_script(req)
        
        assert "truncated" in stdout.lower()


class TestQueryModeJSON:
    """Test JSON output format with schema."""

    def test_json_format_structure(self):
        """Test JSON format returns schema and rows."""
        req = {
            "query": "SELECT 1 as id, 'test' as name",
            "options": {"format": "json"}
        }
        result = run_script_json(req)
        
        assert "schema" in result
        assert "rows" in result
        assert result["error"] is None
        assert len(result["schema"]) == 2
        assert result["schema"][0]["name"] == "id"

    def test_json_format_types(self):
        """Test that types are reported correctly."""
        req = {
            "query": "SELECT 42 as int_col, 3.14::DOUBLE as float_col, 'text' as str_col",
            "options": {"format": "json"}
        }
        result = run_script_json(req)
        
        schema_map = {s["name"]: s["type"].lower() for s in result["schema"]}
        assert "int" in schema_map["int_col"]
        # DuckDB may return Float64, Double, or similar
        assert any(t in schema_map["float_col"] for t in ["float", "double"])


class TestQueryModeRecords:
    """Test records output format (list of dicts)."""

    def test_records_format(self):
        """Test records format returns list of dicts."""
        req = {
            "query": "SELECT 1 as id, 'alice' as name UNION ALL SELECT 2, 'bob'",
            "options": {"format": "records"}
        }
        result = run_script_json(req)
        
        assert "data" in result
        assert isinstance(result["data"], list)
        assert len(result["data"]) == 2
        assert result["data"][0]["name"] == "alice"
        assert result["data"][1]["id"] == 2


class TestQueryModeCSV:
    """Test CSV output format."""

    def test_csv_format(self):
        """Test CSV string output."""
        req = {
            "query": "SELECT 1 as id, 'test' as name",
            "options": {"format": "csv"}
        }
        result = run_script_json(req)
        
        assert "csv" in result
        assert "id" in result["csv"]
        assert "name" in result["csv"]
        assert "test" in result["csv"]


class TestQueryFromFile:
    """Test querying files directly."""

    def test_query_csv_file(self, tmp_path):
        """Test querying a CSV file directly."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name,value\n1,alice,100\n2,bob,200\n")
        
        req = {
            "query": f"SELECT * FROM '{csv_file}'",
            "options": {"format": "records"}
        }
        result = run_script_json(req)
        
        assert result["error"] is None
        assert len(result["data"]) == 2
        assert result["data"][0]["name"] == "alice"

    def test_query_with_aggregation(self, tmp_path):
        """Test aggregation query on file."""
        csv_file = tmp_path / "sales.csv"
        csv_file.write_text("region,amount\nNorth,100\nSouth,200\nNorth,150\n")
        
        req = {
            "query": f"SELECT region, SUM(amount) as total FROM '{csv_file}' GROUP BY region ORDER BY region",
            "options": {"format": "records"}
        }
        result = run_script_json(req)
        
        assert result["error"] is None
        assert len(result["data"]) == 2
        assert result["data"][0]["region"] == "North"
        # Handle both int and string representations
        assert int(result["data"][0]["total"]) == 250

    def test_query_parquet_file(self, tmp_path):
        """Test querying a Parquet file."""
        # First create a parquet file
        parquet_file = tmp_path / "data.parquet"
        create_req = {
            "query": "SELECT 1 as id, 'test' as name",
            "output": {"path": str(parquet_file), "format": "parquet"}
        }
        run_script_json(create_req)
        
        # Now query it
        req = {
            "query": f"SELECT * FROM '{parquet_file}'",
            "options": {"format": "records"}
        }
        result = run_script_json(req)
        
        assert result["error"] is None
        assert result["data"][0]["id"] == 1


class TestQueryWithSources:
    """Test aliased sources."""

    def test_source_alias(self, tmp_path):
        """Test using source alias instead of direct path."""
        csv_file = tmp_path / "users.csv"
        csv_file.write_text("id,name\n1,alice\n2,bob\n")
        
        req = {
            "query": "SELECT * FROM users ORDER BY id",
            "sources": [{"type": "file", "alias": "users", "path": str(csv_file)}],
            "options": {"format": "records"}
        }
        result = run_script_json(req)
        
        assert result["error"] is None
        assert len(result["data"]) == 2

    def test_join_two_sources(self, tmp_path):
        """Test joining two aliased sources."""
        users_file = tmp_path / "users.csv"
        users_file.write_text("id,name\n1,alice\n2,bob\n")
        
        orders_file = tmp_path / "orders.csv"
        orders_file.write_text("user_id,amount\n1,100\n1,50\n2,200\n")
        
        req = {
            "query": """
                SELECT u.name, SUM(o.amount) as total
                FROM users u
                JOIN orders o ON u.id = o.user_id
                GROUP BY u.name
                ORDER BY u.name
            """,
            "sources": [
                {"type": "file", "alias": "users", "path": str(users_file)},
                {"type": "file", "alias": "orders", "path": str(orders_file)}
            ],
            "options": {"format": "records"}
        }
        result = run_script_json(req)
        
        assert result["error"] is None
        assert len(result["data"]) == 2
        assert result["data"][0]["name"] == "alice"
        # Handle both int and string representations
        assert int(result["data"][0]["total"]) == 150


class TestErrorHandling:
    """Test error messages are helpful."""

    def test_missing_query(self):
        """Test error when query is missing."""
        req = {}
        result = run_script_json(req)
        
        assert result.get("error") is not None
        assert "query" in result["error"].lower()

    def test_invalid_sql(self):
        """Test error for invalid SQL syntax."""
        req = {"query": "SELEKT * FORM table"}
        result = run_script_json(req)
        
        assert result.get("error") is not None

    def test_missing_table(self):
        """Test error when table doesn't exist."""
        req = {
            "query": "SELECT * FROM nonexistent_table_xyz",
            "options": {"format": "json"}
        }
        result = run_script_json(req)
        
        assert result.get("error") is not None

    def test_missing_file(self):
        """Test error when file doesn't exist."""
        req = {
            "query": "SELECT * FROM '/nonexistent/path/file.csv'",
            "options": {"format": "json"}
        }
        result = run_script_json(req)
        
        assert result.get("error") is not None

    def test_missing_column(self, tmp_path):
        """Test error for missing column reference."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,alice\n")
        
        req = {
            "query": f"SELECT nonexistent_column FROM '{csv_file}'",
            "options": {"format": "json"}
        }
        result = run_script_json(req)
        
        assert result.get("error") is not None

    def test_invalid_json_input(self):
        """Test error for malformed JSON input."""
        result = subprocess.run(
            ["uv", "run", str(SCRIPT_PATH)],
            input="not valid json {{{",
            capture_output=True,
            text=True,
            cwd=str(SCRIPT_PATH.parent.parent.parent.parent),
        )
        response = json.loads(result.stdout)
        assert response.get("error") is not None
        assert "json" in response["error"].lower()


class TestUtilityStatements:
    """Test DESCRIBE, SUMMARIZE, etc."""

    def test_describe(self, tmp_path):
        """Test DESCRIBE statement."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name,value\n1,alice,100\n")
        
        req = {"query": f"DESCRIBE SELECT * FROM '{csv_file}'"}
        stdout, _ = run_script(req)
        
        assert "id" in stdout
        assert "name" in stdout
        assert "value" in stdout

    def test_summarize(self, tmp_path):
        """Test SUMMARIZE statement."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,value\n1,100\n2,200\n3,300\n")
        
        req = {"query": f"SUMMARIZE SELECT * FROM '{csv_file}'"}
        stdout, _ = run_script(req)
        
        # Should contain statistical info
        assert "min" in stdout.lower() or "100" in stdout


class TestRoundTrip:
    """Test write then read back - data integrity."""

    def test_parquet_round_trip(self, tmp_path):
        """Write Parquet, read back, verify data matches."""
        parquet_file = tmp_path / "roundtrip.parquet"
        
        # Write
        write_req = {
            "query": "SELECT 42 as num, 'hello' as text, 3.14 as decimal",
            "output": {"path": str(parquet_file), "format": "parquet"}
        }
        write_result = run_script_json(write_req)
        assert write_result["success"] is True
        
        # Read back
        read_req = {
            "query": f"SELECT * FROM '{parquet_file}'",
            "options": {"format": "records"}
        }
        read_result = run_script_json(read_req)
        
        assert read_result["error"] is None
        assert int(read_result["data"][0]["num"]) == 42
        assert read_result["data"][0]["text"] == "hello"
        # Handle both float and string representations
        assert abs(float(read_result["data"][0]["decimal"]) - 3.14) < 0.001

    def test_csv_round_trip(self, tmp_path):
        """Write CSV, read back, verify data matches."""
        csv_file = tmp_path / "roundtrip.csv"
        
        # Write
        write_req = {
            "query": "SELECT 1 as id, 'alice' as name UNION ALL SELECT 2, 'bob'",
            "output": {"path": str(csv_file), "format": "csv"}
        }
        write_result = run_script_json(write_req)
        assert write_result["success"] is True
        
        # Read back
        read_req = {
            "query": f"SELECT * FROM '{csv_file}' ORDER BY id",
            "options": {"format": "records"}
        }
        read_result = run_script_json(read_req)
        
        assert read_result["error"] is None
        assert len(read_result["data"]) == 2
        assert read_result["data"][0]["name"] == "alice"

    def test_json_round_trip(self, tmp_path):
        """Write JSON, read back, verify data matches."""
        json_file = tmp_path / "roundtrip.json"
        
        # Write as JSON array
        write_req = {
            "query": "SELECT 'value1' as field1, 123 as field2",
            "output": {"path": str(json_file), "format": "json", "options": {"array": True}}
        }
        write_result = run_script_json(write_req)
        assert write_result["success"] is True
        
        # Read back
        read_req = {
            "query": f"SELECT * FROM '{json_file}'",
            "options": {"format": "records"}
        }
        read_result = run_script_json(read_req)
        
        assert read_result["error"] is None
        assert read_result["data"][0]["field1"] == "value1"
        assert read_result["data"][0]["field2"] == 123


class TestEdgeCases:
    """Test boundary conditions and edge cases."""

    def test_empty_result(self):
        """Test query returning zero rows."""
        req = {
            "query": "SELECT 1 as id WHERE false",
            "options": {"format": "records"}
        }
        result = run_script_json(req)
        
        assert result["error"] is None
        assert result["data"] == []

    def test_null_values(self):
        """Test handling of NULL values."""
        req = {
            "query": "SELECT NULL as nullable, 1 as non_null",
            "options": {"format": "records"}
        }
        result = run_script_json(req)
        
        assert result["error"] is None
        assert result["data"][0]["nullable"] is None
        assert result["data"][0]["non_null"] == 1

    def test_special_characters_in_data(self, tmp_path):
        """Test data with special characters."""
        csv_file = tmp_path / "special.csv"
        csv_file.write_text('id,text\n1,"hello, world"\n2,"quote""test"\n')
        
        req = {
            "query": f"SELECT * FROM '{csv_file}' ORDER BY id",
            "options": {"format": "records"}
        }
        result = run_script_json(req)
        
        assert result["error"] is None
        assert result["data"][0]["text"] == "hello, world"

    def test_unicode_data(self, tmp_path):
        """Test Unicode characters in data."""
        csv_file = tmp_path / "unicode.csv"
        csv_file.write_text('id,text\n1,你好\n2,مرحبا\n3,🎉\n', encoding='utf-8')
        
        req = {
            "query": f"SELECT * FROM '{csv_file}' ORDER BY id",
            "options": {"format": "records"}
        }
        result = run_script_json(req)
        
        assert result["error"] is None
        assert result["data"][0]["text"] == "你好"
        assert result["data"][2]["text"] == "🎉"

    def test_large_result_truncation(self):
        """Test that large results are properly truncated."""
        req = {
            "query": "SELECT * FROM range(1000) t(n)",
            "options": {"format": "records", "max_rows": 50}
        }
        result = run_script_json(req)
        
        assert result["error"] is None
        assert len(result["data"]) == 50
        assert result["truncated"] is True
