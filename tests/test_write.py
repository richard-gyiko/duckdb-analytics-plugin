"""Tests for write/export functionality."""

import json
import subprocess
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parent.parent / "skills" / "data-wrangler" / "scripts" / "query_duckdb.py"


def run_script(request_obj: dict) -> dict:
    """Run the query_duckdb.py script with the given request and return JSON response."""
    result = subprocess.run(
        ["uv", "run", str(SCRIPT_PATH)],
        input=json.dumps(request_obj),
        capture_output=True,
        text=True,
        cwd=str(SCRIPT_PATH.parent.parent.parent.parent),
    )
    # For write mode, output should be JSON
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        # If not JSON, return raw output for debugging
        return {"raw_stdout": result.stdout, "raw_stderr": result.stderr}


class TestWriteParquet:
    """Test Parquet file writing."""

    def test_write_parquet_basic(self, tmp_path):
        """Test basic Parquet write from inline data."""
        output_file = tmp_path / "output.parquet"
        req = {
            "query": "SELECT 1 as id, 'hello' as msg UNION ALL SELECT 2, 'world'",
            "output": {
                "path": str(output_file),
                "format": "parquet",
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True, f"Write failed: {result}"
        assert result.get("output_path") == str(output_file)
        assert result.get("format") == "parquet"
        assert output_file.exists()
        assert result.get("file_size_bytes", 0) > 0
        assert result.get("duration_ms") >= 0

    def test_write_parquet_with_compression(self, tmp_path):
        """Test Parquet write with zstd compression."""
        output_file = tmp_path / "compressed.parquet"
        req = {
            "query": "SELECT * FROM range(100) t(id)",
            "output": {
                "path": str(output_file),
                "format": "parquet",
                "options": {"compression": "zstd"},
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True
        assert output_file.exists()

    def test_write_parquet_with_row_group_size(self, tmp_path):
        """Test Parquet write with custom row group size."""
        output_file = tmp_path / "custom_rg.parquet"
        req = {
            "query": "SELECT * FROM range(1000) t(id)",
            "output": {
                "path": str(output_file),
                "format": "parquet",
                "options": {"row_group_size": 100},
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True
        assert output_file.exists()


class TestWriteCSV:
    """Test CSV file writing."""

    def test_write_csv_basic(self, tmp_path):
        """Test basic CSV write."""
        output_file = tmp_path / "output.csv"
        req = {
            "query": "SELECT 1 as id, 'test' as name",
            "output": {
                "path": str(output_file),
                "format": "csv",
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True
        assert output_file.exists()
        
        content = output_file.read_text()
        assert "id" in content  # header
        assert "name" in content
        assert "1" in content
        assert "test" in content

    def test_write_csv_no_header(self, tmp_path):
        """Test CSV write without header."""
        output_file = tmp_path / "no_header.csv"
        req = {
            "query": "SELECT 1 as id, 'test' as name",
            "output": {
                "path": str(output_file),
                "format": "csv",
                "options": {"header": False},
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True
        content = output_file.read_text().strip()
        # Without header, first line should be data
        assert content == '1,test'

    def test_write_csv_custom_delimiter(self, tmp_path):
        """Test CSV write with custom delimiter."""
        output_file = tmp_path / "tab_delimited.csv"
        req = {
            "query": "SELECT 1 as id, 'test' as name",
            "output": {
                "path": str(output_file),
                "format": "csv",
                "options": {"delimiter": "\t"},
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True
        content = output_file.read_text()
        assert "\t" in content


class TestWriteJSON:
    """Test JSON file writing."""

    def test_write_json_array(self, tmp_path):
        """Test JSON array format write."""
        output_file = tmp_path / "output.json"
        req = {
            "query": "SELECT 1 as id, 'hello' as msg",
            "output": {
                "path": str(output_file),
                "format": "json",
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True
        assert output_file.exists()
        
        content = json.loads(output_file.read_text())
        assert isinstance(content, list)
        assert len(content) == 1
        assert content[0]["id"] == 1
        assert content[0]["msg"] == "hello"

    def test_write_ndjson(self, tmp_path):
        """Test newline-delimited JSON format write."""
        output_file = tmp_path / "output.ndjson"
        req = {
            "query": "SELECT 1 as id UNION ALL SELECT 2",
            "output": {
                "path": str(output_file),
                "format": "json",
                "options": {"array": False},
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["id"] == 1
        assert json.loads(lines[1])["id"] == 2


class TestOverwriteProtection:
    """Test overwrite protection behavior."""

    def test_overwrite_protection_default(self, tmp_path):
        """Test that overwrite is prevented by default."""
        output_file = tmp_path / "exists.parquet"
        
        # First write
        req = {
            "query": "SELECT 1 as id",
            "output": {
                "path": str(output_file),
                "format": "parquet",
            },
        }
        result = run_script(req)
        assert result.get("success") is True
        
        # Second write without overwrite - should fail
        result2 = run_script(req)
        assert result2.get("success") is False
        assert "overwrite" in result2.get("error", "").lower() or "exists" in result2.get("error", "").lower()

    def test_overwrite_allowed_when_enabled(self, tmp_path):
        """Test that overwrite succeeds when enabled."""
        output_file = tmp_path / "overwrite_me.parquet"
        
        # First write
        req = {
            "query": "SELECT 1 as id",
            "output": {
                "path": str(output_file),
                "format": "parquet",
                "options": {"overwrite": True},
            },
        }
        result = run_script(req)
        assert result.get("success") is True
        
        # Second write with overwrite - should succeed
        req["query"] = "SELECT 2 as id"
        result2 = run_script(req)
        assert result2.get("success") is True


class TestPartitionedWrite:
    """Test partitioned write functionality."""

    def test_partition_by_single_column(self, tmp_path):
        """Test partitioning by a single column."""
        output_dir = tmp_path / "partitioned"
        req = {
            "query": """
                SELECT 'A' as category, 1 as value
                UNION ALL SELECT 'B', 2
                UNION ALL SELECT 'A', 3
            """,
            "output": {
                "path": str(output_dir),
                "format": "parquet",
                "options": {"partition_by": ["category"]},
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True
        assert output_dir.is_dir()
        # Should have partition directories
        assert (output_dir / "category=A").exists() or any("category=A" in str(p) for p in output_dir.rglob("*"))

    def test_partition_metadata_returned(self, tmp_path):
        """Test that partition info is returned in result."""
        output_dir = tmp_path / "partitioned2"
        req = {
            "query": """
                SELECT 2020 as year, 1 as value
                UNION ALL SELECT 2021, 2
                UNION ALL SELECT 2020, 3
            """,
            "output": {
                "path": str(output_dir),
                "format": "parquet",
                "options": {"partition_by": ["year"]},
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True
        # Should have file count and total size for partitioned output
        assert "file_count" in result or "total_size_bytes" in result


class TestWriteWithSources:
    """Test writing data from sources."""

    def test_write_from_csv_source(self, tmp_path):
        """Test reading from CSV and writing to Parquet."""
        # Create input CSV
        input_csv = tmp_path / "input.csv"
        input_csv.write_text("id,name\n1,alice\n2,bob\n")
        
        output_file = tmp_path / "output.parquet"
        req = {
            "query": "SELECT * FROM input_data WHERE id = 1",
            "sources": [
                {"type": "file", "alias": "input_data", "path": str(input_csv)}
            ],
            "output": {
                "path": str(output_file),
                "format": "parquet",
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True
        assert output_file.exists()

    def test_write_transformed_data(self, tmp_path):
        """Test applying transformation and writing."""
        input_csv = tmp_path / "sales.csv"
        input_csv.write_text("region,amount\nNorth,100\nSouth,200\nNorth,150\n")
        
        output_file = tmp_path / "summary.parquet"
        req = {
            "query": """
                SELECT region, SUM(amount) as total 
                FROM sales 
                GROUP BY region 
                ORDER BY region
            """,
            "sources": [
                {"type": "file", "alias": "sales", "path": str(input_csv)}
            ],
            "output": {
                "path": str(output_file),
                "format": "parquet",
            },
        }
        result = run_script(req)
        
        assert result.get("success") is True
        assert output_file.exists()


class TestWriteErrorHandling:
    """Test error handling for write operations."""

    def test_invalid_output_path(self, tmp_path):
        """Test error when output path is invalid."""
        req = {
            "query": "SELECT 1",
            "output": {
                "path": "/nonexistent/deeply/nested/path/file.parquet",
                "format": "parquet",
            },
        }
        result = run_script(req)
        
        # Should return an error
        assert result.get("success") is False or result.get("error") is not None

    def test_invalid_format(self, tmp_path):
        """Test error when format is invalid."""
        output_file = tmp_path / "output.xyz"
        req = {
            "query": "SELECT 1",
            "output": {
                "path": str(output_file),
                "format": "xyz",  # Invalid format
            },
        }
        result = run_script(req)
        
        # Should return an error about invalid format
        assert "error" in result or result.get("success") is False

    def test_query_error_during_write(self, tmp_path):
        """Test that query errors are properly reported."""
        output_file = tmp_path / "output.parquet"
        req = {
            "query": "SELECT * FROM nonexistent_table",
            "output": {
                "path": str(output_file),
                "format": "parquet",
            },
        }
        result = run_script(req)
        
        assert result.get("success") is False
        assert "error" in result
