"""
test_extract.py — Tests for the extract module.

This test module verifies the functionality of scripts/extract.py,
specifically the extract_csv function which reads CSV files from the raw data directory.

Key testing patterns:
  1. Mocking file paths with monkeypatch to avoid requiring actual files
  2. Using tmp_path fixture to create temporary CSV files for testing
  3. Testing both success and error cases (FileNotFoundError, other exceptions)
"""

from pathlib import Path

import pandas as pd

from scripts.extract import extract_csv


class TestExtractCsv:
    """
    Test suite for the extract_csv function.

    Grouping tests in a class helps organize related tests and provides
    a namespace to avoid naming conflicts.
    """

    def test_extract_csv_reads_valid_file_successfully(self, tmp_path, monkeypatch):
        """
        Test that extract_csv successfully reads a valid CSV file.

        Approach:
          1. Create a temporary CSV file with sample data
          2. Mock the DATA_RAW_DIR to point to our temporary directory
          3. Call extract_csv with the temporary file
          4. Verify the returned DataFrame has correct structure and data

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching (mocking) module attributes

        Assert:
            - Result is a DataFrame (not None or empty)
            - DataFrame has expected columns
            - DataFrame has correct number of rows

        """
        # Create a temporary CSV file with sample data
        csv_content = "user_id,age,email\nu1,25,alice@mail.com\nu2,30,bob@mail.com"
        csv_file = tmp_path / "test_users.csv"
        csv_file.write_text(csv_content)

        # Mock the DATA_RAW_DIR to point to our temporary directory
        # This prevents the function from looking in the actual data directory
        monkeypatch.setattr("scripts.extract.DATA_RAW_DIR", tmp_path)

        # Call the function under test
        result = extract_csv("test_users.csv")

        # Assertions: verify the result
        assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
        assert len(result) == 2, "DataFrame should have 2 rows"
        assert list(result.columns) == ["user_id", "age", "email"]
        assert result.iloc[0]["user_id"] == "u1"

    def test_extract_csv_file_not_found_returns_empty_dataframe(self, monkeypatch):
        """
        Test that extract_csv returns an empty DataFrame when file is not found.

        When extract_csv encounters a FileNotFoundError (file doesn't exist),
        it should return an empty DataFrame instead of raising an exception.
        This makes the ETL pipeline more resilient.

        Args:
            monkeypatch: pytest fixture for patching module attributes

        Assert:
            - Result is an empty DataFrame (not None)
            - .empty property returns True

        """
        # Mock DATA_RAW_DIR to a non-existent path
        monkeypatch.setattr("scripts.extract.DATA_RAW_DIR", Path("/nonexistent/path"))

        # Call the function with a file that doesn't exist
        result = extract_csv("nonexistent.csv")

        # Assertions: verify graceful error handling
        assert isinstance(result, pd.DataFrame), "Should return DataFrame, not raise"
        assert result.empty, "Should return an empty DataFrame"

    def test_extract_csv_malformed_file_returns_empty_dataframe(self, tmp_path, monkeypatch):
        """
        Test that extract_csv returns empty DataFrame when CSV is malformed.

        When pandas encounters an error reading the CSV (e.g., bad formatting),
        extract_csv should catch the exception and return an empty DataFrame.

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching module attributes

        Assert:
            - Result is an empty DataFrame despite the malformed file
            - No exception is raised to the caller

        """
        # Create a malformed CSV file (missing closing quote)
        bad_csv = tmp_path / "malformed.csv"
        bad_csv.write_text('user_id,age\n"u1",25\n"u2,30')  # Missing quote on second row

        monkeypatch.setattr("scripts.extract.DATA_RAW_DIR", tmp_path)

        # Call the function — should not raise
        result = extract_csv("malformed.csv")

        # Assertions: verify error resilience
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_extract_csv_logs_success_message(self, tmp_path, monkeypatch, caplog):
        """
        Test that extract_csv logs a success message when file is read.

        The extract function uses Python's logging module. We can capture
        log output using the caplog fixture to verify the function is
        logging appropriately.

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching module attributes
            caplog: pytest fixture to capture log output

        Assert:
            - Success message is logged at INFO level
            - Log message includes the filename and row count

        """
        csv_content = "col1,col2\n1,a\n2,b\n3,c"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        monkeypatch.setattr("scripts.extract.DATA_RAW_DIR", tmp_path)

        # Set logging level to capture INFO messages
        with caplog.at_level("INFO"):
            result = extract_csv("test.csv")

        # Assertions: verify logging
        assert len(result) == 3
        assert "Successfully loaded test.csv (3 rows)" in caplog.text

    def test_extract_csv_logs_error_on_file_not_found(self, monkeypatch, caplog):
        """
        Test that extract_csv logs an error message when file is not found.

        When a FileNotFoundError occurs, the function should log the error
        for debugging and monitoring purposes.

        Args:
            monkeypatch: pytest fixture for patching module attributes
            caplog: pytest fixture to capture log output

        Assert:
            - Error message is logged at ERROR level
            - Log mentions the missing file path

        """
        monkeypatch.setattr("scripts.extract.DATA_RAW_DIR", Path("/nonexistent"))

        with caplog.at_level("ERROR"):
            result = extract_csv("missing.csv")

        # Assertions: verify error logging
        assert "File not found" in caplog.text
        assert result.empty

    def test_extract_csv_preserves_data_types(self, tmp_path, monkeypatch):
        """
        Test that extract_csv preserves appropriate data types from the CSV.

        When reading a CSV, pandas infers data types. Numeric columns should
        be read as numeric, string columns as strings, etc.

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching module attributes

        Assert:
            - Numeric columns are float or int
            - String columns are object or StringDtype

        """
        csv_content = "user_id,age,email\n1,25,alice@mail.com\n2,30,bob@mail.com"
        csv_file = tmp_path / "data.csv"
        csv_file.write_text(csv_content)

        monkeypatch.setattr("scripts.extract.DATA_RAW_DIR", tmp_path)
        result = extract_csv("data.csv")

        # Assertions: verify data type preservation
        # user_id should be numeric, email should be string
        assert result["user_id"].dtype in [int, "int64"]
        assert result["age"].dtype in [int, "int64"]
        # Email will be object or StringDtype
        assert result["email"].dtype in [object, "string"]
