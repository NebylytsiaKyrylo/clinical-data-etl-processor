"""
test_load.py — Tests for the load module.

This test module verifies the functionality of scripts/load.py,
specifically the save_csv function which writes processed DataFrames
to CSV files in the processed data directory.

Key testing patterns:
  1. Mocking file paths (DATA_PROCESSED) to use temporary directories
  2. Verifying file creation and content correctness
  3. Testing error handling and logging behavior
"""

import pandas as pd

from scripts.load import save_csv


class TestSaveCsv:
    """
    Test suite for the save_csv function.

    The save_csv function is responsible for:
      - Creating output directories if needed
      - Writing DataFrames to CSV format
      - Logging success/error messages
      - Handling exceptions gracefully
    """

    def test_save_csv_creates_output_file(self, tmp_path, monkeypatch):
        """
        Test that save_csv creates a CSV file in the output directory.

        Basic functionality check: does the function actually write a file?

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching module attributes

        Assert:
            - File is created at DATA_PROCESSED / "processed_data.csv"
            - File exists and is a file (not a directory)

        """
        # Create test DataFrame
        df = pd.DataFrame(
            {
                "user_id": ["u1", "u2"],
                "name": ["Alice", "Bob"],
                "age": [25, 30],
            }
        )

        # Mock the DATA_PROCESSED path to use our temporary directory
        monkeypatch.setattr("scripts.load.DATA_PROCESSED", tmp_path)

        # Call the function
        save_csv(df)

        # Assertions: verify file was created
        output_file = tmp_path / "processed_data.csv"
        assert output_file.exists(), "Output file should be created"
        assert output_file.is_file(), "Output should be a file, not a directory"

    def test_save_csv_writes_correct_data(self, tmp_path, monkeypatch):
        """
        Test that the CSV file contains the correct data.

        Verifies that the DataFrame is correctly serialized to CSV format.

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching module attributes

        Assert:
            - Written CSV contains all rows from the DataFrame
            - Column names are preserved
            - Values are correct

        """
        df = pd.DataFrame(
            {
                "user_id": ["u1", "u2", "u3"],
                "email": ["a@mail.com", "b@mail.com", "c@mail.com"],
                "age": [25, 30, 35],
            }
        )

        monkeypatch.setattr("scripts.load.DATA_PROCESSED", tmp_path)
        save_csv(df)

        # Read the saved file back and verify content
        saved_df = pd.read_csv(tmp_path / "processed_data.csv")

        assert len(saved_df) == 3, "All rows should be written"
        assert list(saved_df.columns) == ["user_id", "email", "age"]
        assert saved_df.iloc[0]["user_id"] == "u1"
        assert saved_df.iloc[2]["age"] == 35

    def test_save_csv_no_index_column(self, tmp_path, monkeypatch):
        """
        Test that the CSV file does not include the DataFrame index.

        When writing a DataFrame to CSV with index=False (as in save_csv),
        the row indices should not appear as a column in the output.

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching module attributes

        Assert:
            - No unnamed index column in the output
            - Column count matches the DataFrame's column count

        """
        df = pd.DataFrame(
            {
                "col1": [1, 2],
                "col2": ["a", "b"],
            }
        )

        monkeypatch.setattr("scripts.load.DATA_PROCESSED", tmp_path)
        save_csv(df)

        saved_df = pd.read_csv(tmp_path / "processed_data.csv")

        # Should have exactly 2 columns (no index)
        assert len(saved_df.columns) == 2
        assert list(saved_df.columns) == ["col1", "col2"]

    def test_save_csv_creates_directory_if_not_exists(self, tmp_path, monkeypatch):
        """
        Test that save_csv creates the output directory if it doesn't exist.

        The function calls os.makedirs with exist_ok=True, so it should
        create parent directories as needed.

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching module attributes

        Assert:
            - Directory is created
            - File is written successfully in the new directory

        """
        df = pd.DataFrame({"col": [1, 2]})

        # Create a path that doesn't exist yet
        non_existent_dir = tmp_path / "new_output" / "subdir"
        monkeypatch.setattr("scripts.load.DATA_PROCESSED", non_existent_dir)

        # Call save_csv
        save_csv(df)

        # Assertions: verify directory and file were created
        assert non_existent_dir.exists(), "Output directory should be created"
        assert (non_existent_dir / "processed_data.csv").exists(), "File should exist in the created directory"

    def test_save_csv_empty_dataframe_logs_warning(self, tmp_path, monkeypatch, caplog):
        """
        Test that saving an empty DataFrame logs a warning.

        When the input DataFrame is empty, the function logs a warning
        but still attempts to save the file.

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching module attributes
            caplog: pytest fixture to capture log output

        Assert:
            - Warning message is logged
            - File is still created (even if empty)

        """
        df_empty = pd.DataFrame()
        monkeypatch.setattr("scripts.load.DATA_PROCESSED", tmp_path)

        # Capture logs at WARNING level
        with caplog.at_level("WARNING"):
            save_csv(df_empty)

        # Assertions: verify warning was logged
        assert "Cannot load empty DataFrames" in caplog.text

        # Note: The function still creates the file even for empty DataFrames
        assert (tmp_path / "processed_data.csv").exists()

    def test_save_csv_success_logging(self, tmp_path, monkeypatch, caplog):
        """
        Test that a successful save operation is logged.

        When the DataFrame is saved successfully, a log message should
        be recorded at INFO level.

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching module attributes
            caplog: pytest fixture to capture log output

        Assert:
            - Success message is logged at INFO level

        """
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        monkeypatch.setattr("scripts.load.DATA_PROCESSED", tmp_path)

        # Capture logs at INFO level
        with caplog.at_level("INFO"):
            save_csv(df)

        # Assertions: verify success message was logged
        assert "Data loaded to .csv files successfully" in caplog.text

    def test_save_csv_handles_special_characters_in_data(self, tmp_path, monkeypatch):
        """
        Test that special characters in data are handled correctly.

        CSV files can contain special characters (commas, quotes, newlines, etc).
        pandas.to_csv should handle these properly using escaping.

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching module attributes

        Assert:
            - Data with special chars is preserved correctly
            - File can be read back without corruption

        """
        df = pd.DataFrame(
            {
                "name": ["Alice Johnson", "Bob, Jr."],
                "description": ['Says "Hello"', "Multiple words"],
            }
        )

        monkeypatch.setattr("scripts.load.DATA_PROCESSED", tmp_path)
        save_csv(df)

        # Read back and verify special characters are preserved
        saved_df = pd.read_csv(tmp_path / "processed_data.csv")

        assert saved_df.iloc[0]["name"] == "Alice Johnson"
        assert "," in saved_df.iloc[1]["name"]  # comma preserved
        assert saved_df.iloc[0]["description"] == 'Says "Hello"'

    def test_save_csv_preserves_data_types_in_file(self, tmp_path, monkeypatch):
        """
        Test that numeric and string data types are readable from the CSV.

        Note: CSV format is text-based, so when we read it back, numeric
        columns are read as numeric (thanks to pandas' type inference).

        Args:
            tmp_path: pytest's temporary directory fixture
            monkeypatch: pytest fixture for patching module attributes

        Assert:
            - Numeric columns are read back as numeric
            - String columns remain strings

        """
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "score": [95.5, 87.3, 92.1],
                "email": ["a@mail.com", "b@mail.com", "c@mail.com"],
            }
        )

        monkeypatch.setattr("scripts.load.DATA_PROCESSED", tmp_path)
        save_csv(df)

        saved_df = pd.read_csv(tmp_path / "processed_data.csv")

        # Verify types (pandas infers them when reading)
        assert saved_df["user_id"].dtype in [int, "int64"]
        assert saved_df["score"].dtype == float
        assert saved_df["email"].dtype == object or saved_df["email"].dtype == "string"
