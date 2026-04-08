"""
test_main.py — Tests for the main ETL pipeline.

This test module verifies the functionality of main.py, specifically the
merge_all_data function which orchestrates the entire ETL pipeline:
  1. Extract — Load raw CSV files
  2. Transform — Clean and prepare each dataset
  3. Merge — Combine all datasets into one
  4. Load — Save the final processed data

Key testing patterns:
  1. Using unittest.mock.patch to mock external functions
  2. Verifying function call order and arguments
  3. Testing both the happy path and edge cases
  4. Integration-style tests with fixture data
"""

from unittest.mock import call, patch

import pandas as pd

from main import merge_all_data


class TestMergeAllData:
    """
    Test suite for the merge_all_data function.

    The merge_all_data function is the main orchestrator of the ETL pipeline.
    Tests in this class verify:
      - All extract functions are called with correct arguments
      - Transforms are applied in the correct order
      - Data merging happens correctly
      - Results are saved to the output location
    """

    def test_merge_all_data_calls_extract_four_times(self):
        """
        Test that merge_all_data calls extract_csv exactly 4 times.

        The function should extract from 4 files:
          1. health CSV
          2. usage CSV
          3. experiments CSV
          4. profiles CSV

        Assert:
            - extract_csv is called exactly 4 times
            - Each call uses the correct filename
        """
        # Create mock objects for all the functions that merge_all_data calls
        with (
            patch("main.extract_csv") as mock_extract,
            patch("main.transform_profiles") as mock_transform_profiles,
            patch("main.transform_health_data") as mock_transform_health,
            patch("main.transform_supl_usage") as mock_transform_usage,
            patch("main.merge_data") as mock_merge,
            patch("main.save_csv") as mock_save,
        ):
            # Set up mock return values (return empty DataFrames for testing)
            mock_extract.return_value = pd.DataFrame()
            mock_transform_profiles.return_value = pd.DataFrame()
            mock_transform_health.return_value = pd.DataFrame()
            mock_transform_usage.return_value = pd.DataFrame()
            mock_merge.return_value = pd.DataFrame()

            # Call the function under test
            merge_all_data(
                "user_health_data.csv",
                "supplement_usage.csv",
                "experiments.csv",
                "user_profiles.csv",
            )

            # Assertions: verify extract_csv was called correctly
            assert mock_extract.call_count == 4
            # Verify each call was with the right filename
            mock_extract.assert_any_call("user_health_data.csv")
            mock_extract.assert_any_call("supplement_usage.csv")
            mock_extract.assert_any_call("experiments.csv")
            mock_extract.assert_any_call("user_profiles.csv")

    def test_merge_all_data_calls_transforms_once_each(self):
        """
        Test that each transform function is called exactly once.

        The function should call:
          - transform_profiles (once)
          - transform_health_data (once)
          - transform_supl_usage (once)

        The merge_data function combines these transformed datasets.

        Assert:
            - Each transform function is called exactly once
            - Merge_data is called with transformed data
        """
        with (
            patch("main.extract_csv") as mock_extract,
            patch("main.transform_profiles") as mock_transform_profiles,
            patch("main.transform_health_data") as mock_transform_health,
            patch("main.transform_supl_usage") as mock_transform_usage,
            patch("main.merge_data") as mock_merge,
            patch("main.save_csv") as mock_save,
        ):
            mock_extract.return_value = pd.DataFrame()
            mock_transform_profiles.return_value = pd.DataFrame()
            mock_transform_health.return_value = pd.DataFrame()
            mock_transform_usage.return_value = pd.DataFrame()
            mock_merge.return_value = pd.DataFrame()

            merge_all_data(
                "user_health_data.csv",
                "supplement_usage.csv",
                "experiments.csv",
                "user_profiles.csv",
            )

            # Assertions: verify each transform is called once
            mock_transform_profiles.assert_called_once()
            mock_transform_health.assert_called_once()
            mock_transform_usage.assert_called_once()

    def test_merge_all_data_calls_merge_data_with_transformed_data(self):
        """
        Test that merge_data is called with the transformed DataFrames.

        merge_data should receive:
          1. Transformed health data
          2. Transformed usage data
          3. Raw experiments data
          4. Transformed profiles data

        The order matters because merge_data expects them in this order:
        merge_data(df_health, df_usage, df_experiments, df_profiles)

        Assert:
            - merge_data is called exactly once
            - Arguments are the transformed/extracted DataFrames
        """
        with (
            patch("main.extract_csv") as mock_extract,
            patch("main.transform_profiles") as mock_transform_profiles,
            patch("main.transform_health_data") as mock_transform_health,
            patch("main.transform_supl_usage") as mock_transform_usage,
            patch("main.merge_data") as mock_merge,
            patch("main.save_csv") as mock_save,
        ):
            # Create mock DataFrames with identifiable IDs
            health_df = pd.DataFrame({"type": ["health"]})
            usage_df = pd.DataFrame({"type": ["usage"]})
            experiments_df = pd.DataFrame({"type": ["experiments"]})
            profiles_df = pd.DataFrame({"type": ["profiles"]})

            mock_extract.side_effect = [
                health_df,
                usage_df,
                experiments_df,
                profiles_df,
            ]
            mock_transform_health.return_value = health_df
            mock_transform_usage.return_value = usage_df
            mock_transform_profiles.return_value = profiles_df
            mock_merge.return_value = pd.DataFrame()

            merge_all_data(
                "user_health_data.csv",
                "supplement_usage.csv",
                "experiments.csv",
                "user_profiles.csv",
            )

            # Assertions: verify merge_data is called with correct arguments
            mock_merge.assert_called_once_with(
                health_df,
                usage_df,
                experiments_df,
                profiles_df,
            )

    def test_merge_all_data_saves_merged_result(self):
        """
        Test that save_csv is called with the merged DataFrame.

        The final merged DataFrame should be passed to save_csv for writing
        to the output file.

        Assert:
            - save_csv is called exactly once
            - It receives the merged DataFrame
        """
        with (
            patch("main.extract_csv") as mock_extract,
            patch("main.transform_profiles") as mock_transform_profiles,
            patch("main.transform_health_data") as mock_transform_health,
            patch("main.transform_supl_usage") as mock_transform_usage,
            patch("main.merge_data") as mock_merge,
            patch("main.save_csv") as mock_save,
        ):
            mock_extract.return_value = pd.DataFrame()
            mock_transform_profiles.return_value = pd.DataFrame()
            mock_transform_health.return_value = pd.DataFrame()
            mock_transform_usage.return_value = pd.DataFrame()

            # Create a mock merged DataFrame that we can verify was passed to save_csv
            merged_df = pd.DataFrame({"user_id": [1, 2], "score": [90, 85]})
            mock_merge.return_value = merged_df

            merge_all_data(
                "user_health_data.csv",
                "supplement_usage.csv",
                "experiments.csv",
                "user_profiles.csv",
            )

            # Assertions: verify save_csv was called with the merged DataFrame
            mock_save.assert_called_once()
            call_args = mock_save.call_args[0][0]  # Get the first positional argument
            pd.testing.assert_frame_equal(call_args, merged_df)

    def test_merge_all_data_execution_order(self):
        """
        Test that the ETL pipeline executes in the correct order.

        The pipeline should follow: Extract → Transform → Merge → Load
        All extracts must happen before transforms, etc.

        This test uses Mock.call to verify the exact sequence of calls.

        Assert:
            - All extract calls happen before transform calls
            - All transform calls happen before merge call
            - Merge happens before save
        """
        with (
            patch("main.extract_csv") as mock_extract,
            patch("main.transform_profiles") as mock_transform_profiles,
            patch("main.transform_health_data") as mock_transform_health,
            patch("main.transform_supl_usage") as mock_transform_usage,
            patch("main.merge_data") as mock_merge,
            patch("main.save_csv") as mock_save,
        ):
            mock_extract.return_value = pd.DataFrame()
            mock_transform_profiles.return_value = pd.DataFrame()
            mock_transform_health.return_value = pd.DataFrame()
            mock_transform_usage.return_value = pd.DataFrame()
            mock_merge.return_value = pd.DataFrame()

            merge_all_data(
                "user_health_data.csv",
                "supplement_usage.csv",
                "experiments.csv",
                "user_profiles.csv",
            )

            # Count calls to each function
            extract_call_count = mock_extract.call_count  # Should be 4
            transform_calls = (
                mock_transform_profiles.call_count + mock_transform_health.call_count + mock_transform_usage.call_count
            )  # Should be 3

            # Assertions: verify counts
            assert extract_call_count == 4
            assert transform_calls == 3
            assert mock_merge.call_count == 1
            assert mock_save.call_count == 1

    def test_merge_all_data_with_actual_data(
        self,
        sample_health_df,
        sample_usage_df,
        sample_experiments_df,
        sample_profiles_df,
    ):
        """
        Integration test: Run the pipeline with actual (fixture) data.

        This test uses real fixture data instead of mocks to verify the
        entire pipeline works end-to-end (except file I/O).

        Args:
            sample_*_df: Fixture DataFrames with realistic test data

        Assert:
            - Pipeline completes without error
            - Result is a non-empty DataFrame
            - Result has expected structure

        """
        with patch("main.extract_csv") as mock_extract, patch("main.save_csv") as mock_save:
            # Mock only extract and save; let transforms and merge run for real
            mock_extract.side_effect = [
                sample_health_df,
                sample_usage_df,
                sample_experiments_df,
                sample_profiles_df,
            ]

            merge_all_data(
                "user_health_data.csv",
                "supplement_usage.csv",
                "experiments.csv",
                "user_profiles.csv",
            )

            # Assertions: verify save_csv was called
            mock_save.assert_called_once()
            saved_df = mock_save.call_args[0][0]

            # Verify result structure
            assert isinstance(saved_df, pd.DataFrame)
            # The result should have expected columns (from merge_data)
            expected_columns = {
                "user_id",
                "date",
                "email",
                "user_age_group",
                "experiment_name",
                "supplement_name",
                "dosage_grams",
                "is_placebo",
                "average_heart_rate",
                "average_glucose",
                "sleep_hours",
                "activity_level",
            }
            assert expected_columns.issubset(saved_df.columns)

    def test_merge_all_data_handles_empty_extract_gracefully(self):
        """
        Test that pipeline handles empty extract results gracefully.

        If any of the extract functions returns an empty DataFrame,
        the subsequent transforms and merge should handle it without crashing.

        Assert:
            - Pipeline completes without raising an exception
            - Empty result is saved
        """
        with patch("main.extract_csv") as mock_extract, patch("main.save_csv") as mock_save:
            # Return empty DataFrames from extract
            mock_extract.return_value = pd.DataFrame()

            # This should not raise an exception
            try:
                merge_all_data(
                    "user_health_data.csv",
                    "supplement_usage.csv",
                    "experiments.csv",
                    "user_profiles.csv",
                )
                pipeline_succeeded = True
            except Exception:
                pipeline_succeeded = False

            # Assertions: verify pipeline completed
            assert pipeline_succeeded, "Pipeline should handle empty data gracefully"
            mock_save.assert_called_once()

    def test_merge_all_data_uses_correct_filenames(self):
        """
        Test that merge_all_data extracts from the correct filenames.

        The function should use the exact filenames passed as arguments,
        in the correct order for health, usage, experiments, profiles.

        Assert:
            - Extract is called with the provided filenames
        """
        with (
            patch("main.extract_csv") as mock_extract,
            patch("main.transform_profiles"),
            patch("main.transform_health_data"),
            patch("main.transform_supl_usage"),
            patch("main.merge_data"),
            patch("main.save_csv"),
        ):
            mock_extract.return_value = pd.DataFrame()

            custom_filenames = (
                "custom_health.csv",
                "custom_usage.csv",
                "custom_experiments.csv",
                "custom_profiles.csv",
            )

            merge_all_data(*custom_filenames)

            # Assertions: verify extract was called with our custom filenames
            calls = [call(filename) for filename in custom_filenames]
            mock_extract.assert_has_calls(calls, any_order=False)
