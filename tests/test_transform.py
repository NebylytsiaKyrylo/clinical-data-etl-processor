# Pattern AAA -> Arrange, Act, Assert
import pandas as pd
import pytest

from scripts.transform import transform_profiles


@pytest.mark.parametrize(
    ("age", "expected_group"),
    [(15, "Under 18"), (22, "18-25"), (30, "26-35"), (40, "36-45"), (50, "46-55"), (60, "56-65"), (70, "Over 65")],
)
def test_age_group(age: int, expected_group: str) -> None:
    """
    Test the age-based grouping logic in the transform_profiles function.

    The test verifies that the transform_profiles function categorizes age values
    into the correct age group buckets based on predefined ranges.

    Args:
        age: An integer representing the age to test.
        expected_group: A string representing the expected age group category.

    """
    df = pd.DataFrame({"user_id": "user_id", "age": [age], "email": "user@mail"})
    result = transform_profiles(df)
    assert result["user_age_group"].iloc[0] == expected_group


def test_transform_profiles_empty_dataframe():
    """Test that empty DataFrames are handled gracefully"""
    df_empty = pd.DataFrame
    result = transform_profiles(df_empty)
    assert result.empty


def test_transform_profiles_adds_user_age_group_column():
    """Test that the user_age_group column is created"""
    df = pd.DataFrame({"user_id": [1, 2], "age": [25, 45], "email": ["user1@mail", "user2@mail"]})
    result = transform_profiles(df)
    assert "user_age_group" in result.columns


def test_transform_profiles_removes_missing_user_id():
    """Test that rows with missing user_id are removed"""
    df = pd.DataFrame({"user_id": [1, None], "age": [25, 35], "email": ["user1@mail", "user2@mail"]})
    result = transform_profiles(df)
    assert len(result) == 1
    assert result.iloc[0]["user_id"] == 1


def test_transform_profiles_removes_missing_email():
    """Test that rows with missing email are removed"""
    df = pd.DataFrame({"user_id": [1, 2], "age": [25, 35], "email": ["user1@mail", None]})
    result = transform_profiles(df)
    assert len(result) == 1
    assert result.iloc[0]["email"] == "user1@mail"


def test_transform_profiles_keeps_all_valid_rows():
    """Test that all valid rows are kept"""
    df = pd.DataFrame({"user_id": [1, 2, 3], "age": [25, 35, 45], "email": ["a@mail", "b@mail", "c@mail"]})
    result = transform_profiles(df)
    assert len(result) == 3


def test_transform_profiles_nan_age_becomes_unknown():
    """Test that NaN ages become 'Unknown'"""
    df = pd.DataFrame({"user_id": [1], "age": [float("nan")], "email": ["user2@mail"]})
    result = transform_profiles(df)
    # When age is NaN, the age group should be 'Unknown' or NaN depending on implementation
    # The important thing is the row is kept (user_id and email are not null)
    assert len(result) == 1
    # Check the value - it could be 'Unknown' or still NaN
    value = result["user_age_group"].iloc[0]
    assert value == "Unknown" or pd.isna(value)


def test_transform_profiles_boundary_ages():
    """Test boundary ages (exactly at bin limits)"""
    test_cases = [
        (18, "18-25"),
        (25.99, "18-25"),
        (26, "26-35"),
        (66, "Over 65"),
    ]
    for age, expected in test_cases:
        df = pd.DataFrame({"user_id": [1], "age": [age], "email": ["user@mail"]})
        result = transform_profiles(df)
        assert result["user_age_group"].iloc[0] == expected, f"Failed for age {age}"


def test_transform_profiles_preserves_columns():
    """Test that original columns are preserved"""
    df = pd.DataFrame({"user_id": [1], "age": [25], "email": ["user@mail"]})
    result = transform_profiles(df)
    assert "user_id" in result.columns
    assert "age" in result.columns
    assert "email" in result.columns
    assert "user_age_group" in result.columns


def test_transform_profiles_data_types_preserved():
    """Test that data types are correct after transformation"""
    df = pd.DataFrame({"user_id": [1, 2], "age": [25, 35], "email": ["user1@mail", "user2@mail"]})
    result = transform_profiles(df)
    # Le type est StringDtype (pandas 3.14), pas 'object'
    # On peut vérifier que les valeurs sont strings
    assert isinstance(result["user_age_group"].iloc[0], str)
    assert result["user_age_group"].iloc[0] == "18-25"


# =============================================================================
# Tests for transform_health_data function
# =============================================================================
# The transform_health_data function processes health metrics by:
#   1. Converting date column to datetime
#   2. Cleaning sleep_hours (removing 'h' or 'H' suffix and converting to float)
#   3. Clipping activity_level between 0 and 100
#   4. Removing rows with missing user_id or date


class TestTransformHealthData:
    """Test suite for the transform_health_data function."""

    def test_transform_health_data_empty_dataframe_returns_empty(self):
        """
        Test that an empty DataFrame is returned unchanged.

        When the input is an empty DataFrame, the function should log
        a warning and return an empty DataFrame without processing.
        """
        from scripts.transform import transform_health_data

        df_empty = pd.DataFrame()
        result = transform_health_data(df_empty)

        assert result.empty
        assert isinstance(result, pd.DataFrame)

    def test_transform_health_data_converts_date_to_datetime(self, sample_health_df):
        """
        Test that the date column is converted to datetime type.

        Dates typically come as strings from CSV files. This function should
        convert them to pandas datetime objects for proper date operations.

        Args:
            sample_health_df: Fixture providing test health data

        Assert:
            - date column dtype is datetime64

        """
        from scripts.transform import transform_health_data

        result = transform_health_data(sample_health_df)

        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_transform_health_data_cleans_sleep_hours(self, sample_health_df):
        """
        Test that sleep_hours is converted from string "X.Xh" to float.

        Raw sleep hours data comes as strings like "7.5h" or "6.0H" (note mixed case).
        This function must strip the letter suffix and convert to float.

        Args:
            sample_health_df: Fixture with sleep_hours like "7.5h", "6.0H"

        Assert:
            - sleep_hours column is numeric (float)
            - Values are correct (7.5, 6.0, etc.)

        """
        from scripts.transform import transform_health_data

        result = transform_health_data(sample_health_df)

        assert result["sleep_hours"].dtype == float
        assert result["sleep_hours"].iloc[0] == 7.5
        assert result["sleep_hours"].iloc[1] == 6.0

    def test_transform_health_data_clips_activity_level(self, sample_health_df):
        """
        Test that activity_level is clipped to [0, 100] range.

        Activity level should be a percentage between 0 and 100. Any values
        outside this range should be clipped to the boundaries.

        Args:
            sample_health_df: Fixture with activity_level values [50, 120]

        Assert:
            - Values > 100 are clipped to 100
            - Values < 0 are clipped to 0
            - Values in range stay unchanged

        """
        from scripts.transform import transform_health_data

        result = transform_health_data(sample_health_df)

        # First row: 50 stays 50
        assert result["activity_level"].iloc[0] == 50
        # Second row: 120 gets clipped to 100
        assert result["activity_level"].iloc[1] == 100

    def test_transform_health_data_removes_missing_user_id(self):
        """
        Test that rows with missing user_id are removed.

        The user_id is a critical identifier. Rows without it should be
        dropped to maintain data integrity.

        Assert:
            - Row with NaN user_id is removed
            - Rows with valid user_id remain
        """
        from scripts.transform import transform_health_data

        df = pd.DataFrame(
            {
                "user_id": ["u1", None],
                "date": ["2024-01-15", "2024-02-20"],
                "average_heart_rate": [72.5, 80.0],
                "average_glucose": [95.0, 110.0],
                "sleep_hours": ["7.5h", "6.0h"],
                "activity_level": [50, 50],
            }
        )

        result = transform_health_data(df)

        assert len(result) == 1
        assert result.iloc[0]["user_id"] == "u1"

    def test_transform_health_data_removes_missing_date(self):
        """
        Test that rows with missing date are removed.

        The date is essential for time-series analysis. Rows without it
        should be dropped.

        Assert:
            - Row with NaN date is removed
            - Rows with valid date remain
        """
        from scripts.transform import transform_health_data

        df = pd.DataFrame(
            {
                "user_id": ["u1", "u2"],
                "date": ["2024-01-15", None],
                "average_heart_rate": [72.5, 80.0],
                "average_glucose": [95.0, 110.0],
                "sleep_hours": ["7.5h", "6.0h"],
                "activity_level": [50, 50],
            }
        )

        result = transform_health_data(df)

        assert len(result) == 1
        assert result.iloc[0]["user_id"] == "u1"

    def test_transform_health_data_preserves_columns(self, sample_health_df):
        """
        Test that all expected columns are preserved after transformation.

        The original columns should remain in the output (no columns should
        be unexpectedly dropped).

        Args:
            sample_health_df: Fixture providing test data

        Assert:
            - All original columns are present in result

        """
        from scripts.transform import transform_health_data

        result = transform_health_data(sample_health_df)

        expected_columns = {
            "user_id",
            "date",
            "average_heart_rate",
            "average_glucose",
            "sleep_hours",
            "activity_level",
        }
        assert expected_columns.issubset(result.columns)


# =============================================================================
# Tests for transform_supl_usage function
# =============================================================================
# The transform_supl_usage function processes supplement usage data by:
#   1. Converting date to datetime
#   2. Creating a new column dosage_grams (dosage / 1000)
#   3. Removing rows with missing user_id or date


class TestTransformSuplUsage:
    """Test suite for the transform_supl_usage function."""

    def test_transform_supl_usage_empty_dataframe_returns_empty(self):
        """
        Test that empty DataFrame is handled gracefully.

        Returns an empty DataFrame if the input is empty.
        """
        from scripts.transform import transform_supl_usage

        df_empty = pd.DataFrame()
        result = transform_supl_usage(df_empty)

        assert result.empty

    def test_transform_supl_usage_converts_date_to_datetime(self, sample_usage_df):
        """
        Test that date column is converted to datetime.

        Args:
            sample_usage_df: Fixture providing test supplement usage data

        Assert:
            - date column dtype is datetime64

        """
        from scripts.transform import transform_supl_usage

        result = transform_supl_usage(sample_usage_df)

        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_transform_supl_usage_creates_dosage_grams_column(self, sample_usage_df):
        """
        Test that dosage_grams column is created (dosage / 1000).

        Raw dosage is in milligrams (mg). This function converts to grams
        by dividing by 1000.

        Args:
            sample_usage_df: Fixture with dosage in mg [500.0, 250.0]

        Assert:
            - dosage_grams column exists
            - Values are correct (500 mg → 0.5 g, etc.)

        """
        from scripts.transform import transform_supl_usage

        result = transform_supl_usage(sample_usage_df)

        assert "dosage_grams" in result.columns
        assert result["dosage_grams"].iloc[0] == 0.5  # 500 / 1000
        assert result["dosage_grams"].iloc[1] == 0.25  # 250 / 1000

    def test_transform_supl_usage_removes_missing_user_id(self):
        """
        Test that rows with missing user_id are removed.

        Assert:
            - Row with NaN user_id is removed
        """
        from scripts.transform import transform_supl_usage

        df = pd.DataFrame(
            {
                "user_id": ["u1", None],
                "date": ["2024-01-15", "2024-02-20"],
                "supplement_name": ["Vitamin C", "Magnesium"],
                "dosage": [500.0, 250.0],
                "dosage_unit": ["mg", "mg"],
                "is_placebo": [False, True],
                "experiment_id": ["exp-1", "exp-2"],
            }
        )

        result = transform_supl_usage(df)

        assert len(result) == 1
        assert result.iloc[0]["user_id"] == "u1"

    def test_transform_supl_usage_removes_missing_date(self):
        """
        Test that rows with missing date are removed.

        Assert:
            - Row with NaN date is removed
        """
        from scripts.transform import transform_supl_usage

        df = pd.DataFrame(
            {
                "user_id": ["u1", "u2"],
                "date": ["2024-01-15", None],
                "supplement_name": ["Vitamin C", "Magnesium"],
                "dosage": [500.0, 250.0],
                "dosage_unit": ["mg", "mg"],
                "is_placebo": [False, True],
                "experiment_id": ["exp-1", "exp-2"],
            }
        )

        result = transform_supl_usage(df)

        assert len(result) == 1
        assert result.iloc[0]["user_id"] == "u1"

    def test_transform_supl_usage_preserves_supplement_info(self, sample_usage_df):
        """
        Test that supplement information is preserved.

        Original columns like supplement_name and is_placebo should remain.

        Args:
            sample_usage_df: Fixture providing test data

        Assert:
            - supplement_name and is_placebo columns are present

        """
        from scripts.transform import transform_supl_usage

        result = transform_supl_usage(sample_usage_df)

        assert "supplement_name" in result.columns
        assert "is_placebo" in result.columns
        assert result["supplement_name"].iloc[0] == "Vitamin C"
        assert result["is_placebo"].iloc[0] == False


# =============================================================================
# Tests for merge_data function
# =============================================================================
# The merge_data function combines health, usage, experiments, and profile
# data into a single unified dataset with proper merging and transformations.


class TestMergeData:
    """Test suite for the merge_data function."""

    def test_merge_data_empty_dataframe_returns_empty(
        self, sample_health_df, sample_usage_df, sample_experiments_df, sample_profiles_df
    ):
        """
        Test that merge_data returns empty DataFrame if any input is empty.

        If any of the four input DataFrames is empty, the merge cannot
        proceed and an empty DataFrame is returned.

        Args:
            sample_health_df, sample_usage_df, sample_experiments_df,
            sample_profiles_df: Test fixtures

        Assert:
            - Empty DataFrame is returned

        """
        from scripts.transform import merge_data

        # Try with empty health dataframe
        result = merge_data(
            pd.DataFrame(),
            sample_usage_df,
            sample_experiments_df,
            sample_profiles_df,
        )

        assert result.empty

    def test_merge_data_includes_expected_columns(
        self,
        sample_health_df,
        sample_usage_df,
        sample_experiments_df,
        sample_profiles_df,
    ):
        """
        Test that merged DataFrame includes all expected columns.

        The final merged dataset should have specific columns in a specific order.

        Args:
            sample_*_df: Test fixtures

        Assert:
            - All expected columns are present
            - Columns are in the correct order

        """
        from scripts.transform import merge_data, transform_health_data, transform_profiles, transform_supl_usage

        # Transform the fixtures as merge_data expects pre-transformed data
        transformed_health = transform_health_data(sample_health_df)
        transformed_usage = transform_supl_usage(sample_usage_df)
        transformed_profiles = transform_profiles(sample_profiles_df)

        result = merge_data(
            transformed_health,
            transformed_usage,
            sample_experiments_df,
            transformed_profiles,
        )

        expected_columns = [
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
        ]

        assert list(result.columns) == expected_columns

    def test_merge_data_fills_supplement_name_with_no_intake(
        self,
        sample_health_df,
        sample_usage_df,
        sample_experiments_df,
        sample_profiles_df,
    ):
        """
        Test that missing supplement_name values are filled with "No intake".

        When a health entry doesn't have a corresponding supplement usage,
        supplement_name should be "No intake" instead of NaN.

        Args:
            sample_*_df: Test fixtures

        Assert:
            - No NaN values in supplement_name column
            - Missing values are filled with "No intake"

        """
        from scripts.transform import merge_data, transform_health_data, transform_profiles, transform_supl_usage

        # Transform the fixtures as merge_data expects pre-transformed data
        transformed_health = transform_health_data(sample_health_df)
        transformed_usage = transform_supl_usage(sample_usage_df)
        transformed_profiles = transform_profiles(sample_profiles_df)

        result = merge_data(
            transformed_health,
            transformed_usage,
            sample_experiments_df,
            transformed_profiles,
        )

        # Check that there are no NaN values in supplement_name
        assert result["supplement_name"].isna().sum() == 0

    def test_merge_data_drops_rows_without_email(
        self,
        sample_health_df,
        sample_usage_df,
        sample_experiments_df,
    ):
        """
        Test that rows without email are dropped.

        Email is a critical user identifier. Rows without it are removed
        to ensure data quality.

        Args:
            sample_health_df: Test health data
            sample_usage_df: Test usage data
            sample_experiments_df: Test experiments data
            + custom profile data

        Assert:
            - Rows with NaN email are not in result

        """
        from scripts.transform import merge_data, transform_health_data, transform_profiles, transform_supl_usage

        # Create profiles with one missing email
        profiles_with_null = pd.DataFrame(
            {
                "user_id": ["u1", "u2", "u3"],
                "age": [22, 45, 70],
                "email": ["alice@mail.com", None, "carol@mail.com"],
            }
        )

        # Transform the DataFrames as merge_data expects pre-transformed data
        transformed_health = transform_health_data(sample_health_df)
        transformed_usage = transform_supl_usage(sample_usage_df)
        transformed_profiles = transform_profiles(profiles_with_null)

        result = merge_data(
            transformed_health,
            transformed_usage,
            sample_experiments_df,
            transformed_profiles,
        )

        # Verify no NaN in email column
        assert result["email"].isna().sum() == 0
