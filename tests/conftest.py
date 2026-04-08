"""
conftest.py — Global pytest configuration and shared fixtures.

This file is automatically loaded by pytest before running tests.
It's used to:
  - Configure Python's import path (sys.path)
  - Define shared fixtures available to all test files

Fixtures defined here are available in ALL test files without importing—
pytest automatically injects them when a test function has a parameter
with the same name as a fixture.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Add the project root directory to sys.path so imports like
# `from scripts.transform import ...` work correctly.
sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Shared Test Fixtures — Reusable Test Data
# ---------------------------------------------------------------------------
# A pytest fixture is a function decorated with @pytest.fixture.
# It provides reusable data or objects to multiple tests.
# Pytest automatically detects when a test needs a fixture by matching
# the parameter name to a fixture name.
#
# Example usage in a test:
#
#   def test_transform(sample_profiles_df):
#       result = transform_profiles(sample_profiles_df)
#       assert ...
#
# For more info: https://docs.pytest.org/en/stable/how-to/fixtures.html
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_profiles_df() -> pd.DataFrame:
    """
    Return a DataFrame with valid user profile data.

    Used in tests for transform_profiles and merge_data functions.

    Returns:
        pd.DataFrame: DataFrame with columns user_id, age, email.

    """
    return pd.DataFrame(
        {
            "user_id": ["u1", "u2", "u3"],
            "age": [22, 45, 70],
            "email": ["alice@mail.com", "bob@mail.com", "carol@mail.com"],
        }
    )


@pytest.fixture
def sample_health_df() -> pd.DataFrame:
    """
    Return a DataFrame with valid health data.

    Note: sleep_hours intentionally contains the 'h' suffix (e.g., "7.5h")
    as in real raw data — transform_health_data is responsible for cleaning it.

    Returns:
        pd.DataFrame: DataFrame with columns user_id, date, heart rate,
                     glucose, sleep_hours, activity_level.

    """
    return pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "date": ["2024-01-15", "2024-02-20"],
            "average_heart_rate": [72.5, 80.0],
            "average_glucose": [95.0, 110.0],
            "sleep_hours": ["7.5h", "6.0H"],  # Mixed case intentional
            "activity_level": [50, 120],  # 120 exceeds max of 100
        }
    )


@pytest.fixture
def sample_usage_df() -> pd.DataFrame:
    """
    Return a DataFrame with valid supplement usage data.

    dosage is in mg — transform_supl_usage creates dosage_grams (dosage / 1000).

    Returns:
        pd.DataFrame: DataFrame with supplement usage information.

    """
    return pd.DataFrame(
        {
            "user_id": ["u1", "u2"],
            "date": ["2024-01-15", "2024-02-20"],
            "supplement_name": ["Vitamin C", "Magnesium"],
            "dosage": [500.0, 250.0],
            "dosage_unit": ["mg", "mg"],
            "is_placebo": [False, True],
            "experiment_id": ["exp-1", "exp-2"],
        }
    )


@pytest.fixture
def sample_experiments_df() -> pd.DataFrame:
    """
    Return a DataFrame with valid experiment data.

    Returns:
        pd.DataFrame: DataFrame with experiment_id, name, and description.

    """
    return pd.DataFrame(
        {
            "experiment_id": ["exp-1", "exp-2"],
            "name": ["Endurance", "Strength"],
            "description": ["Desc 1", "Desc 2"],
        }
    )
