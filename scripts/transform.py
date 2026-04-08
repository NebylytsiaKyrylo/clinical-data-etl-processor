import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def transform_profiles(df_profiles: pd.DataFrame) -> pd.DataFrame:
    """
    Transform user profiles by adding an age group column and filtering out rows.

    This function takes a DataFrame containing user profiles and assigns users to age
    groups based on predefined bins. Rows with missing `user_id` or `email` values are
    dropped. An additional column `user_age_group` is added to the DataFrame to represent
    the user's age group.

    Args:
        df_profiles (pd.DataFrame): The input DataFrame containing user profiles. The
            DataFrame must have columns `age`, `user_id`, and `email`.

    Returns:
        pd.DataFrame: A transformed DataFrame with an added `user_age_group` column, where
            users are categorized into predefined age groups. Rows with missing `user_id`
            or `email` values are removed.

    Raises:
        None

    """

    if df_profiles.empty:
        logger.warning("DataFrame profiles is empty")
        return pd.DataFrame()

    bins = [0, 18, 26, 36, 46, 56, 66, np.inf]
    labels = ["Under 18", "18-25", "26-35", "36-45", "46-55", "56-65", "Over 65"]
    df_profiles["user_age_group"] = pd.cut(
        df_profiles["age"],
        bins=bins,
        labels=labels,
        right=False,
    )
    df_profiles["user_age_group"] = df_profiles["user_age_group"].astype(str).replace("nan", "Unknown")

    return df_profiles.dropna(subset=["user_id", "email"])


def transform_health_data(df_health: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the health df by operations : type conversion, value clipping, and removal of invalid rows.

    The function processes the given health data by converting the date column to a datetime
    object, transforming the sleep hours string into a float, and clipping the values of the
    activity level column within specified bounds. Rows with missing user_id or date data are
    removed.

    Args:
        df_health (pd.DataFrame): A pandas DataFrame containing the health data to be
            transformed. The DataFrame is expected to have columns like 'date', 'sleep_hours',
            'activity_level', and 'user_id'.

    Returns:
        pd.DataFrame: A new DataFrame containing the transformed health data. If the input
        DataFrame is empty, an empty DataFrame is returned instead.

    """

    if df_health.empty:
        logger.warning("DataFrame health is empty")
        return pd.DataFrame()

    df_health["date"] = pd.to_datetime(df_health["date"])
    df_health["sleep_hours"] = df_health["sleep_hours"].str.replace("h", "", case=False, regex=False).astype(float)
    df_health["activity_level"] = df_health["activity_level"].clip(lower=0, upper=100)

    return df_health.dropna(subset=["user_id", "date"])


def transform_supl_usage(df_usage: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and processes a DataFrame containing supplement usage data.

    This function checks if the DataFrame is empty, converts the 'date' column to a datetime object, creates a new
    column 'dosage_grams' by converting 'dosage' into grams, and removes rows with null values
    in 'user_id' or 'date' columns.

    Args:
        df_usage (pd.DataFrame): A DataFrame representing supplement usage data. It must contain
            columns 'date', 'dosage', and 'user_id'.

    Returns:
        pd.DataFrame: A processed DataFrame with transformed data. If the input DataFrame is empty,
        it returns an empty DataFrame.

    """

    if df_usage.empty:
        logger.warning("DataFrame usage is empty")
        return pd.DataFrame()

    df_usage["date"] = pd.to_datetime(df_usage["date"])
    df_usage["dosage_grams"] = df_usage["dosage"] / 1000

    return df_usage.dropna(subset=["user_id", "date"])


def merge_data(
    df_health: pd.DataFrame,
    df_usage: pd.DataFrame,
    df_experiments: pd.DataFrame,
    df_profiles: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge multiple dataframes into a single dataset while ensuring proper handling of missing or invalid data.

    Args:
        df_health (pd.DataFrame): A dataframe containing user health metrics such as heart rate,
            glucose levels, and other related data.
        df_usage (pd.DataFrame): A dataframe containing user activity data, such as dosage,
            usage habits, and associated experiment IDs.
        df_experiments (pd.DataFrame): A dataframe containing information about experiments,
            such as experiment IDs and names.
        df_profiles (pd.DataFrame): A dataframe containing user profile information, such as
            email, age group, and other identifying details.

    Returns:
        pd.DataFrame: A processed and merged dataframe containing selected columns from all input
        dataframes. Columns include user profile details, health metrics, activity data, and
        experiment details. Missing or invalid data is appropriately managed.

    """

    if df_profiles.empty or df_health.empty or df_usage.empty or df_experiments.empty:
        logger.warning("One of the dataframes is empty")
        return pd.DataFrame()

    df_usage_exp = df_usage.merge(
        df_experiments[["experiment_id", "name"]],
        on="experiment_id",
        how="left",
    ).rename(columns={"name": "experiment_name"})

    df_merged = df_usage_exp.merge(df_health, on=["user_id", "date"], how="outer")
    df_final = df_merged.merge(df_profiles, on="user_id", how="left")
    df_final["supplement_name"] = df_final["supplement_name"].fillna("No intake")
    df_final = df_final.dropna(subset=["email"])

    final_columns = [
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

    return df_final[final_columns]
