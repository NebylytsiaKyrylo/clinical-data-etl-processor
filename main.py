import logging

from scripts.extract import extract_csv
from scripts.load import save_csv
from scripts.transform import merge_data, transform_health_data, transform_profiles, transform_supl_usage

# Initialize logging
logging.basicConfig(format="%(process)d-%(levelname)s-%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def merge_all_data(health_csv: str, usage_csv: str, experiments_csv: str, profiles_csv: str) -> None:
    """
    Merges and processes data.

    Merges and processes data from multiple CSV sources, performing extraction,
    transformation, merging, and saving of the final dataset. This function is
    intended for end-to-end data processing workflows involving health, usage,
    experiment, and profile data.

    Args:
        health_csv (str): Path to the CSV file containing raw health data.
        usage_csv (str): Path to the CSV file containing raw usage data.
        experiments_csv (str): Path to the CSV file containing raw experiment data.
        profiles_csv (str): Path to the CSV file containing raw profile data.
    """

    # 1. Extract
    logger.info("Loading data...")
    df_health_raw = extract_csv(health_csv)
    df_usage_raw = extract_csv(usage_csv)
    df_experiments_raw = extract_csv(experiments_csv)
    df_profiles_raw = extract_csv(profiles_csv)

    # 2. Transform
    logger.info("Transforming data...")
    df_profiles = transform_profiles(df_profiles_raw)
    df_health = transform_health_data(df_health_raw)
    df_usage = transform_supl_usage(df_usage_raw)

    # 3. Merge
    logger.info("Merging data...")
    merged_df = merge_data(df_health, df_usage, df_experiments_raw, df_profiles)
    logger.info(f"Merged DataFrame shape: {merged_df.shape}")

    # 4. Load
    logger.info("Loading data...")
    save_csv(merged_df)


if __name__ == "__main__":
    merge_all_data("user_health_data.csv", "supplement_usage.csv", "experiments.csv", "user_profiles.csv")
