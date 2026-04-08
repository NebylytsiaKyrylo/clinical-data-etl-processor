import logging
import os

import pandas as pd

from config import DATA_PROCESSED

logger = logging.getLogger(__name__)


def save_csv(df_processed_data: pd.DataFrame) -> None:
    try:
        if df_processed_data.empty:
            logger.warning("Cannot load empty DataFrames")

        os.makedirs(DATA_PROCESSED, exist_ok=True)
        df_processed_data.to_csv(DATA_PROCESSED / "processed_data.csv", index=False)
        logger.info("Data loaded to .csv files successfully")

    except Exception as e:
        logger.error(f"An error occurred while loading data: {e}", exc_info=True)
