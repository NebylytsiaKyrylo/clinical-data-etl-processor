import logging

import pandas as pd

from config import DATA_RAW_DIR

logger = logging.getLogger(__name__)


def extract_csv(file_name: str) -> pd.DataFrame:
    """
    Extracts a CSV file and loads its content into a pandas DataFrame.

    This function reads a CSV file from the raw data directory and loads its content
    into a DataFrame. If the file is not found or an error occurs during loading, it
    returns an empty DataFrame. Additionally, the function logs success or error
    messages during the operation.

    Args:
        file_name (str): The name of the CSV file to be extracted.

    Returns:
        pd.DataFrame: A DataFrame containing the content of the CSV file. If the file
        cannot be accessed or an error occurs, an empty DataFrame is returned.
    """

    file_path = DATA_RAW_DIR / file_name

    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded {file_name} ({len(df)} rows)")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading {file_name}: {e!s}")
        return pd.DataFrame()
