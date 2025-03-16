"""
iShares MBS ETF (MBB) Data Retrieval Script

This script retrieves historical data for the iShares MBS ETF (MBB) using multiple data sources. 
It provides flexibility to either pull data directly from Yahoo Finance or load it from a 
locally stored CSV file.

The script performs the following tasks:
- Pulls MBB data from Yahoo Finance (if enabled via configuration).
- If Yahoo Finance is disabled or unavailable, it loads data from a manual CSV file.
- Ensures the retrieved data includes a proper date column.
- Saves the retrieved data as a **Parquet** file for efficient storage and retrieval.
- Provides functions to load stored Parquet files into pandas DataFrames for analysis.

Data Sources:
- **Yahoo Finance API** (requires internet access)
- **Manually maintained CSV file** (fallback option)

Usage:
- Run the script directly to fetch and save the data.
- Use the provided functions to load stored Parquet files.

Dependencies:
- pandas
- pathlib
- yfinance
- settings (for managing configuration paths and credentials)

Configuration:
- `USE_YFINANCE`: Boolean flag that determines whether to use Yahoo Finance or a manual file.

Author: Aadi Deshpande
Date: 16th March 2025
"""

from pathlib import Path
import pandas as pd
import yfinance as yf
from settings import config

# Load configuration settings
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
DATA_DIR = Path(config("DATA_DIR"))
USE_YFINANCE = config("USE_YFINANCE")  # Boolean config variable

# Define manual file path
MANUAL_FILE_PATH = DATA_DIR / "manual" / "ishares_mbs_etf_daily.csv"
OUTPUT_FILE_PATH = DATA_DIR / "pulled" / "MBB_data.parquet"

# Define ticker and date range
TICKER = "MBB"
START_DATE = "2000-01-01"
END_DATE = "2025-02-26"

def pull_MBB_data():
    """
    Pulls historical iShares MBS ETF (MBB) data from Yahoo Finance.

    Returns:
        pd.DataFrame: DataFrame containing:
            - Date: Date of the recorded data.
            - Open, High, Low, Close, Adj Close: Pricing data.
            - Volume: Trading volume.
        Returns None if data retrieval fails.
    """
    try:
        mbb_data = yf.download(TICKER, start=START_DATE, end=END_DATE, progress=False)

        if mbb_data.empty:
            print("⚠️ No data found for MBB from Yahoo Finance.")
            return None

        # Ensure the Date column is included
        mbb_data.reset_index(inplace=True)  # Moves the index (date) to a separate column
        print("✅ Successfully pulled MBB data from Yahoo Finance.")
        return mbb_data

    except Exception as e:
        print(f"⚠️ Yahoo Finance data pull failed: {e}")
        return None

def load_from_manual_csv():
    """
    Loads historical iShares MBS ETF (MBB) data from a local CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the data from the manual CSV file.
        Returns None if the file is not found or cannot be loaded.
    """
    try:
        mbb_data = pd.read_csv(MANUAL_FILE_PATH)
        print(f"✅ Successfully loaded MBB data from {MANUAL_FILE_PATH}")
        return mbb_data
    except Exception as e:
        print(f"⚠️ Failed to load manual CSV file: {e}")
        return None

def save_as_parquet(data, path):
    """
    Saves the given DataFrame as a Parquet file.

    Args:
        data (pd.DataFrame): DataFrame to be saved.
        path (Path): Path to save the Parquet file.

    Returns:
        None
    """
    try:
        data.to_parquet(path, index=False)
        print(f"✅ Data successfully saved as Parquet: {path}")
    except Exception as e:
        print(f"⚠️ Failed to save as Parquet: {e}")

def load_MBB_data(data_dir=DATA_DIR):
    """
    Loads saved MBB data from a Parquet file.

    Args:
        data_dir (Path): Directory where the data is stored.

    Returns:
        pd.DataFrame: DataFrame loaded from the Parquet file.
    """
    path = Path(data_dir) / "pulled" / "MBB_data.parquet"
    return pd.read_parquet(path)

if __name__ == "__main__":
    # Determine whether to use Yahoo Finance or the manual file based on config
    if USE_YFINANCE:
        print("🔄 Using Yahoo Finance to fetch MBB data...")
        mbb_data = pull_MBB_data()
    else:
        print("🔄 Using manual file from /data/manual to fetch MBB data...")
        mbb_data = load_from_manual_csv()

    # If data is successfully retrieved, save it
    if mbb_data is not None:
        save_as_parquet(mbb_data, OUTPUT_FILE_PATH)
    else:
        print("🚨 Failed to retrieve MBB data from both Yahoo Finance and manual file.")
