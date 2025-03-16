"""
S&P U.S. Treasury Bond Index Data Retrieval Script

This script retrieves historical data for the S&P U.S. Treasury Bond Index using multiple 
data sources, ensuring redundancy if one source fails. The sources include:

1. **WRDS (Wharton Research Data Services)** - The primary source of the index data.
2. **Investpy (Investing.com data)** - A backup source if WRDS is unavailable.
3. **Yahoo Finance (yfinance)** - Another backup source if both WRDS and investpy fail.
4. **Manual Excel File** - A last-resort option if all external data sources fail.

The script performs the following tasks:
- Attempts to pull data from WRDS.
- If WRDS fails, it tries to retrieve data from Investpy.
- If Investpy fails, it attempts to pull data from Yahoo Finance.
- If all fail, it loads data from a manually maintained Excel file.
- Saves the retrieved data as a **Parquet** file for efficient storage and retrieval.
- Provides functions to load stored Parquet files into pandas DataFrames for analysis.

Data Sources:
- **WRDS Database** (requires access credentials)
- **Investpy (Investing.com API)**
- **Yahoo Finance API**
- **Manually maintained Excel file** (if all external sources fail)

Usage:
- Run the script directly to fetch and save the data.
- Use the provided functions to load stored Parquet files.

Dependencies:
- pandas
- pathlib
- wrds
- yfinance
- investpy
- settings (for managing configuration paths and credentials)

Author: Aadi Deshpande
Date: 16th March 2025
"""

from pathlib import Path
import pandas as pd
import wrds
import yfinance as yf
import investpy
from settings import config

# Load configuration settings
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
DATA_DIR = Path(config("DATA_DIR"))

# Define manual file path
MANUAL_FILE_PATH = DATA_DIR / "manual" / "s_&_p_treasury_bond_index.xls"
OUTPUT_FILE_PATH = DATA_DIR / "pulled" / "SP_Treasury_Bond_Index.parquet"

# Define WRDS credentials
WRDS_USERNAME = config("WRDS_USERNAME")

def pull_SP_Treasury_Bond_Index(wrds_username=WRDS_USERNAME):
    """
    Pulls S&P U.S. Treasury Bond Index (^SPBDUSBT) data from WRDS.

    Args:
        wrds_username (str): WRDS username credential.

    Returns:
        pd.DataFrame: DataFrame containing:
            - date: Date of the index.
            - index_value: The value of the index.
            - return: Daily returns.
            - yield_to_worst: Yield to worst values.
        Returns None if data retrieval fails.
    """
    sql_query = """
        SELECT 
            date, index_value, return, yield_to_worst
        FROM 
            spd.udb  -- Adjust based on actual WRDS schema
        WHERE 
            index_code = 'SPBDUSBT' 
            AND date BETWEEN '2021-12-31' AND '2025-02-26'
    """

    try:
        db = wrds.Connection(wrds_username=wrds_username)
        treasury_data = db.raw_sql(sql_query, date_cols=["date"])
        db.close()

        if treasury_data.empty:
            print("⚠️ No data found for S&P Treasury Bond Index in WRDS.")
            return None

        print("✅ Successfully pulled S&P Treasury Bond Index data from WRDS.")
        return treasury_data

    except Exception as e:
        print(f"⚠️ WRDS data pull failed: {e}")
        return None

def load_from_manual_excel():
    """
    Loads S&P Treasury Bond Index data from a local Excel file.

    Returns:
        pd.DataFrame: DataFrame containing the data from the manual Excel file.
        Returns None if the file is not found or cannot be loaded.
    """
    try:
        treasury_data = pd.read_excel(MANUAL_FILE_PATH, engine="xlrd")
        print(f"✅ Successfully loaded Treasury Bond Index data from {MANUAL_FILE_PATH}")
        return treasury_data
    except Exception as e:
        print(f"⚠️ Failed to load manual Excel file: {e}")
        return None

def pull_SP_Treasury_Bond_Index_yahoo(start_date="2022-01-01", end_date=None):
    """
    Pulls S&P U.S. Treasury Bond Index (^SPBDUSBT) data from Yahoo Finance.

    Args:
        start_date (str): Start date for data retrieval in 'YYYY-MM-DD' format.
        end_date (str, optional): End date for data retrieval in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: DataFrame containing:
            - date: Date of the index.
            - index_value: The value of the index.
            - return: Daily returns.
            - yield_to_worst: Placeholder (set to NaN).
        Returns None if data retrieval fails.
    """
    if end_date is None:
        end_date = pd.Timestamp.today().strftime('%Y-%m-%d')

    try:
        df = yf.download("^SPBDUSBT", start=start_date, end=end_date)
        if df.empty:
            print("⚠️ No data found for S&P Treasury Bond Index from Yahoo Finance.")
            return None

        df.reset_index(inplace=True)
        df.rename(columns={"Adj Close": "index_value"}, inplace=True)
        df["return"] = df["index_value"].pct_change()
        df["yield_to_worst"] = pd.NA
        treasury_data = df[["Date", "index_value", "return", "yield_to_worst"]].copy()
        treasury_data.rename(columns={"Date": "date"}, inplace=True)
        print("✅ Successfully pulled S&P Treasury Bond Index data from Yahoo Finance.")
        return treasury_data
    except Exception as e:
        print(f"⚠️ Yahoo Finance data pull failed: {e}")
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

def load_SP_Treasury_Bond_Index(data_dir=DATA_DIR):
    """
    Loads the saved S&P Treasury Bond Index data from a Parquet file.

    Args:
        data_dir (Path): Directory where the data is stored.

    Returns:
        pd.DataFrame: DataFrame loaded from the Parquet file.
    """
    path = Path(data_dir) / "pulled" / "SP_Treasury_Bond_Index.parquet"
    return pd.read_parquet(path)

if __name__ == "__main__":
    # Attempt to pull data from WRDS
    treasury_data = pull_SP_Treasury_Bond_Index(wrds_username=WRDS_USERNAME)

    # If WRDS fails, try pulling data from Investpy
    if treasury_data is None:
        print("🔄 Attempting to pull Treasury Bond Index data from Investpy...")
        treasury_data = pull_SP_Treasury_Bond_Index_yahoo()

    # If Yahoo Finance fails, try loading from a manual Excel file
    if treasury_data is None:
        print("🔄 Attempting to load Treasury Bond Index data from manual Excel file...")
        treasury_data = load_from_manual_excel()

    # If data is successfully retrieved from any source, save it as a Parquet file
    if treasury_data is not None:
        save_as_parquet(treasury_data, OUTPUT_FILE_PATH)
    else:
        print("🚨 Failed to retrieve S&P Treasury Bond Index data from all sources.")
