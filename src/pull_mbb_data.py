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
    """Pull iShares MBS ETF (MBB) data from Yahoo Finance and ensure date is included."""
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
    """Load iShares MBS ETF data from a local CSV file if Yahoo Finance is disabled."""
    try:
        mbb_data = pd.read_csv(MANUAL_FILE_PATH)
        print(f"✅ Successfully loaded MBB data from {MANUAL_FILE_PATH}")
        return mbb_data
    except Exception as e:
        print(f"⚠️ Failed to load manual CSV file: {e}")
        return None


def save_as_parquet(data, path):
    """Save the given DataFrame as a Parquet file."""
    try:
        data.to_parquet(path, index=False)
        print(f"✅ Data successfully saved as Parquet: {path}")
    except Exception as e:
        print(f"⚠️ Failed to save as Parquet: {e}")

def load_MBB_data(data_dir=DATA_DIR):
    """Load saved MBB data from a Parquet file."""
    path = Path(data_dir) / "pulled" / "MBB_data.parquet"
    return pd.read_parquet(path)

if __name__ == "__main__":
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
