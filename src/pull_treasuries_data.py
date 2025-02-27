from pathlib import Path
import pandas as pd
import wrds
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
    """Pull S&P U.S. Treasury Bond Index (^SPBDUSBT) data from WRDS."""
    
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
        # Connect to the WRDS database
        db = wrds.Connection(wrds_username=wrds_username)

        # Execute the SQL query
        treasury_data = db.raw_sql(sql_query, date_cols=["date"])

        # Close the connection
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
    """Load S&P Treasury Bond Index data from a local Excel file if WRDS fails."""
    try:
        treasury_data = pd.read_excel(MANUAL_FILE_PATH, engine="xlrd")
        print(f"✅ Successfully loaded Treasury Bond Index data from {MANUAL_FILE_PATH}")
        return treasury_data
    except Exception as e:
        print(f"⚠️ Failed to load manual Excel file: {e}")
        return None

def save_as_parquet(data, path):
    """Save the given DataFrame as a Parquet file."""
    try:
        data.to_parquet(path, index=False)
        print(f"✅ Data successfully saved as Parquet: {path}")
    except Exception as e:
        print(f"⚠️ Failed to save as Parquet: {e}")

def load_SP_Treasury_Bond_Index(data_dir=DATA_DIR):
    """Load saved S&P Treasury Bond Index data from a Parquet file."""
    path = Path(data_dir) / "pulled" / "SP_Treasury_Bond_Index.parquet"
    return pd.read_parquet(path)

if __name__ == "__main__":
    # Attempt to pull data from WRDS
    treasury_data = pull_SP_Treasury_Bond_Index(wrds_username=WRDS_USERNAME)

    # If WRDS fails, try loading from manual Excel file
    if treasury_data is None:
        print("🔄 Attempting to load Treasury Bond Index data from manual Excel file...")
        treasury_data = load_from_manual_excel()

    # If data is successfully retrieved from any source, save it
    if treasury_data is not None:
        save_as_parquet(treasury_data, OUTPUT_FILE_PATH)
    else:
        print("🚨 Failed to retrieve S&P Treasury Bond Index data from both WRDS and manual Excel file.")
