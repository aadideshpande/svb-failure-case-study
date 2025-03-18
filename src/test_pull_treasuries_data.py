from pathlib import Path
import pandas as pd
import wrds
from pandas.tseries.offsets import MonthEnd
import data_preprocessing
import datetime
from settings import config

# Load configuration settings
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

from pull_treasuries_data import (
    load_from_manual_excel
)

def test_load_from_manual_excel():
    df = load_from_manual_excel()
    
    # Ensure df is a pandas DataFrame
    assert isinstance(df, pd.DataFrame), "Returned object is not a DataFrame"
    
    # Check if the shape matches the expected structure
    expected_rows = 2527  # Replace with the actual expected number of rows
    expected_columns = 2  # Replace with the actual expected number of columns
    
    assert df.shape[0] > 0, "DataFrame has no rows"
    assert df.shape[1] == expected_columns, f"Expected {expected_columns} columns, but got {df.shape[1]}"
