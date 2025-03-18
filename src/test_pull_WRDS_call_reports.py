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

from pull_WRDS_call_reports import (
    pull_RCON_series_1,
    pull_RCON_series_2,
    pull_RCFD_series_1,
    pull_RCFD_series_2,
    pull_BHCK1975,
    load_wrds_call_research,
)

def test_load_RCON_series_1():
    df = pull_RCON_series_1(WRDS_USERNAME)
    
    # Ensure df is a pandas DataFrame
    assert isinstance(df, pd.DataFrame), "Returned object is not a DataFrame"
    
    # Check if the shape matches the expected structure
    expected_rows = 28828  # Replace with the actual expected number of rows
    expected_columns = 6  # Replace with the actual expected number of columns
    
    assert df.shape[0] > 0, "DataFrame has no rows"
    assert df.shape[1] == expected_columns, f"Expected {expected_columns} columns, but got {df.shape[1]}"

def test_load_wrds_call_research():
    df = load_wrds_call_research()
    
    # Ensure df is a pandas DataFrame
    assert isinstance(df, pd.DataFrame), "Returned object is not a DataFrame"
    
    # Check if the shape matches the expected structure
    expected_rows = 24565  # Replace with the actual expected number of rows
    expected_columns = 371  # Replace with the actual expected number of columns
    
    assert df.shape[0] > 0, "DataFrame has no rows"
    assert df.shape[1] == expected_columns, f"Expected {expected_columns} columns, but got {df.shape[1]}"

def test_load_RCON_series_2():
    df = pull_RCON_series_2(WRDS_USERNAME)
    
    # Ensure df is a pandas DataFrame
    assert isinstance(df, pd.DataFrame), "Returned object is not a DataFrame"
    
    # Check if the shape matches the expected structure
    expected_rows = 28828  # Replace with the actual expected number of rows
    expected_columns = 4  # Replace with the actual expected number of columns
    
    assert df.shape[0] > 0, "DataFrame has no rows"
    assert df.shape[1] == expected_columns, f"Expected {expected_columns} columns, but got {df.shape[1]}"

def test_load_RCFD_series_1():
    df = pull_RCFD_series_1(WRDS_USERNAME)
    
    # Ensure df is a pandas DataFrame
    assert isinstance(df, pd.DataFrame), "Returned object is not a DataFrame"
    
    # Check if the shape matches the expected structure
    expected_rows = 28828  # Replace with the actual expected number of rows
    expected_columns = 13  # Replace with the actual expected number of columns
    
    assert df.shape[0] > 0, "DataFrame has no rows"
    assert df.shape[1] == expected_columns, f"Expected {expected_columns} columns, but got {df.shape[1]}"

def test_load_RCFD_series_2():
    df = pull_RCFD_series_2(WRDS_USERNAME)
    
    # Ensure df is a pandas DataFrame
    assert isinstance(df, pd.DataFrame), "Returned object is not a DataFrame"
    
    # Check if the shape matches the expected structure
    expected_rows = 28828  # Replace with the actual expected number of rows
    expected_columns = 7  # Replace with the actual expected number of columns
    
    assert df.shape[0] > 0, "DataFrame has no rows"
    assert df.shape[1] == expected_columns, f"Expected {expected_columns} columns, but got {df.shape[1]}"