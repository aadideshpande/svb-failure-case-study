"""
WRDS Bank Data Extraction and Processing Script

This script retrieves various financial data series from the WRDS (Wharton Research Data Services) 
database. It pulls RCON and RCFD series data, along with the BHCK1975 series, using SQL queries 
to extract relevant financial metrics such as deposits, securities, loans, and total assets.

The script performs the following:
- Pulls RCON series data (insured/uninsured deposits, total deposits).
- Pulls RCFD series data (treasury securities, mortgage-backed securities, loans).
- Pulls BHCK1975 series data (total deposits).
- Saves the extracted data as Parquet files for efficient storage and retrieval.
- Provides functions to load saved Parquet files into pandas DataFrames for analysis.

Data Sources:
- WRDS Call Reports (RCON, RCFD, BHCK series)
- SQL queries executed through the WRDS database connection

Usage:
- Run the script directly to fetch and save the data.
- Use the provided functions to load stored Parquet files.

Dependencies:
- pandas
- pathlib
- wrds
- settings (for managing configuration paths and credentials)

Author: Aadi Deshpande
Date: 16th March 2025
"""

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

# Function definitions...


def pull_RCON_series_1(wrds_username=WRDS_USERNAME):
    """
    Pull RCON Series 1 data from WRDS for the period between '2021-12-31' and '2023-03-31'.

    This function retrieves bank data including various deposit types from the
    'bank.wrds_call_rcon_1' table.

    Args:
        wrds_username (str): WRDS username credential.

    Returns:
        pd.DataFrame: DataFrame containing columns:
            - rssd9001: Bank identifier.
            - rssd9017: Bank type code.
            - rssd9999: Reporting date.
            - uninsured_deposits: Uninsured deposits (rcon5597).
            - insured_deposit_1: Insured deposit 1 (rconf049).
            - insured_deposit_2: Insured deposit 2 (rconf045).
    """
    print("Pulling RCON Series 1")
    sql_query = """
    SELECT 
        b.rssd9001,                     
        b.rssd9017,                     
        b.rssd9999,                     

        -- Uninsured Deposits
        b.rcon5597 AS uninsured_deposits,
        b.rconf049 AS insured_deposit_1,
        b.rconf045 AS insured_deposit_2

    FROM 
        bank.wrds_call_rcon_1 AS b

    WHERE 
        b.rssd9999 BETWEEN '2021-12-31' AND '2023-03-31'
    """
    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=wrds_username)
    
    # Execute the SQL query and parse the date column
    rcon_series_1 = db.raw_sql(sql_query, date_cols=["rssd9999"])

    print("Pulled RCON Series 1")

    # Close the connection
    db.close()

    return rcon_series_1

def pull_RCON_series_2(wrds_username=WRDS_USERNAME):
    """
    Pull RCON Series 2 data from WRDS for the period between '2021-12-31' and '2023-03-31'.

    This function retrieves bank data including total deposits from the
    'bank.wrds_call_rcon_2' table.

    Args:
        wrds_username (str): WRDS username credential.

    Returns:
        pd.DataFrame: DataFrame containing columns:
            - rssd9001: Bank identifier.
            - rssd9017: Bank type code.
            - rssd9999: Reporting date.
            - total_deposits: Total deposits (rcon2200).
    """
    sql_query = """
    SELECT 
        b.rssd9001,                     
        b.rssd9017,                     
        b.rssd9999,                     

        -- Total Deposits
        b.rcon2200 AS total_deposits  

    FROM 
        bank.wrds_call_rcon_2 AS b

    WHERE 
        b.rssd9999 BETWEEN '2021-12-31' AND '2023-03-31'
    """
    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=wrds_username)
    
    # Execute the SQL query and parse the date column
    rcon_series_2 = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return rcon_series_2

def pull_RCFD_series_1(wrds_username=WRDS_USERNAME):
    """
    Pull RCFD Series 1 data from WRDS for the period between '2021-12-31' and '2023-03-31'.

    This function retrieves data on various securities and loan types from the
    'bank.wrds_call_rcfd_1' table.

    Args:
        wrds_username (str): WRDS username credential.

    Returns:
        pd.DataFrame: DataFrame containing columns:
            - rssd9001: Bank identifier.
            - rssd9017: Bank type code.
            - rssd9999: Reporting date.
            - htm_treasury: Held-to-Maturity Treasury Securities (rcfd1702).
            - afs_treasury: Available-for-Sale Treasury Securities (rcfd1705).
            - htm_mbs: Held-to-Maturity Mortgage-Backed Securities (rcfd1716).
            - htm_other_securities: Other Held-to-Maturity Securities (rcfd1773).
            - res_mortgage_loans: Residential Mortgage Loans (rcfd5367).
            - cre_loans: Commercial Real Estate Loans (rcfd5369).
            - ci_loans_part1: Commercial & Industrial Loans Part 1 (rcfd1763).
            - ci_loans_part2: Commercial & Industrial Loans Part 2 (rcfd1764).
            - ci_loans_total: Total Commercial & Industrial Loans (sum of part1 and part2).
            - consumer_loans: Consumer Loans (rcfdb528).
    """
    sql_query = """
    SELECT 
        b.rssd9001,                        -- Bank identifier
        b.rssd9017,                        -- Bank type code
        b.rssd9999,                        -- Reporting Date

        -- Treasury Securities (HTM & AFS)
        b.rcfd1702 AS htm_treasury,         -- Held-to-Maturity Treasury Securities
        b.rcfd1705 AS afs_treasury,         -- Available-for-Sale Treasury Securities

        -- Mortgage-Backed Securities (HTM & AFS)
        b.rcfd1716 AS htm_mbs,              -- Held-to-Maturity Mortgage-Backed Securities

        -- Other Securities & Loans
        b.rcfd1773 AS htm_other_securities, -- Other Held-to-Maturity Securities
        b.rcfd5367 AS res_mortgage_loans,   -- Residential Mortgage Loans
        b.rcfd5369 AS cre_loans,            -- Commercial Real Estate Loans

        -- Commercial & Industrial Loans (C&I)
        b.rcfd1763 AS ci_loans_part1,       -- C&I Loans Part 1
        b.rcfd1764 AS ci_loans_part2,       -- C&I Loans Part 2
        b.rcfd1763 + b.rcfd1764  AS ci_loans_total,       -- C&I Loans Total

        -- Consumer Loans
        b.rcfdb528 AS consumer_loans        -- Consumer Loans (Auto, Credit Cards, etc.)
    FROM 
        bank.wrds_call_rcfd_1 AS b

    WHERE 
        b.rssd9999 BETWEEN '2021-12-31' AND '2023-03-31'
    """
    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=wrds_username)
    
    # Execute the SQL query and parse the date column
    rcfd_series_1 = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return rcfd_series_1

def pull_RCFD_series_2(wrds_username=WRDS_USERNAME):
    """
    Pull RCFD Series 2 data from WRDS for the period between '2021-12-31' and '2023-03-31'.

    This function retrieves data on available-for-sale securities, loans, and assets from the
    'bank.wrds_call_rcfd_2' table.

    Args:
        wrds_username (str): WRDS username credential.

    Returns:
        pd.DataFrame: DataFrame containing columns:
            - rssd9001: Bank identifier.
            - rssd9017: Bank type code.
            - rssd9999: Reporting date.
            - afs_mbs: Available-for-Sale Mortgage-Backed Securities (rcfd1731).
            - afs_other_securities: Available-for-Sale Other Securities (rcfd1772).
            - total_loans: Total Loans and Leases (rcfd2122).
            - total_assets: Total Bank Assets (rcfd2170).
    """
    sql_query = """
    SELECT 
        b.rssd9001,                     
        b.rssd9017,                     
        b.rssd9999,   

        b.rcfd1731 AS afs_mbs,              
        -- Available-for-Sale Mortgage-Backed Securities                  

        -- Other Securities
        b.rcfd1772 AS afs_other_securities, -- Available-for-Sale Other Securities

        -- Loans & Total Assets
        b.rcfd2122 AS total_loans,          -- Total Loans and Leases
        b.rcfd2170 AS total_assets          -- Total Bank Assets
    FROM 
        bank.wrds_call_rcfd_2 AS b

    WHERE 
        b.rssd9999 BETWEEN '2021-12-31' AND '2023-03-31'
    """
    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=wrds_username)
    
    # Execute the SQL query and parse the date column
    rcfd_series_2 = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return rcfd_series_2

def pull_BHCK1975(wrds_username=WRDS_USERNAME):
    """
    Pull BHCK1975 series data from WRDS Call Reports for the period between '2021-12-31' and '2023-03-31'.

    This function retrieves total deposits from the 'bank_all.wrds_holding_bhck_1' table.

    Args:
        wrds_username (str): WRDS username credential.

    Returns:
        pd.DataFrame: DataFrame containing columns:
            - rssd9001: Bank RSSD ID.
            - rssd9017: Institution Name.
            - rssd9999: Report Date.
            - bhck1975: Total Deposits.
    """
    sql_query = """
    SELECT 
        b.rssd9001,  -- Bank RSSD ID
        b.rssd9017,  -- Institution Name
        b.rssd9999,  -- Report Date
        b.bhck1975   -- Total Deposits
    FROM 
        bank_all.wrds_holding_bhck_1 AS b -- Replace with the correct WRDS table
    WHERE 
        b.rssd9999 BETWEEN '2021-12-31' AND '2023-03-31'
    """
    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=wrds_username)
    
    # Execute the SQL query and parse the date column
    bhck1975_data = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return bhck1975_data

def load_BHCK1975(data_dir=DATA_DIR):
    """
    Load BHCK1975 data from a local Parquet file.

    Args:
        data_dir (Path): Directory path where the data is stored.

    Returns:
        pd.DataFrame: DataFrame loaded from the 'BHCK1975.parquet' file.
    """
    path = Path(data_dir) / "pulled" / "BHCK1975.parquet"
    return pd.read_parquet(path)

def load_RCON_series_1(data_dir=DATA_DIR):
    """
    Load RCON Series 1 data from a local Parquet file.

    Args:
        data_dir (Path): Directory path where the data is stored.

    Returns:
        pd.DataFrame: DataFrame loaded from the 'RCON_Series_1.parquet' file.
    """
    path = Path(data_dir) / "pulled" / "RCON_Series_1.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_RCON_series_2(data_dir=DATA_DIR):
    """
    Load RCON Series 2 data from a local Parquet file.

    Args:
        data_dir (Path): Directory path where the data is stored.

    Returns:
        pd.DataFrame: DataFrame loaded from the 'RCON_Series_2.parquet' file.
    """
    path = Path(data_dir) / "pulled" / "RCON_Series_2.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_RCFD_series_1(data_dir=DATA_DIR):
    """
    Load RCFD Series 1 data from a local Parquet file.

    Args:
        data_dir (Path): Directory path where the data is stored.

    Returns:
        pd.DataFrame: DataFrame loaded from the 'RCFD_Series_1.parquet' file.
    """
    path = Path(data_dir) / "pulled" / "RCFD_Series_1.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_RCFD_series_2(data_dir=DATA_DIR):
    """
    Load RCFD Series 2 data from a local Parquet file.

    Args:
        data_dir (Path): Directory path where the data is stored.

    Returns:
        pd.DataFrame: DataFrame loaded from the 'RCFD_Series_2.parquet' file.
    """
    path = Path(data_dir) / "pulled" / "RCFD_Series_2.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_wrds_call_research(data_dir=DATA_DIR):
    """
    Load WRDS Call Research data from a local Parquet file.

    Args:
        data_dir (Path): Directory path where the data is stored.

    Returns:
        pd.DataFrame: DataFrame loaded from the 'wrds_call_research.parquet' file.
    """
    path = Path(data_dir) / "manual"
    filtered_path = path / "wrds_call_research_filtered.parquet"
    original_path = path / "wrds_call_research.parquet"
    if filtered_path.exists():
        return pd.read_parquet(filtered_path)
    if original_path.exists():
        print("Filtered data does not exist......we create the new parquet file")
        df = pd.read_parquet(original_path)
        start_date = datetime.date(2022, 3, 31)
        end_date = datetime.date.today()
        filtered_df = data_preprocessing.filter_data(df, start_date, end_date)
        filtered_df.to_parquet(filtered_path)
        return filtered_df
    path = Path(data_dir) / "manual" / "wrds_call_research.parquet"
    comp = pd.read_parquet(filtered_path)
    return comp

if __name__ == "__main__":
    print("Pulling data....")
    # rcon_series_1  = pull_RCON_series_1(wrds_username=WRDS_USERNAME)
    # rcon_series_1.to_parquet(DATA_DIR / "pulled" / "RCON_Series_1.parquet")

    # rcon_series_2 = pull_RCON_series_2(wrds_username=WRDS_USERNAME)
    # rcon_series_2.to_parquet(DATA_DIR / "pulled" / "RCON_Series_2.parquet")
  
    # rcfd_series_1 = pull_RCFD_series_1(wrds_username=WRDS_USERNAME)
    # rcfd_series_1.to_parquet(DATA_DIR / "pulled" / "RCFD_Series_1.parquet")

    # rcfd_series_2 = pull_RCFD_series_2(wrds_username=WRDS_USERNAME)
    # rcfd_series_2.to_parquet(DATA_DIR / "pulled" / "RCFD_Series_2.parquet")
    
    # bhck1975_data = pull_BHCK1975(wrds_username=WRDS_USERNAME)
    # bhck1975_data.to_parquet(DATA_DIR / "pulled" / "BHCK1975.parquet")
