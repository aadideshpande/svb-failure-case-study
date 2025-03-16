
from pathlib import Path

import pandas as pd
import wrds
from pandas.tseries.offsets import MonthEnd

from settings import config

OUTPUT_DIR = Path(config("OUTPUT_DIR"))
DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

def pull_RCON_series_1(wrds_username=WRDS_USERNAME):
    """Pull RCON series from WRDS.
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
    
    # Execute the SQL query
    rcon_series_1 = db.raw_sql(sql_query, date_cols=["rssd9999"])

    print("Pulled RCON Series 1")

    # Close the connection
    db.close()

    return rcon_series_1

def pull_RCON_series_2(wrds_username=WRDS_USERNAME):
    """Pull RCON series from WRDS.
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
    
    # Execute the SQL query
    rcon_series_2 = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return rcon_series_2

def pull_RCFD_series_1(wrds_username=WRDS_USERNAME):
    """Pull RCON series from WRDS.
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
    
    # Execute the SQL query
    rcfd_series_1 = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return rcfd_series_1

def pull_RCFD_series_2(wrds_username=WRDS_USERNAME):
    """Pull RCON series from WRDS.
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
    
    # Execute the SQL query
    rcfd_series_2 = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return rcfd_series_2

def pull_BHCK1975(wrds_username=WRDS_USERNAME):
    """Pull BHCK1975 series from WRDS Call Reports."""
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
    
    # Execute the SQL query
    bhck1975_data = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return bhck1975_data


def load_BHCK1975(data_dir=DATA_DIR):
    path = Path(data_dir) / "pulled" / "BHCK1975.parquet"
    return pd.read_parquet(path)



def load_RCON_series_1(data_dir=DATA_DIR):
    path = Path(data_dir) / "pulled" / "RCON_Series_1.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_RCON_series_2(data_dir=DATA_DIR):
    path = Path(data_dir) / "pulled" / "RCON_Series_2.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_RCFD_series_1(data_dir=DATA_DIR):
    path = Path(data_dir) / "pulled" / "RCFD_Series_1.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_RCFD_series_2(data_dir=DATA_DIR):
    path = Path(data_dir) / "pulled" / "RCFD_Series_2.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_wrds_call_research(data_dir=DATA_DIR):
    path = Path(data_dir) / "manual" / "wrds_call_research.parquet"
    comp = pd.read_parquet(path)
    return comp



if __name__ == "__main__":
    print("Pulling data....")
    rcon_series_1  = pull_RCON_series_1(wrds_username=WRDS_USERNAME)
    rcon_series_1.to_parquet(DATA_DIR / "pulled" / "RCON_Series_1.parquet")

    rcon_series_2 = pull_RCON_series_2(wrds_username=WRDS_USERNAME)
    rcon_series_2.to_parquet(DATA_DIR / "pulled" / "RCON_Series_2.parquet")
  
    rcfd_series_1 = pull_RCFD_series_1(wrds_username=WRDS_USERNAME)
    rcfd_series_1.to_parquet(DATA_DIR / "pulled" / "RCFD_Series_1.parquet")

    rcfd_series_2 = pull_RCFD_series_2(wrds_username=WRDS_USERNAME)
    rcfd_series_2.to_parquet(DATA_DIR / "pulled" / "RCFD_Series_2.parquet")
    
    bhck1975_data = pull_BHCK1975(wrds_username=WRDS_USERNAME)
    bhck1975_data.to_parquet(DATA_DIR / "pulled" / "BHCK1975.parquet")