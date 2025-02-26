
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
    sql_query = """
        SELECT 
            b.rssd9001, b.rssd9017, b.rssd9999, b.rcona555, b.rcona247,
            b.rcona556, b.rcona557, b.rcona558, b.rcona559, b.rcona560,
            b.rcona564, b.rcona565, b.rcona566, b.rcona567, b.rcona568,
            b.rcona569, b.rcon5597, b.rconf045, b.rconf049
        FROM 
            bank.wrds_call_rcon_1 AS b
        WHERE 
            b.rssd9999 BETWEEN '2021-12-31' AND '2023-09-30'
        """

    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=wrds_username)
    
    # Execute the SQL query
    rcon_series_1 = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return rcon_series_1

def pull_RCON_series_2(wrds_username=WRDS_USERNAME):
    """Pull RCON series from WRDS.
    """
    sql_query = """
        SELECT 
            b.rssd9001, b.rssd9017, b.rssd9999, b.rcona549,
            b.rcona550, b.rcona551, b.rcona552, b.rcona553, b.rcona554, 
            b.rcona561, b.rcona562, b.rcona570, b.rcona571, b.rcona572, 
            b.rcona573, b.rcona574, b.rcona575, b.rcon2170
        FROM 
            bank.wrds_call_rcon_2 AS b
        WHERE 
            b.rssd9999 BETWEEN '2021-12-31' AND '2023-09-30'
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
            b.rssd9001, b.rssd9017, b.rssd9999, b.rcfda555, b.rcfda247,
            b.rcfda556, b.rcfda557, b.rcfda558, b.rcfda559, b.rcfda560,
            b.rcfda561, b.rcfda562, b.rcfda570, b.rcfda571, b.rcfda572, 
            b.rcfda573, b.rcfda574, b.rcfda575
        FROM 
            bank.wrds_call_rcfd_1 AS b
        WHERE 
            b.rssd9999 BETWEEN '2021-12-31' AND '2023-09-30'
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
            b.rssd9001, b.rssd9017, b.rssd9999, b.rcfda549,
            b.rcfda550, b.rcfda551, b.rcfda552, b.rcfda553, b.rcfda554, 
            b.rcfd2170
        FROM 
            bank.wrds_call_rcfd_2 AS b
        WHERE 
            b.rssd9999 BETWEEN '2021-12-31' AND '2023-09-30'
        """

    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=wrds_username)
    
    # Execute the SQL query
    rcfd_series_2 = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return rcfd_series_2


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





if __name__ == "__main__":
    rcon_series_1  = pull_RCON_series_1(wrds_username=WRDS_USERNAME)
    rcon_series_1.to_parquet(DATA_DIR / "pulled" / "RCON_Series_1.parquet")

    rcon_series_2 = pull_RCON_series_2(wrds_username=WRDS_USERNAME)
    rcon_series_2.to_parquet(DATA_DIR / "pulled" / "RCON_Series_2.parquet")
  
    rcfd_series_1 = pull_RCFD_series_1(wrds_username=WRDS_USERNAME)
    rcfd_series_1.to_parquet(DATA_DIR / "pulled" / "RCFD_Series_1.parquet")

    rcfd_series_2 = pull_RCFD_series_2(wrds_username=WRDS_USERNAME)
    rcfd_series_2.to_parquet(DATA_DIR / "pulled" / "RCFD_Series_2.parquet")