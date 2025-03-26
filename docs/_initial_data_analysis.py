#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import warnings
import wrds
# import data_read
import pull_WRDS_call_reports, pull_treasuries_data
import pull_mbb_data
import data_preprocessing, compute_treasury_changes
# import calc_functions
# import Calc_table_statistic

warnings.filterwarnings("ignore")


# ### Treasuries Data

# In[ ]:


import investpy
import pandas as pd

indices = investpy.get_indices(country='united states')
indices[indices['name'].str.contains('S&P', case=False)]


# In[ ]:


indices[indices['symbol'].str.contains('SPBDUSBT', case=False)]


# In[ ]:


import importlib
importlib.reload(pull_treasuries_data)
treasuries_data = pull_treasuries_data.pull_SP_Treasury_Bond_Index_investpy()


# In[ ]:


treasuries_data = pull_treasuries_data.load_from_manual_excel()


# In[ ]:


treasuries_data.head()


# In[ ]:


treasuries_data = data_preprocessing.preprocess_treasuries_data(treasuries_data)
treasuries_data.head()


# ##### Tried to pull S&P Bond Index from WRDS, Yahoo and Investpy but this did not work. Hence, we use data that was pulled manually

# ### MBS ETF Data

# In[ ]:


etf_mbs = pull_mbb_data.pull_MBB_data()
if etf_mbs is not None:
    etf_mbs = data_preprocessing.rename_etf_mbs_columns(etf_mbs)
    etf_mbs.head()


# In[ ]:





# In[ ]:


etf_mbs = pull_mbb_data.load_from_manual_csv()


# In[ ]:


etf_mbs.dtypes


# In[ ]:


etf_mbs.head()


# ##### We pull the MBS ETF Data from yfinance. Also, as an alternative, we have manual data

# ### Call Reports Data

# In[ ]:


rcfd_data_1 = pull_WRDS_call_reports.load_RCFD_series_1()
rcfd_data_2 = pull_WRDS_call_reports.load_RCFD_series_2()
rcon_data_1 = pull_WRDS_call_reports.load_RCON_series_1()
rcon_data_2 = pull_WRDS_call_reports.load_RCON_series_2()


# In[ ]:


importlib.reload(data_preprocessing)
datasets = [rcfd_data_1, rcfd_data_2, rcon_data_1, rcon_data_2]
data_preprocessing.missing_data_analysis(datasets, dataset_names=[
    'rcfd_data_1', 'rcfd_data_2', 'rcon_data_1', 'rcon_data_2'
], show_plot=True)


# In[ ]:


df = pull_WRDS_call_reports.load_wrds_call_research()


# In[ ]:


# Convert 'date' column to datetime format
df['date'] = pd.to_datetime(df['date'])

# Define the date range
start_date = "2022-01-01"
end_date = "2023-03-31"

# Filter the DataFrame based on the date range
filtered_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

# Display the filtered data
filtered_df.head()


# In[ ]:


filtered_df.shape


# In[ ]:


filtered_df = filtered_df[['rssd9001', 'date', 'assets', 'securitiesheldtomaturity',
                      'securitiesavailableforsale', 'mbsassets', 'absassets', 'loans', 'totaldep', 'alldepuninsured', 'treasurysec', 'timedepuninsured', 'domdepuninsured',
                      'securitiesrmbs_less_3m', 'securitiesrmbs_3m_1y', 'securitiesrmbs_1y_3y', 'securitiesrmbs_3y_5y', 'securitiesrmbs_5y_15y', 'securitiesrmbs_over_15y',
                      'resloans_less_3m', 'resloans_3m_1y', 'resloans_1y_3y', 'resloans_3y_5y', 'resloans_5y_15y', 'resloans_over_15y', 'securitiestreasury_less_3m', 'securitiestreasury_3m_1y', 'securitiestreasury_1y_3y','securitiestreasury_3y_5y', 'securitiestreasury_5y_15y', 'securitiestreasury_over_15y', 'loansleases_less_3m','loansleases_3m_1y', 'loansleases_1y_3y', 'loansleases_3y_5y', 'loansleases_5y_15y', 'loansleases_over_15y']
]


# In[ ]:


importlib.reload(data_preprocessing)
data_preprocessing.missing_values_percentage(filtered_df)


# In[ ]:




