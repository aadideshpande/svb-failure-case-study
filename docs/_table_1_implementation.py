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
import helper_functions

warnings.filterwarnings("ignore")


# In[ ]:


import importlib
importlib.reload(pull_WRDS_call_reports)
importlib.reload(data_preprocessing)


# ### Pull and Preprocess WRDS Call Research Data

# In[ ]:


df = pull_WRDS_call_reports.load_wrds_call_research()


# In[ ]:


df.shape


# In[ ]:


df.dtypes


# In[ ]:


df_filtered = df
df_filtered.head()


# In[ ]:


df_filtered = df_filtered.sort_values(by=['date'])  # Ensure the data is sorted by date
df_filtered = df_filtered.groupby('rssd9001').first().reset_index()


# In[ ]:


# df_filtered[df_filtered['rssd9001'] == '37'].head()


# In[ ]:


df_filtered.shape


# In[ ]:


len(df_filtered['rssd9001'].unique())


# In[ ]:


df_new = df_filtered[['rssd9001', 'date', 'assets', 'securitiesheldtomaturity',
                      'securitiesavailableforsale', 'mbsassets', 'absassets', 'loans', 'totaldep', 'alldepuninsured', 'treasurysec', 'timedepuninsured', 'domdepuninsured',
                      'securitiesrmbs_less_3m', 'securitiesrmbs_3m_1y', 'securitiesrmbs_1y_3y', 'securitiesrmbs_3y_5y', 'securitiesrmbs_5y_15y', 'securitiesrmbs_over_15y',
                      'resloans_less_3m', 'resloans_3m_1y', 'resloans_1y_3y', 'resloans_3y_5y', 'resloans_5y_15y', 'resloans_over_15y']]


# In[ ]:


df_new.shape


# ### Load and Preprocess ETF MBS Data

# In[ ]:


etf_mbs = pull_mbb_data.load_from_manual_csv()


# In[ ]:


etf_mbs.head()


# In[ ]:


type(etf_mbs.iloc[0]['Date'])


# ### Load and Preprocess Treasuries Data

# In[ ]:


treasuries_data = pull_treasuries_data.load_from_manual_excel()


# In[ ]:


treasuries_data.head()


# In[ ]:


treasuries_data = data_preprocessing.preprocess_treasuries_data(treasuries_data)


# In[ ]:


treasuries_data.head()


# In[ ]:





# In[ ]:


df_new.head()


# In[ ]:


df_sorted = df_new.sort_values(by=['date'])
df_final = df_sorted.groupby('rssd9001').first().reset_index()


# ### Calculate MTM Losses

# In[ ]:


etf_mbs_change = helper_functions.get_etf_mbs_change(etf_mbs)
treasury_change = helper_functions.get_treasury_change(treasuries_data)


# In[ ]:


df_final['mtm_loss_mbs'] = df_final['mbsassets'] * etf_mbs_change
df_final['mtm_loss_treasury'] = df_final['treasurysec'] * treasury_change
df_final['mtm_loss_loans'] = df_final['loans'] * treasury_change
df_final['total_mtm_loss'] = df_final['mtm_loss_mbs'] + df_final['mtm_loss_treasury'] + df_final['mtm_loss_loans']


# In[ ]:


aggregate_loss = df_final['total_mtm_loss'].sum()


# In[ ]:


df_final['share_rmbs'] = df_final['mtm_loss_mbs'] / df_final['total_mtm_loss']
df_final['share_treasury_other'] = df_final['mtm_loss_treasury'] / df_final['total_mtm_loss']
df_final['share_residential_mortgage'] = df_final['mtm_loss_loans'] / df_final['total_mtm_loss']


# In[ ]:


df_final['loss_to_asset'] = df_final['total_mtm_loss'] / df_final['assets']
df_final['uninsured_deposit_mm_asset'] = df_final['alldepuninsured'] / (df_final['assets'] - df_final['total_mtm_loss'])


# ### Assets

# In[ ]:


df_assets = df_new[['rssd9001', 'date', 'assets']]


# In[ ]:


df_asset = df_assets #total assets all banks

#list of GSIB bank IDs
GSIB = helper_functions.GSIB_bank_id()
GSIB = [str(element) for element in GSIB]

#total assets all GSIB banks
df_asset_GSIB = df_asset[df_asset['rssd9001'].isin(GSIB)]

#total assets large non-GSIB banks
df_asset_large_ex_GSIB = df_asset[(~df_asset['rssd9001'].isin(GSIB)) & (df_asset['assets']>1384000)]

#total assets small banks
df_asset_small = df_asset[(~df_asset['rssd9001'].isin(GSIB)) & (df_asset['assets']<=1384000)] 


# In[ ]:


df.dtypes


# In[ ]:


df_asset[df_asset['rssd9001'] == '30810'].head()


# In[ ]:


len(df_asset_large_ex_GSIB), len(df_asset_GSIB), len(df_asset_small), len(df_new)


# ### Insured

# In[ ]:


df_insured = df_new[['rssd9001', 'date', 'alldepuninsured','timedepuninsured', 'domdepuninsured']]


# ### RMBs

# In[ ]:


df_rmbs = df_final[['rssd9001', 'date', 'securitiesrmbs_less_3m', 'securitiesrmbs_3m_1y', 'securitiesrmbs_1y_3y', 'securitiesrmbs_3y_5y', 'securitiesrmbs_5y_15y', 'securitiesrmbs_over_15y']]


# In[ ]:


df_rmbs[df_rmbs['rssd9001'] == '37'].head()


# ### Loans

# In[ ]:


df_loans = df_final[['rssd9001', 'date', 'resloans_less_3m', 'resloans_3m_1y', 'resloans_1y_3y', 'resloans_3y_5y', 'resloans_5y_15y', 'resloans_over_15y']]


# In[ ]:


df_loans[df_loans['rssd9001'] == '37'].head()


# In[ ]:


df_final.columns


# ### Treasuries

# In[ ]:


df_treasuries = df_filtered[['rssd9001', 'date', 'securitiestreasury_less_3m',
'securitiestreasury_3m_1y',
'securitiestreasury_1y_3y',
'securitiestreasury_3y_5y',
'securitiestreasury_5y_15y',
'securitiestreasury_over_15y']]


# In[ ]:


df_treasuries[df_treasuries['rssd9001'] == '37'].head()


# ### Other Loans

# In[ ]:


df_other_loans = df_filtered[['rssd9001', 'date', 'loansleases_less_3m',
'loansleases_3m_1y',
'loansleases_1y_3y',
'loansleases_3y_5y',
'loansleases_5y_15y',
'loansleases_over_15y']]


# In[ ]:


df_other_loans[df_other_loans['rssd9001'] == '37'].head()


# ### Other

# In[ ]:


df_rmbs = df_rmbs.rename(columns={
'securitiesrmbs_less_3m': '<3m',
'securitiesrmbs_3m_1y':'3m-1y',
'securitiesrmbs_1y_3y':'1y-3y',
'securitiesrmbs_3y_5y':'3y-5y',
'securitiesrmbs_5y_15y':'5y-15y',
'securitiesrmbs_over_15y':'>15y',
})

df_treasuries = df_treasuries.rename(columns={
'securitiestreasury_less_3m': '<3m',
'securitiestreasury_3m_1y':'3m-1y',
'securitiestreasury_1y_3y':'1y-3y',
'securitiestreasury_3y_5y':'3y-5y',
'securitiestreasury_5y_15y':'5y-15y',
'securitiestreasury_over_15y':'>15y',
})

df_loans = df_loans.rename(columns={
'resloans_less_3m': '<3m',
'resloans_3m_1y':'3m-1y',
'resloans_1y_3y':'1y-3y',
'resloans_3y_5y':'3y-5y',
'resloans_5y_15y':'5y-15y',
'resloans_over_15y':'>15y',
})

df_other_loans = df_other_loans.rename(columns={
'loansleases_less_3m': '<3m',
'loansleases_3m_1y':'3m-1y',
'loansleases_1y_3y':'1y-3y',
'loansleases_3y_5y':'3y-5y',
'loansleases_5y_15y':'5y-15y',
'loansleases_over_15y':'>15y',
})


# In[ ]:


sum_asset = 0
for df in [df_rmbs, df_loans, df_treasuries, df_other_loans]:
    total = df[['<3m', '3m-1y', '1y-3y', '3y-5y', '5y-15y', '>15y']].sum().sum()
    sum_asset += total

print('total assets:',"{:,.2f}".format(sum_asset))
total_asset = df_asset['assets'].sum()
print('assets ratio:',"{:,.2%}".format(sum_asset/total_asset))


# In[ ]:


rcon_series_1 = pull_WRDS_call_reports.load_RCON_series_1()


# In[ ]:





# In[ ]:


rcon_series_1['rssd9999'] = pd.to_datetime(rcon_series_1['rssd9999'])
# Define required dates
required_dates = [pd.Timestamp("2022-03-31"), pd.Timestamp("2023-03-31")]

rcon_series_1 = rcon_series_1.groupby('rssd9001').filter(lambda x: all(date in x['rssd9999'].values for date in required_dates))
rcon_series_1 = rcon_series_1.rename(columns={
    'rssd9999': 'date'
})


# In[ ]:


df_uninsured = rcon_series_1[['rssd9001', 'date', 'uninsured_deposits']]
df_insured = rcon_series_1[['rssd9001', 'date', 'insured_deposit_1', 'insured_deposit_2']]


# In[ ]:


asset_small = df_asset_small['rssd9001'].unique()
asset_large_ex_GSIB = df_asset_large_ex_GSIB['rssd9001'].unique()


# In[ ]:


len(df_asset_large_ex_GSIB), len(df_asset_small), len(GSIB)


# In[ ]:


importlib.reload(helper_functions)
etf_mbs_change = helper_functions.get_etf_mbs_change(etf_mbs)
treasury_change = helper_functions.get_treasury_change(treasuries_data)
rmbs_multiplier = etf_mbs_change / treasury_change
rmbs_multiplier


# In[ ]:


etf_mbs.tail()


# In[ ]:


rmbs_multiplier


# In[ ]:


treasury_prices = pd.read_excel("../data/manual/combined_index_df.xlsx")
treasury_prices.set_index('date', inplace=True)
start_date = "2022-03-31"
end_date ="2023-03-31"


# In[ ]:


importlib.reload(helper_functions)
bank_losses = helper_functions.calculate_losses(treasury_prices, df_rmbs, df_loans, df_treasuries, df_other_loans, df_asset, rmbs_multiplier)


# In[ ]:


bank_losses[bank_losses['bank_ID'] == '12311'].head()


# In[ ]:


median_percentage = bank_losses[['Share RMBs', 'Share Treasury and Other', 
        'Share Residential Mortgage', 'Share Other Loan']].median()
median_percentage


# In[ ]:


std_percentages =bank_losses[['Share RMBs', 'Share Treasury and Other', 
        'Share Residential Mortgage', 'Share Other Loan']].std()
std_percentages


# In[ ]:


total_sum_loss = bank_losses['total_loss'].sum()
total_sum_loss


# In[ ]:


median_bank_loss = bank_losses['total_loss'].median()
median_bank_loss 


# In[ ]:


core_asset = bank_losses['core_asset'].sum()
gross_asset = bank_losses['gross_asset'].sum()
core_asset / gross_asset


# In[ ]:


median_loss_asset_ratio = bank_losses['loss/gross_asset'].median()
median_loss_asset_ratio


# In[ ]:


average_loss_asset_ratio = bank_losses['loss/gross_asset'].mean()
average_loss_asset_ratio


# In[ ]:


std_loss_asset_ratio = bank_losses['loss/gross_asset'].std()
std_loss_asset_ratio


# In[ ]:


df_uninsured.head()
uninsured_deposit = df_uninsured[df_uninsured['date'] == '2022-03-31']


# In[ ]:


uninsured_deposit['rssd9001'] = uninsured_deposit['rssd9001'].astype(str)


# In[ ]:


uninsured_deposit.head()


# In[ ]:


uninsured_deposit['uninsured_deposits'].sum()/gross_asset 


# In[ ]:


uninsured_deposit['uninsured_deposits'].sum()


# In[ ]:


uninsured_deposit.columns


# In[ ]:


importlib.reload(helper_functions)
un_mm_ratio = helper_functions.calculate_uninsured_deposit_mm_asset(uninsured_deposit, bank_losses)


# In[ ]:


un_mm_ratio['total_loss'].sum()


# In[ ]:


un_mm_ratio['total_asset'].sum()


# In[ ]:


un_mm_ratio['mm_asset'].sum()


# In[ ]:


un_mm_ratio['Uninsured_Deposit_MM_Asset'].median()


# In[ ]:


un_mm_ratio['Uninsured_Deposit_MM_Asset'].std()


# In[ ]:


#df_insured['insured_deposit'] = df_insured['insured_deposit_1'] + df_insured['insured_deposit_2']


# In[ ]:


# df_insured['insured_deposit'].sum()


# In[ ]:


importlib.reload(helper_functions)
large_ex_GSIB = helper_functions.large_ex_GSIB_bank_id(df_asset_large_ex_GSIB)
small = helper_functions.small_bank_id(df_asset_small)


# In[ ]:


len(df_asset_GSIB), len(df_asset_large_ex_GSIB), len(df_asset_small)


# In[ ]:


df_RMBS_GSIB = df_rmbs[df_rmbs['rssd9001'].isin(GSIB)]
df_RMBS_large_ex_GSIB = df_rmbs[df_rmbs['rssd9001'].isin(large_ex_GSIB)]
df_RMBS_small = df_rmbs[df_rmbs['rssd9001'].isin(small)]
len(df_RMBS_GSIB), len(df_RMBS_large_ex_GSIB), len(df_RMBS_small)


# In[ ]:


df_loans_GSIB = df_loans[df_loans['rssd9001'].isin(GSIB)]
df_loans_large_ex_GSIB = df_loans[df_loans['rssd9001'].isin(large_ex_GSIB)]
df_loans_small = df_loans[df_loans['rssd9001'].isin(small)]
len(df_loans_GSIB), len(df_loans_large_ex_GSIB), len(df_loans_small)


# In[ ]:


df_treasuries_GSIB = df_treasuries[df_treasuries['rssd9001'].isin(GSIB)]
df_treasuries_large_ex_GSIB = df_treasuries[df_treasuries['rssd9001'].isin(large_ex_GSIB)]
df_treasuries_small = df_treasuries[df_treasuries['rssd9001'].isin(small)]
len(df_treasuries_GSIB), len(df_treasuries_large_ex_GSIB), len(df_treasuries_small)


# In[ ]:


df_other_loans_GSIB = df_other_loans[df_other_loans['rssd9001'].isin(GSIB)]
df_other_loans_large_ex_GSIB = df_other_loans[df_other_loans['rssd9001'].isin(large_ex_GSIB)]
df_other_loans_small = df_other_loans[df_other_loans['rssd9001'].isin(small)]
len(df_other_loans_GSIB), len(df_other_loans_large_ex_GSIB), len(df_other_loans_small)


# In[ ]:


uninsured_deposit_GSIB = uninsured_deposit[uninsured_deposit['rssd9001'].isin(GSIB)]
uninsured_deposit_large_ex_GSIB = uninsured_deposit[uninsured_deposit['rssd9001'].isin(large_ex_GSIB)]
uninsured_deposit_small = uninsured_deposit[uninsured_deposit['rssd9001'].isin(small)]
len(uninsured_deposit_GSIB), len(uninsured_deposit_large_ex_GSIB), len(uninsured_deposit_small)


# In[ ]:


bank_losses = helper_functions.calculate_losses(
    treasury_prices, df_rmbs, df_loans, df_treasuries, df_other_loans, df_asset, rmbs_multiplier
)


# In[ ]:


bank_losses_gsib = helper_functions.calculate_losses(
    treasury_prices, df_RMBS_GSIB, df_loans_GSIB, df_treasuries_GSIB, df_other_loans_GSIB, df_asset_GSIB, rmbs_multiplier 
)


# In[ ]:


bank_losses_large = helper_functions.calculate_losses(
    treasury_prices, df_RMBS_large_ex_GSIB, df_loans_large_ex_GSIB, df_treasuries_large_ex_GSIB, df_other_loans_large_ex_GSIB, df_asset_large_ex_GSIB, rmbs_multiplier
)


# In[ ]:


bank_losses_small = helper_functions.calculate_losses(
    treasury_prices, df_RMBS_small, df_loans_small, df_treasuries_small, df_other_loans_small, df_asset_large_ex_GSIB, rmbs_multiplier
)


# In[ ]:


uninsured_deposit_mm_asset_small = helper_functions.calculate_uninsured_deposit_mm_asset(uninsured_deposit_small, bank_losses_small)
uninsured_deposit_mm_asset_large_ex_GSIB = helper_functions.calculate_uninsured_deposit_mm_asset(uninsured_deposit_large_ex_GSIB, bank_losses_large)
uninsured_deposit_mm_asset_GSIB = helper_functions.calculate_uninsured_deposit_mm_asset(uninsured_deposit_GSIB, bank_losses_gsib)    


# In[ ]:


importlib.reload(helper_functions)
final_stats = helper_functions.final_statistic_table(bank_losses, un_mm_ratio, None)
final_stats_small = helper_functions.final_statistic_table(bank_losses_small, uninsured_deposit_mm_asset_small, None, index_name = 'Small Banks')
final_stats_large_ex_GSIB = helper_functions.final_statistic_table(bank_losses_large, uninsured_deposit_mm_asset_large_ex_GSIB, None, index_name = 'Large Ex GSIB Banks')
final_stats_GSIB = helper_functions.final_statistic_table(bank_losses_gsib, uninsured_deposit_mm_asset_GSIB, None, index_name = 'GSIB Banks')


# In[ ]:


table_1 = pd.concat([final_stats, final_stats_small, final_stats_large_ex_GSIB, final_stats_GSIB], axis=1)
table_1


# In[ ]:


helper_functions.save_dataframe_as_latex_table_1(table_1, filename="replicated_table_1.tex", caption="Replicated Table 1: Mark-to-Market Statistics by Bank Size", label="tab:replicated1")


# In[ ]:


table_1.index


# In[ ]:




