#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().run_line_magic('pip', 'install investpy')


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





# In[ ]:


# idrssd_to_lei = pd.read_parquet('../manual data/idrssd_to_lei.parquet')
# lei_legalevents = pd.read_parquet('../manual data/lei_legalevents.parquet')
# lei_main = pd.read_parquet('../manual data/lei_main.parquet')
# lei_successorentity = pd.read_parquet('../manual data/lei_successorentity.parquet')
# wrds_bank_crsp_link = pd.read_parquet('../manual data/wrds_bank_crsp_link.parquet')
# wrds_call_research = pd.read_parquet('../data/manual/wrds_call_research.parquet')
wrds_call_research = pull_WRDS_call_reports.load_wrds_call_research()
# wrds_struct_rel_ultimate = pd.read_parquet('../manual data/wrds_struct_rel_ultimate.parquet')


# In[ ]:


# wrds_bank_crsp_link['rssd9001'] = wrds_bank_crsp_link['rssd9001'].astype('int')
wrds_call_research['rssd9001'] = wrds_call_research['rssd9001'].astype('int')


# From Table A1, you need:
# 	1.	Total Assets (assets)
# 	2.	Cash Holdings (cash)
# 	3.	Securities (Total, Treasury, RMBS, CMBS, ABS, Other) (securities, possibly other breakdowns)
# 	4.	Loans (Total, Real Estate, Commercial, Consumer, etc.)
# 	5.	Reverse Repo and Fed Funds Sold (reverse_repo, fedfundsrate)
# 	6.	Categorization into Bank Size Groups
# 	•	Small banks: Assets < $1.384B
# 	•	Large banks (non-GSIB): Assets ≥ $1.384B
# 	•	GSIB: Systemically important banks (may require separate classification logic)

# Step 3: Categorize Banks by Size: Use the assets column to classify banks

# Step 4: Compute Median & Standard Deviation for Each Group

# In[ ]:


import pandas as pd
import numpy as np
import datetime

# Step 1: Select data for Q1 2022 explicitly
report_date = datetime.date(2022, 3, 31)
df_date = wrds_call_research[wrds_call_research['date'] == pd.Timestamp(report_date)].copy()

# Verify unique banks at reporting date
num_banks = df_date['rssd9001'].nunique()
print(f"Number of banks as of {report_date}: {num_banks}")


# In[ ]:


# small, large(non gsib), gsib
# Categorize banks based on total asset size and GSIB status
small_bank_threshold = 1_384_000  # Assets in thousands (1.384B)

# First classify banks into Small vs Large
df_date["bank_category"] = np.where(
    df_date["assets"] < small_bank_threshold, "Small", "Large (non-GSIB)"
)

# Define GSIB banks explicitly by RSSD IDs
gsib_list = [
    852218, 480228, 476810, 413208, # JP Morgan, Bank of America, Citigroup, HSBC
    2980209, 2182786, 541101, 655839, 1015560, 229913, # Barclays, Goldman Sachs, BNY Mellon, etc.
    1456501, 722777, 35301, 925411, 497404, 3212149, # Morgan Stanley, Santander, State Street, etc.
    451965 # Wells Fargo
]

# Within large banks, classify GSIB banks separately
df_date.loc[
    (df_date["bank_category"] == "Large (non-GSIB)") & 
    (df_date["rssd9001"].isin(gsib_list)), 
    "bank_category"
] = "GSIB"

# Verify categories
print(df_date["bank_category"].value_counts())


# In[ ]:


# Explicitly calculation: other_security & other_re_loan & loanstonondep
df_date['other_security'] = df_date['securities'].fillna(0) - (
    df_date[['treasurysec', 'residentialmbsat', 'commercialmbsat', 'absassets']].fillna(0).sum(axis=1)
)

df_date['other_re_loan'] = df_date['reloans'].fillna(0) - (
    df_date[['loans1to4fam', 'realoansnonres']].fillna(0).sum(axis=1)
)

# Loan to Non-Depository (loanstonondep)
df_date['loanstonondep'] = df_date[['otherciloans', 'otherbankacceptances']].fillna(0).sum(axis=1)


# In[ ]:


# Define all asset-related columns clearly and explicitly
asset_cols = {
    'cash': 'Cash',
    'securities': 'Security',
    'treasurysec': 'Treasury',
    'residentialmbsat': 'RMBS',
    'commercialmbsat': 'CMBS',
    'absassets': 'ABS',
    'other_security': 'Other Security',
    'loansnet': 'Total Loan',
    'reloans': 'Real Estate Loan',
    'loans1to4fam': 'Residential Mortgage',
    'realoansnonres': 'Commercial Mortgage',
    'other_re_loan': 'Other Real Estate Loan',
    'agloans': 'Agricultural Loan',
    'ciloans': 'Commercial & Industrial Loan',
    'persloans': 'Consumer Loan',
    'loanstonondep': 'Loan to Non-Depository',
    'fedfundsrepoasset': 'Fed Funds Sold',
    'fedfundsrepoliab': 'Reverse Repo' 
}


# In[ ]:


# Temporarily fill any missing columns with zeros (for safety)
for col in ['loans1to4fam', 'realoansnonres', 'loanstonondep', 'fedfundsrepoliab']:
    if col not in df_date.columns:
        df_date[col] = 0

# Compute percentages of each asset category relative to total assets
df_date['assets'] = df_date['assets'].replace({0: np.nan})

for col in asset_cols.keys():
    df_date[f'{col}_pct'] = (df_date[col].fillna(0) / df_date['assets']) * 100


# In[ ]:


# Calculate Aggregate separately (sum percentages explicitly)
aggregate_assets_sum = df_date['assets'].sum()

aggregate_pct_dict = {
    f'{col}_pct': (df_date[col].sum() / aggregate_assets_sum) * 100
    for col in asset_cols.keys()
}

aggregate_df = pd.DataFrame(aggregate_pct_dict, index=['Aggregate'])


# In[ ]:


# Compute median and std for other bank categories
grouped = df_date.groupby('bank_category')

non_agg_summary = grouped[[f'{col}_pct' for col in asset_cols]].agg(['median', 'std'])

# Add total assets (in billions) and number of banks explicitly
non_agg_summary[('Total Asset ($B)', '')] = grouped['assets'].sum() / 1e6
non_agg_summary[('Number of Banks', '')] = grouped['rssd9001'].nunique()

# Flatten multi-level column index
non_agg_summary.columns = [' '.join(col).strip() for col in non_agg_summary.columns.values]


# In[ ]:


non_agg_summary2 = grouped[[f'{col}' for col in asset_cols]].agg(['sum'])
non_agg_summary2.columns = [' '.join(col).strip() for col in non_agg_summary2.columns.values]


# In[ ]:


non_agg_summary2


# In[ ]:


# Rename columns clearly
rename_dict = {}
for key, val in asset_cols.items():
    rename_dict[f'{key}_pct median'] = f'{val} Median'
    rename_dict[f'{key}_pct std'] = f'{val} Std'

non_agg_summary.rename(columns=rename_dict, inplace=True)


# In[ ]:


# Explicit calculation of Aggregate row (sum percentages)
aggregate_data = {f'{val} Median': (df_date[col].sum() / aggregate_assets_sum) * 100 for col, val in asset_cols.items()}
aggregate_summary = pd.DataFrame(aggregate_pct_dict, index=['Aggregate'])

# Add total asset sum and bank count explicitly for aggregate
aggregate_extra = pd.DataFrame({
    'Total Asset ($B)': aggregate_assets_sum / 1e6,  # convert thousands to billions
    'Number of Banks': df_date['rssd9001'].nunique()
}, index=['Aggregate'])


# In[ ]:


aggregate_summary


# In[ ]:


# Step 10: Clearly define Total Asset and Number of Banks
aggregate_assets_sum = df_date['assets'].sum()
aggregate_extra = pd.DataFrame({
    'Total Asset ($B)': [aggregate_assets_sum / 1e6],  # Convert from thousands to billions
    'Number of Banks': df_date['rssd9001'].nunique()
}, index=['Aggregate'])

# Clearly define aggregate percentage DataFrame
aggregate_pct_dict = {
    f'{col}_pct': (df_date[col].sum() / aggregate_assets_sum) * 100
    for col in asset_cols.keys()
}
aggregate_pct_df = pd.DataFrame(aggregate_pct_dict, index=['Aggregate'])

# Create std rows as NaNs explicitly for aggregate (since it's sum, no std)
aggregate_std_df = pd.DataFrame({f'{col}_pct': np.nan for col in asset_cols.keys()}, index=['Aggregate'])

# Rename columns clearly (match non_agg_summary columns)
aggregate_pct_df.columns = [f'{asset_cols[col.split("_pct")[0]]} Median' for col in aggregate_pct_df.columns]

# Combine median and NaN std explicitly
aggregate_summary_final = pd.concat([aggregate_extra, aggregate_pct_df], axis=1)
for col in aggregate_summary_final.columns:
    if 'Median' in col:
        aggregate_summary_final[col.replace('Median', 'Std')] = np.nan  # explicitly NaN for Aggregate std

# Ensure columns order matches non-aggregate exactly
aggregate_summary_final = aggregate_summary_final[non_agg_summary.columns]

# Step 11: Combine aggregate and non-aggregate clearly
final_summary = pd.concat([aggregate_summary_final, non_agg_summary])

# Replace infinite values with NaN and round clearly
final_summary.replace([np.inf, -np.inf], np.nan, inplace=True)
final_summary = final_summary.round(2)

# After you obtain final_summary (before transposing):

# Reorder columns clearly to put "Total Asset" and "Number of Banks" first
first_cols = ['Total Asset ($B)', 'Number of Banks']
other_cols = [col for col in final_summary.columns if col not in first_cols]

final_summary = final_summary[first_cols + other_cols]

# Now transpose explicitly
final_summary['Number of Banks'] = final_summary['Number of Banks'].astype(int)
final_table = final_summary.T


# In[ ]:


final_summary['Number of Banks']


# In[ ]:


final_table_styled = (
    final_table.style
    .format(precision=2, na_rep='')
    .set_properties(**{
        'text-align': 'center',
        'font-size': '14px',
        'padding': '8px'
    })
    .set_table_styles([
        {'selector': 'th',
         'props': [('background-color', 'white'),
                   ('color', 'black'),
                   ('text-align', 'center'),
                   ('font-size', '15px'),
                   ('border-bottom', '2px solid black'),
                   ('padding', '10px')]},
        {'selector': 'td',
         'props': [('background-color', 'white'),
                   ('color', 'black')]}
    ])
)

# Display styled table explicitly
display(final_table_styled)


# In[ ]:


final_table = final_table.rename(index=lambda x: x.replace("Commercial & Industrial", "C/I"))
final_table = final_table.rename(index=lambda x: x.replace("Total Asset ($B)", "Total Asset (Billions)"))


# In[ ]:


helper_functions.save_dataframe_as_latex_table_1(final_table, filename="replicated_table_1A.tex", caption="Replicated Table 1A: Bank Asset Composition", label="tab:replicated1A")


# In[ ]:


def dataframe_to_image(df, filename="dataframe.png"):
    fig, ax = plt.subplots(figsize=(len(df.columns) * 1.2, len(df) * 0.5))  # Adjust size dynamically
    ax.axis('tight')
    ax.axis('off')
    
    table = pd.plotting.table(ax, df, loc='center', cellLoc='center', colWidths=[0.2]*len(df.columns))
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.2)  # Adjust scale
    
    plt.savefig(filename, bbox_inches='tight', dpi=300)
    plt.close()


# dataframe_to_image(final_table, "table_a1.png")


# In[ ]:


final_table


# In[ ]:


non_agg_summary2.rename(columns=lambda x: x.replace(" sum", ""), inplace=True)
non_agg_summary2


# In[ ]:


aggregate_sums = non_agg_summary2.sum()
total_assets_sum = aggregate_sums[["cash", "securities", "reloans", "ciloans", "other_security"]]# .sum()
total_liabilities_sum = aggregate_sums[["loansnet", "loanstonondep", "fedfundsrepoasset", "fedfundsrepoliab"]]# .sum()

# # Compute proportions
# asset_aggregates = {
#     category: (aggregate_sums[category] / total_assets_sum) * 100
#     for category in ["cash", "securities", "reloans", "ciloans", "other_security"]
# }

# liability_aggregates = {
#     category: (aggregate_sums[category] / total_liabilities_sum) * 100
#     for category in ["loansnet", "loanstonondep", "fedfundsrepoasset", "fedfundsrepoliab"]
# }

# # Convert to DataFrame for display
# aggregate_df = pd.DataFrame([asset_aggregates, liability_aggregates], index=["Assets (%)", "Liabilities (%)"])


# In[ ]:


total_assets_sum, total_liabilities_sum


# In[ ]:


import importlib
importlib.reload(helper_functions)
helper_functions.save_stacked_bar_chart(total_assets_sum, total_liabilities_sum, "assets_liabilities_chart.png")


# In[ ]:




