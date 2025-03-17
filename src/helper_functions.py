import numpy as np
import pandas as pd
from settings import config
from pathlib import Path
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

def get_etf_mbs_change(etf_mbs,start_date = "03/31/2022",end_date = "03/31/2023" ):
    etf_mbs_change = etf_mbs.loc[etf_mbs['Date'] == end_date, 'Close/Last'].values[0] /etf_mbs.loc[etf_mbs['Date'] == start_date, 'Close/Last'].values[0] - 1
    return etf_mbs_change


def get_treasury_change(treasuries_data,start_date = "2022-03-31",end_date = "2023-03-31"):
    treasury_change = treasuries_data.loc[treasuries_data['date'] == end_date, 'index'].values[0] / treasuries_data.loc[treasuries_data['date'] == start_date, 'index'].values[0] - 1
    return treasury_change


def GSIB_bank_id():
    """
    Returns a list of GSIB bank IDs.
    """
    #GSIB = [35301,93619,229913,398668,413208,451965,476810,480228,488318,
     #497404,541101,651448,688079,722777,812164,852218,934329,1225761,
     #1443266,1456501,2182786,2362458,2489805,2531991,3066025]
    GSIB = [852218, 480228, 476810, 413208, #JP Morgan, Bank of America, Citigroup, HSBC
      2980209, 2182786, 541101, 655839, 1015560, 229913,#Barclays, Goldman Sachs, BNY Mellon, CCB COMMUNITY BANK, ICBC, Mizuho
       1456501, 722777, 35301, 925411, 497404, 3212149, #Morgan Stanley, Santander, State Street, Sumitomo Mitsui, TD Bank, UBS
      451965] #wells fargo
    return GSIB

def large_ex_GSIB_bank_id(large):
    bank_id_large_ex_GSIB = []
    for bank_id in large['rssd9001']:
       bank_id_large_ex_GSIB.append(bank_id)
    return bank_id_large_ex_GSIB

def small_bank_id(small):
    bank_id_small = []
    for bank_id in small['rssd9001']:
       bank_id_small.append(bank_id)
    return bank_id_small

def calculate_losses(treasury_prices, df_rmbs, df_loans, df_treasuries, df_other_loans, df_asset, rmbs_multiplier):
    start_date = "2022-03-31"
    end_date ="2023-03-31"
    price_change = {
        '<1y': treasury_prices.loc[end_date, 'iShares 0-1'] / treasury_prices.loc[start_date, 'iShares 0-1'] - 1,
        '1y-3y': treasury_prices.loc[end_date, 'iShares 1-3'] / treasury_prices.loc[start_date, 'iShares 1-3'] - 1,
        '3y-5y': treasury_prices.loc[end_date, 'sp 3-5'] / treasury_prices.loc[start_date, 'sp 3-5'] - 1,
        '7y-10y': 0.5 * (treasury_prices.loc[end_date, 'iShares 7-10'] / treasury_prices.loc[start_date, 'iShares 7-10'] - 1) + 0.5 * (treasury_prices.loc[end_date, 'iShares 10-20'] / treasury_prices.loc[start_date, 'iShares 10-20'] - 1),
        '>20y': treasury_prices.loc[end_date, 'iShares 20+'] / treasury_prices.loc[start_date, 'iShares 20+'] - 1,
    }
    # Define the mapping of buckets to be used for aggregation
    bucket_mapping = {
        '<3m': '<1y',
        '3m-1y': '<1y',
        '1y-3y': '1y-3y',
        '3y-5y': '3y-5y',
        '5y-15y': '7y-10y',  # Assuming '5y-15y' should be mapped to '7y-10y' based on provided price_change calculation
        '>15y': '>20y',
    }
    aggregated_assets = {}
    for name, df in zip(['RMBS', 'Loans', 'Treasury', 'OtherLoan'], 
                        [df_rmbs, df_loans, df_treasuries, df_other_loans]):
        # Ensure columns for aggregation are present
        columns_to_aggregate = [col for col in list(bucket_mapping.keys()) if col in df.columns]
        aggregated_assets[name] = df.groupby(['rssd9001', 'date'])[columns_to_aggregate].sum().reset_index()

    # Initialize DataFrame to store results
    bank_losses_assets = pd.DataFrame(columns=[
        'date', 'bank_ID', 'RMBs_loss', 'treasury_loss', 'loans_loss', 'other_loan_loss', 
        'total_loss', 'Share RMBs', 'Share Treasury and Other', 
        'Share Residential Mortgage', 'Share Other Loan', 'RMBs_asset', 'treasury_asset', 
        'residential_mortgage_asset', 'other_loan_asset', 'core_asset', 'gross_asset', 'loss/core_asset', 'loss/gross_asset',
    ])

    # Iterate over each bank to calculate losses and assets
    for _, df_row in df_asset.iterrows():
        bank_id = df_row['rssd9001']
        bank_date = df_row['date']
        bank_total_asset = df_row['assets']
        
        #Initialize variables for loss and asset calculations
        rmbs_loss = loans_loss = treasury_loss = other_loan_loss = total_loss = 0
        rmbs_asset = treasury_asset = loan_asset = other_loan_asset = core_asset = 0
        
        #Calculating losses for RMBs
        if 'RMBS' in aggregated_assets and not aggregated_assets['RMBS'].empty:
            rmbs_row = aggregated_assets['RMBS'][(aggregated_assets['RMBS']['date'] == bank_date) & (aggregated_assets['RMBS']['rssd9001'] == bank_id)]
            for bucket, treasury_bucket in bucket_mapping.items():
                if bucket in rmbs_row.columns:
                    asset_amount = rmbs_row.iloc[0][bucket] if not rmbs_row.empty else 0
                    rmbs_loss += (asset_amount * rmbs_multiplier * price_change[treasury_bucket])
                    rmbs_asset += asset_amount

        #Calculating losses for loans
        loans_row = aggregated_assets['Loans'][(aggregated_assets['Loans']['date'] == bank_date) &(aggregated_assets['Loans']['rssd9001'] == bank_id)]
        if not loans_row.empty:
            for bucket, treasury_bucket in bucket_mapping.items():
                if bucket in loans_row.columns:
                    asset_amount = loans_row.iloc[0][bucket]
                    loans_loss += (asset_amount * rmbs_multiplier * price_change[treasury_bucket])
                    loan_asset += asset_amount

        #Calculating Treasuries
        treasury_row = aggregated_assets['Treasury'][(aggregated_assets['Treasury']['date'] == bank_date) & (aggregated_assets['Treasury']['rssd9001'] == bank_id)]
        if not treasury_row.empty:
            for bucket, treasury_bucket in bucket_mapping.items():
                if bucket in treasury_row.columns:
                    asset_amount = treasury_row.iloc[0][bucket]
                    treasury_loss += (asset_amount * price_change[treasury_bucket])
                    treasury_asset += asset_amount
        #Other loans
        other_loan_row = aggregated_assets['OtherLoan'][(aggregated_assets['OtherLoan']['date'] == bank_date) & (aggregated_assets['OtherLoan']['rssd9001'] == bank_id)]
        if not other_loan_row.empty:
            for bucket, treasury_bucket in bucket_mapping.items():
                if bucket in other_loan_row.columns:
                    asset_amount = other_loan_row.iloc[0][bucket]
                    other_loan_loss += (asset_amount * price_change[treasury_bucket])
                    other_loan_asset += asset_amount

            # Calculate total loss and core asset      
        total_loss = rmbs_loss + treasury_loss + loans_loss + other_loan_loss
        core_asset = rmbs_asset + treasury_asset + loan_asset + other_loan_asset

        # Append the results to the DataFrame
        bank_losses_assets.loc[len(bank_losses_assets)] = {
            'date': bank_date,
            'bank_ID': bank_id,
            'RMBs_loss': rmbs_loss,
            'treasury_loss': treasury_loss,
            'loans_loss': loans_loss,
            'other_loan_loss': other_loan_loss,
            'total_loss': total_loss,
            'Share RMBs': rmbs_loss / total_loss * 100 if total_loss else 0,
            'Share Treasury and Other': treasury_loss / total_loss * 100 if total_loss else 0,
            'Share Residential Mortgage': loans_loss / total_loss * 100 if total_loss else 0,
            'Share Other Loan': other_loan_loss / total_loss * 100 if total_loss else 0,
            'RMBs_asset': rmbs_asset,
            'treasury_asset': treasury_asset,
            'residential_mortgage_asset': loan_asset,
            'other_loan_asset': other_loan_asset,
            'core_asset': core_asset,
            'gross_asset': bank_total_asset,
            'loss/core_asset': -(total_loss / core_asset) if core_asset else 0,
            'loss/gross_asset': -(total_loss / bank_total_asset) if bank_total_asset else 0,
        }
    return bank_losses_assets


def calculate_uninsured_deposit_mm_asset(uninsured_deposit, bank_losses):
    
    # Initialize an empty list to store the results
    results = []
    
    # Adjust the uninsured_deposit DataFrame to use both 'bank_name' and 'Bank_ID' as a multi-index for quick lookup
    uninsured_lookup = uninsured_deposit.set_index(['rssd9001'])['uninsured_deposits'].to_dict()
    
    # Iterate over each row in bank_losses DataFrame
    for index, bank_loss_row in bank_losses.iterrows():
        bank_id = bank_loss_row['bank_ID']
        
        # Adjust the lookup to include 'Bank_ID'
        uninsured_deposit_value = uninsured_lookup.get((bank_id), 0)
        
        # Calculate 'MM Asset' as the sum of 'total_loss' and 'gross_asset' (as defined in the paper)
        mm_asset = bank_loss_row['total_loss'] + bank_loss_row['gross_asset']
        
        # Calculate Uninsured Deposit/MM Asset ratio 
        if mm_asset > 0:
            uninsured_deposit_mm_asset_ratio = uninsured_deposit_value / mm_asset
        
        # Append to final dataframe
        results.append({
            'bank_ID': bank_id, 
            'total_loss': bank_loss_row['total_loss'], 
            'total_asset': bank_loss_row['gross_asset'],
            'mm_asset': mm_asset,
            'uninsured_deposit': uninsured_deposit_value, 
            'Uninsured_Deposit_MM_Asset': uninsured_deposit_mm_asset_ratio
        })
    
    # Convert results list to DataFrame and sort by 'Bank_ID'
    results_df = pd.DataFrame(results).sort_values(by=['bank_ID'])
    
    return results_df



def final_statistic_table(bank_losses_assets, uninsured_deposit_mm_asset, insured_deposit_coverage=None, index_name = 'All Banks'):
    # Merge the DataFrames on bank_name and Bank_ID to include uninsured deposit/MM Asset ratios and insured deposit coverage ratios
    
    
    bank_count = len(bank_losses_assets.index)

    final_stats = pd.DataFrame({
        'Aggregate Loss': [f"{-round(bank_losses_assets['total_loss'].sum() / 1e9, 1)}T"],  # Convert to trillions
        'Bank Level Loss': [f"{-round(bank_losses_assets['total_loss'].median() / 1e3, 1)}M"],  # Convert to millions
        'Bank Level Loss Std': [f"{round(bank_losses_assets['total_loss'].std() / 1e6, 2)}B"],  # Std deviation for Bank Level Loss
        'Share RMBS': [round(bank_losses_assets['Share RMBs'].median() * 100, 1)],  # Median percentage
        'Share RMBS Std': [round(bank_losses_assets['Share RMBs'].std() * 100, 1)],  # Std deviation for Share RMBS
        'Share Treasury and Other': [round(bank_losses_assets['Share Treasury and Other'].median() * 100, 1)],  # Median percentage
        'Share Treasury and Other Std': [round(bank_losses_assets['Share Treasury and Other'].std() * 100, 1)],  # Std deviation
        'Share Residential Mortgage': [round(bank_losses_assets['Share Residential Mortgage'].median() * 100, 1)],  # Median percentage
        'Share Residential Mortgage Std': [round(bank_losses_assets['Share Residential Mortgage'].std() * 100, 1)],  # Std deviation
        'Share Other Loan': [round(bank_losses_assets['Share Other Loan'].median() * 100, 1)],  # Median percentage
        'Share Other Loan Std': [round(bank_losses_assets['Share Other Loan'].std() * 100, 1)],  # Std deviation
        'Loss/Asset': [round(bank_losses_assets['loss/gross_asset'].median() * 100, 1)],  # Median percentage
        'Loss/Asset Std': [round(bank_losses_assets['loss/gross_asset'].std() * 100, 1)],  # Std deviation
        'Uninsured Deposit/MM Asset': [round(uninsured_deposit_mm_asset['Uninsured_Deposit_MM_Asset'].median() * 100, 1)],  # Median percentage
        'Uninsured Deposit/MM Asset Std': [round(uninsured_deposit_mm_asset['Uninsured_Deposit_MM_Asset'].std() * 100, 1)],  # Std deviation
        # 'Insured Deposit Coverage Ratio': [round(insured_deposit_coverage['insured_deposit_coverage_ratio'].median() * 100, 1)],  # Median percentage
        # 'Insured Deposit Coverage Ratio Std': [round(insured_deposit_coverage['insured_deposit_coverage_ratio'].std() * 100, 1)],  # Std deviation
        'Number of Banks': [len(bank_losses_assets.index.unique())]  # Count of unique banks
    })

    # Rename index to 'All Banks'
    final_stats.index = [index_name]

    final_stats = final_stats.T
    
    return final_stats

import pandas as pd

def save_dataframe_as_latex_table_1(df, filename="table_1.tex", caption="Table Caption", label="tab:example"):
    """
    Convert a Pandas DataFrame to a LaTeX table and save it to a file.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to convert.
    filename (str): The output .tex file name.
    caption (str): The caption for the LaTeX table.
    label (str): The label for referencing the table in LaTeX.
    """
    latex_code = df.to_latex(index=True, caption=caption, label=label, column_format="lcccc", escape=False)
    
    with open(OUTPUT_DIR / filename, "w") as f:
        f.write(latex_code)
    
    print(f"LaTeX table saved as {filename}")

