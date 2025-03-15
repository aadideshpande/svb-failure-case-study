import numpy as np
import pandas as pd
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