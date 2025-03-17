import pandas as pd
import matplotlib.pyplot as plt
from settings import config
from pathlib import Path
OUTPUT_DIR = Path(config("OUTPUT_DIR"))


def missing_data_analysis(dfs, dataset_names=None, show_plot=True):
    """
    Analyze missing data for multiple DataFrames.

    Parameters:
    - dfs (list of pd.DataFrame): A list of DataFrames to analyze.
    - dataset_names (list of str): Names of the datasets for labeling.
    - show_plot (bool): Whether to display bar charts.

    Returns:
    - A dictionary containing missing data summaries for each dataset.
    """
    if dataset_names is None:
        dataset_names = [f"Dataset {i+1}" for i in range(len(dfs))]
    
    missing_summaries = {}

    # Create a subplot for multiple datasets
    if show_plot:
        fig, axes = plt.subplots(1, len(dfs), figsize=(5 * len(dfs), 5))  # One row, multiple columns

        if len(dfs) == 1:  # Handle case where only one dataset is provided
            axes = [axes]

        for i, (df, name, ax) in enumerate(zip(dfs, dataset_names, axes)):
            missing_counts = df.isnull().sum()
            missing_percent = (missing_counts / len(df)) * 100

            # Store the missing data summary
            missing_summary = pd.DataFrame({
                "Missing Count": missing_counts,
                "Missing Percentage": missing_percent
            }).sort_values(by="Missing Percentage", ascending=False)
            
            missing_summaries[name] = missing_summary

            # Plot the missing data
            missing_summary["Missing Percentage"].plot(kind="bar", color="red", alpha=0.7, ax=ax)
            ax.set_title(f"Missing Data - {name}")
            ax.set_xlabel("Columns")
            ax.set_ylabel("Missing Percentage (%)")
            ax.tick_params(axis='x', rotation=45)
            ax.grid(axis="y", linestyle="--", alpha=0.7)

        plt.tight_layout()
        # 
        filename = OUTPUT_DIR / 'missing_data.png'
        plt.savefig(filename)
        plt.show()

def clean_sp_treasury_data(sp_treasury_data):
    """
    Cleans the S&P U.S. Treasury Bond Index data.
    
    Steps:
    1. Strips whitespace from column names.
    2. Renames 'Effective date' to 'date' and converts it to datetime.
    3. Renames 'S&P U.S. Treasury Bond Index' to 'treasury_index' for consistency.
    4. Sorts the data by date to ensure time-series continuity.
    5. Drops any duplicate rows if they exist.
    6. Handles any missing values by forward-filling (if necessary).
    
    Args:
    - sp_treasury_data (pd.DataFrame): Raw S&P Treasury Index data.
    
    Returns:
    - pd.DataFrame: Cleaned S&P Treasury Index data.
    """
    
    # Step 1: Standardize column names
    sp_treasury_data.rename(columns=lambda x: x.strip(), inplace=True)

    # Step 2: Rename key columns
    sp_treasury_data.rename(columns={
        'Effective date': 'date',
        'S&P U.S. Treasury Bond Index': 'treasury_index'
    }, inplace=True)

    # Step 3: Convert 'date' to datetime format
    sp_treasury_data['date'] = pd.to_datetime(sp_treasury_data['date'])

    # Step 4: Sort data by date (ensures proper time-series order)
    sp_treasury_data.sort_values(by='date', inplace=True)

    # Step 5: Drop duplicate rows if any exist
    sp_treasury_data.drop_duplicates(inplace=True)

    # Step 6: Handle missing values (forward-fill to maintain continuity)
    sp_treasury_data.fillna(method='ffill', inplace=True)

    print("✅ S&P Treasury Bond Index data cleaned successfully!")
    return sp_treasury_data

# Example usage (assuming sp_treasury_data is already loaded)
# cleaned_sp_treasury_data = clean_sp_treasury_data(sp_treasury_data)

import pandas as pd

def clean_and_merge_rcon_rcfd(rcon1, rcon2, rcfd1, rcfd2):
    """
    Cleans and merges RCON and RCFD datasets to produce a single structured dataset.
    
    Arguments:
    - rcon1: DataFrame from pull_RCON_series_1
    - rcon2: DataFrame from pull_RCON_series_2
    - rcfd1: DataFrame from pull_RCFD_series_1
    - rcfd2: DataFrame from pull_RCFD_series_2
    
    Returns:
    - Merged DataFrame containing relevant columns from both RCON and RCFD.
    """

    # Standardize column names to lowercase for consistency
    rcon1.columns = rcon1.columns.str.lower()
    rcon2.columns = rcon2.columns.str.lower()
    rcfd1.columns = rcfd1.columns.str.lower()
    rcfd2.columns = rcfd2.columns.str.lower()

    # Merge RCON datasets
    rcon_combined = pd.merge(rcon1, rcon2, on=['rssd9001', 'rssd9017', 'rssd9999'], how='outer')

    # Merge RCFD datasets
    rcfd_combined = pd.merge(rcfd1, rcfd2, on=['rssd9001', 'rssd9017', 'rssd9999'], how='outer')

    # Merge RCON and RCFD datasets
    final_dataset = pd.merge(rcon_combined, rcfd_combined, on=['rssd9001', 'rssd9017', 'rssd9999'], how='outer')

    # Convert date column to datetime format
    final_dataset['rssd9999'] = pd.to_datetime(final_dataset['rssd9999'])

    # Filter for Q1 2022 (January 1, 2022 - March 31, 2022)
    q1_2022_data = final_dataset[
        (final_dataset['rssd9999'] >= "2022-01-01") & 
        (final_dataset['rssd9999'] <= "2023-03-31")
    ]

    # Drop duplicates (if any)
    q1_2022_data = q1_2022_data.drop_duplicates()

    print("✅ Final dataset cleaned and merged successfully!")
    return q1_2022_data


def get_total_asset(rcfd_series_2, rcon_series_2, report_date = '03/31/2022'):
    """
    This function takes in the rcfd and rcon series and returns the total asset data for the given report date.

    Args:
    rcfd_series_2 (pd.DataFrame): rcfd series data
    rcon_series_2 (pd.DataFrame): rcon series data

    Returns:
    df_asset (pd.DataFrame): Total asset data for the given report date
    
    """

    #This grabs the
    asset_level_domestic_foriegn = rcfd_series_2[['rssd9001','rssd9017','rssd9999','rcfd2170']]
    asset_level_domestic = rcon_series_2[['rssd9001','rssd9017','rssd9999','rcon2170']]

    #drop the rows with missing values
    asset_level_domestic_foriegn.dropna(inplace = True)
    asset_level_domestic.dropna(inplace = True)

    filtered_asset_level_domestic_foriegn = asset_level_domestic_foriegn[asset_level_domestic_foriegn['rssd9999'] == report_date]
    filtered_asset_level_domestic = asset_level_domestic[asset_level_domestic['rssd9999'] == report_date]

    filtered_asset_level_domestic_foriegn  = filtered_asset_level_domestic_foriegn.rename(columns={
    'rcfd2170': 'Total Asset'})

    filtered_asset_level_domestic  = filtered_asset_level_domestic.rename(columns={
    'rcon2170': 'Total Asset'})

    # Concatenate the two dataframes
    df_asset = pd.concat([filtered_asset_level_domestic_foriegn, filtered_asset_level_domestic])

    df_asset = df_asset[['rssd9001','rssd9017','Total Asset']]

    df_asset  = df_asset.rename(columns={
    'rssd9001': 'Bank_ID',
    'rssd9017': 'bank_name',
    'rssd9999': 'report_date',
    'Total Asset': 'gross_asset',
    })

    return df_asset

def filter_data(df, start_date, end_date):
    df_filtered = df[
        (df['date'] >= start_date) &
        (df['date'] <= end_date)
    ]
    df_filtered['date'] = pd.to_datetime(df_filtered['date'])
    # Define required dates
    required_dates = [pd.Timestamp("2022-03-31"), pd.Timestamp("2023-03-31")]
    df_filtered['rssd9001'] = df_filtered['rssd9001'].astype(str)
    # Group by 'rssd9001' and filter out groups that do not contain both required dates
    df_filtered = df_filtered.groupby('rssd9001').filter(lambda x: all(date in x['date'].values for date in required_dates))
    return df_filtered

def preprocess_treasuries_data(df: pd.DataFrame):
    df.rename(columns=lambda x: x.strip(), inplace=True)
    df['Effective date'] = pd.to_datetime(df['Effective date'])
    df.rename(columns={
        'Effective date': 'date',
        'S&P U.S. Treasury Bond Index': 'index'
    }, inplace=True)
    return df

def get_partitioned_data(df, gsib, large, small):
    df_gsib = df[df['rssd9001'].isin(gsib)]
    df_large = df[df['rssd9001'].isin(large)]
    df_small = df[df['rssd9001'].isin(small)]
    return df_gsib, df_large, df_small

def rename_etf_mbs_columns(df):
    """
    Flatten a MultiIndex column structure where columns look like
    ('Close','MBB'), ('Date',''), etc. The function returns a DataFrame
    whose columns are single-level strings, e.g. "Close_MBB" or "Date".
    """
    # Map each tuple in df.columns to a single string
    df.columns = [
        f"{col[0]}_{col[1]}" if col[1] else col[0]
        for col in df.columns
    ]
    return df

def missing_values_percentage(df):
    """
    Calculate the percentage of missing values in each column, 
    sort them in descending order, and plot the top 10 columns.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        None
    """
    # Calculate percentage of missing values
    missing_percent = (df.isnull().sum() / len(df)) * 100

    # Sort in descending order
    missing_percent_sorted = missing_percent[missing_percent > 0].sort_values(ascending=False)

    # Plot the top 10 columns with missing values
    plt.figure(figsize=(15, 3))
    missing_percent_sorted[:10].plot(kind='bar', color='red', edgecolor='black')
    plt.title('Top 10 Columns with Missing Values (%)')
    plt.ylabel('Percentage')
    plt.xlabel('Columns')
    plt.xticks(rotation=45)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    filename = OUTPUT_DIR / 'wrds_premium_data.png'
    plt.savefig(filename)
    plt.show()

