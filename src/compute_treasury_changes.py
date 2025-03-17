import pandas as pd
import matplotlib.pyplot as plt

def compute_treasury_index_changes(sp_treasury_data):
    """
    Computes percentage changes for mark-to-market (MTM) loss calculations.

    Steps:
    1. Compute daily percentage changes in the S&P Treasury Bond Index.
    2. Compute cumulative percentage changes from the first available date.
    
    Args:
    - sp_treasury_data (pd.DataFrame): Cleaned S&P Treasury Index data.
    
    Returns:
    - pd.DataFrame: Updated DataFrame with 'daily_change' and 'cumulative_change' columns.
    """

    # Step 1: Compute daily percentage change
    sp_treasury_data['daily_change'] = sp_treasury_data['treasury_index'].pct_change() * 100

    # Step 2: Compute cumulative percentage change from the first available date
    base_index_value = sp_treasury_data.iloc[0]['treasury_index']
    sp_treasury_data['cumulative_change'] = ((sp_treasury_data['treasury_index'] - base_index_value) / base_index_value) * 100

    print("✅ Percentage changes computed successfully!")
    return sp_treasury_data

# Example usage (assuming cleaned_sp_treasury_data is available)
# updated_sp_treasury_data = compute_treasury_index_changes(cleaned_sp_treasury_data)
