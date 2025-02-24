import pandas as pd
import matplotlib.pyplot as plt

def missing_data_analysis(df, show_plot=True):
    """
    Analyze missing data in a DataFrame.

    Parameters:
    - df (pd.DataFrame): The input DataFrame.
    - show_plot (bool): Whether to display a bar chart of missing values.

    Returns:
    - A DataFrame summarizing the missing data per column.
    """
    # Compute missing values
    missing_counts = df.isnull().sum()
    total_rows = len(df)
    missing_percent = (missing_counts / total_rows) * 100

    # Create a summary DataFrame
    missing_summary = pd.DataFrame({
        "Missing Count": missing_counts,
        "Missing Percentage": missing_percent
    }).sort_values(by="Missing Percentage", ascending=False)

    # Plot missing data if enabled
    if show_plot:
        plt.figure(figsize=(10, 5))
        missing_summary["Missing Percentage"].plot(kind="bar", color="red", alpha=0.7)
        plt.title("Missing Data Percentage per Column")
        plt.xlabel("Columns")
        plt.ylabel("Missing Percentage (%)")
        plt.xticks(rotation=45, ha="right")
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        plt.show()

    return missing_summary
