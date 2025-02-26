import pandas as pd
import matplotlib.pyplot as plt

import pandas as pd
import matplotlib.pyplot as plt

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
        plt.show()

    # return missing_summaries

