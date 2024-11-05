import pandas as pd
import glob
import os
import numpy as np

# Step 1: Define folder path and unique identifier column
folder_path = "./data/"  # Update this to your folder path
identifier_column = "SEQN"  # Ensure this column is consistent across files

# Step 2: Get a list of all CSV files
csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

if not csv_files:
    print("No CSV files found. Please check the folder path.")
    exit()

print(f"Found {len(csv_files)} CSV files.")

# Step 3: Function to merge files incrementally using inner joins
def incremental_merge(files):
    """Incrementally merge files with inner joins to reduce memory usage."""
    merged_df = None  # Initialize the merged DataFrame

    for i, file in enumerate(files):
        print(f"Processing file {i + 1}/{len(files)}: {file}")

        # Read the current file in chunks to avoid memory overload
        current_df = pd.read_csv(file, dtype=str, low_memory=True)

        if merged_df is None:
            merged_df = current_df  # Initialize the merged DataFrame with the first file
        else:
            # Merge only matching rows to keep the size smaller (inner join)
            merged_df = pd.merge(merged_df, current_df, on=identifier_column, how='outer')

        def merge_rows(group):
            merged_row = group.iloc[0].copy()  # Start with the first row
            # Iterate through each column and fill missing values
            for col in group.columns:
                if col != 'SEQN':  # Don't modify SEQN
                    # Use non-null values to fill missing data
                 merged_row[col] = group[col].dropna().max() if not group[col].dropna().empty else np.nan
            return merged_row

        # Step 2: Group by SEQN and apply the merging function to each group
        merged_df = merged_df.groupby('SEQN', as_index=False).apply(merge_rows)
        merged_df = merged_df.reset_index(drop=True)
        print(f"Merged DataFrame now has {len(merged_df)} rows.")

        # Free up memory
        del current_df

    return merged_df

# Step 4: Merge the files incrementally
try:
    merged_df = incremental_merge(csv_files)

    # Step 5: Save the final merged DataFrame to a CSV file
    output_file = "merged_result.csv"
    merged_df.to_csv(output_file, index=False)
    print(f"Merged CSV saved to: {os.path.abspath(output_file)}")

except Exception as e:
    print(f"An error occurred: {e}")

