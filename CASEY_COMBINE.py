
#%%
import os
import pandas as pd
from glob import glob

# Directory containing the CSV files
input_dir = r"C:\UVI\QAQC stuff\casey\output"
output_dir = r"C:\UVI\QAQC stuff\casey\combined"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Get all CSV files in the directory
csv_files = glob(os.path.join(input_dir, "*.csv"))

# Group files by the second part of their filename
file_groups = {}
for file in csv_files:
    filename = os.path.basename(file)
    parts = filename.split("_")
    if len(parts) > 1:
        key = parts[1]  # Use the second part of the filename
        file_groups.setdefault(key, []).append(file)

# Process each group
for key, files in file_groups.items():
    merged_df = None

    for idx, file in enumerate(files):
        df = pd.read_csv(file, parse_dates=["Date Time, GMT-04:00"])
        
        # Find the temperature column
        temp_col = [col for col in df.columns if "Temp" in col or "Temperature" in col]
        if temp_col:
            temp_col = temp_col[0]  # Get the first match
            df.rename(columns={temp_col: f"Temp {'A' if idx == 0 else 'B'}"}, inplace=True)

        # Merge on "Date Time, GMT-04:00"
        if merged_df is None:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on="Date Time, GMT-04:00", how="outer")

    # Ensure both Temp A and Temp B exist
    if "Temp A" not in merged_df.columns:
        merged_df["Temp A"] = None
    if "Temp B" not in merged_df.columns:
        merged_df["Temp B"] = None

    # Create new columns
    merged_df["Average"] = merged_df[["Temp A", "Temp B"]].mean(axis=1)
    merged_df["Variance"] = abs(merged_df["Temp A"] - merged_df["Temp B"])

    # Keep only necessary columns
    merged_df = merged_df[["Date Time, GMT-04:00", "Temp A", "Temp B", "Average", "Variance"]]

    # Save the combined file
    output_file = os.path.join(output_dir, f"Combined_{key}.csv")
    merged_df.to_csv(output_file, index=False)
    print(f"Saved: {output_file}")

print("Processing complete.")

# %%

# import black point combined
blkpt_combined = pd.read_csv(r'C:\UVI\QAQC stuff\casey\combined\Temp_Black Point_combined.csv')
# Convert 'Date' column to datetime format (if not already parsed correctly)
blkpt_combined["Date"] = pd.to_datetime(blkpt_combined["Date"], errors="coerce")

# Filter for dates from January 1, 2014, onwards
blkpt_combined = blkpt_combined[blkpt_combined["Date"] >= "2014-01-01 00:00:00"]
# %%
casey_combine = r'C:\UVI\QAQC stuff\casey\combined'

casey_files = {}

for file_name in os.listdir(casey_combine):
    if file_name.startswith('Combined'):
        file_path = os.path.join(casey_combine, file_name)

        # Load data
        data = pd.read_csv(file_path)

        # Convert the 'Date' column to datetime format
        if 'Date Time, GMT-04:00' in data.columns:
            data['Date Time, GMT-04:00'] = pd.to_datetime(data['Date Time, GMT-04:00'])

        # Store the data in dictionary
        casey_files[file_name] = data

print(casey_files.items())
# %%
# Define the key column
datetime_col = "Date Time, GMT-04:00"

# List to hold data
data_frames = []

# Loop through each DataFrame in casey_files
for df in casey_files.values():
    # Keep only relevant columns
    df = df[[datetime_col, "Average", "Variance"]]
    data_frames.append(df)

# Concatenate all data
merged_df = pd.concat(data_frames, ignore_index=True)

# Drop duplicates and sort by date
merged_df.drop_duplicates(subset=[datetime_col], inplace=True)
merged_df.sort_values(by=datetime_col, inplace=True)

# Save or display the combined data
print(merged_df.head())

# Optionally save to CSV
output_file = r'C:\UVI\QAQC stuff\casey\combined\All_Casey_Combined.csv'
merged_df.to_csv(output_file, index=False)
print(f"Saved: {output_file}")
# %%
# PUT THIS IN THE QAQC CODE AFTER CHECKING LENTGHS FOR THE CASEY STUFF
# CASEY STUFF

import os

# Define the export folder path
export_folder = r"C:\UVI\QAQC stuff\casey\output"

# Ensure the export folder exists
os.makedirs(export_folder, exist_ok=True)

# Iterate through each site code
for site_code, file_numbers in df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        # Iterate through each file identifier
        for identifier, info in identifiers.items():
            df = info['DataFrame']
            filename = f"{site_code}_{file_number}_{identifier}.csv"
            filepath = os.path.join(export_folder, filename)

            # Export DataFrame to CSV
            df.to_csv(filepath, index=False)
            print(f"Exported: {filepath}")