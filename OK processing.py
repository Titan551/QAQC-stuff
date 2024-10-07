#OK processing

#%% Import libraries
import os
import pandas as pd
import glob
import matplotlib.pyplot as plt
from datetime import datetime
#%% Get file paths
 
# Define folder path where your CSV files are located
folder_path = r'C:\UVI\QAQC stuff\PD_processing\Provisional_Duplicates_2024'

# Define pattern to match CSV files
Start_file_name = 'OK_*.csv'  # Corrected the pattern to match CSV files

# Use glob to get a list of file paths matching the pattern set in Start_file_name
csv_files = glob.glob(os.path.join(folder_path, Start_file_name))

# Print file paths
print(csv_files)
#%% Create dataframes
PD_frames = []  # Create an empty list to store dataframes

# Iterate over each file path and read CSV files into dataframes
for file in csv_files:
    df = pd.read_csv(file)
    PD_frames.append(df)  # Append each dataframe to the list
print(PD_frames)
# %% drop columns and export
# Define the output folder path
output_folder = r'C:\UVI\QAQC stuff\PD_processing\PD_2024_output'

# Rename the Average_temp column to Temperature
for df in PD_frames:
    df.rename(columns={'Average_temp': 'Temperature'}, inplace=True)

# Export the files dropping the OK_ from the naming convention and keeping selected columns
for idx, df in enumerate(PD_frames):
    # Drop 'OK_' from the file name
    file_name = os.path.basename(csv_files[idx])[3:]
    # Select desired columns
    selected_columns = ['#', 'Date Time, GMT-04:00', 'Temperature']
    selected_df = df[selected_columns]
    # Export to a new CSV file in the output folder
    selected_df.to_csv(os.path.join(output_folder, file_name), index=False)
    print(f"File '{file_name}' was exported to {output_folder}")