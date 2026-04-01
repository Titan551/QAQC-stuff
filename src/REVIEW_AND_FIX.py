# REVIEW/FIX

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

# Set paths from config
from config import CONFIG, get_path_for

review_folder = get_path_for("04_TOREVIEW")
deployment_log_path = os.path.join(CONFIG['BASE_DIRECTORY'],"Temperature_UVI_deployment_log.csv")
output_folder = get_path_for("05_READY")

#%% IMPORT FUNCTIONS
from QAQC_HELPER_FUNCTIONS import (
    import_trimmed,
    trim_dataframe
)



#%%
'''
THE SECTIONS BELOW ARE FOR ADDITIONAL TRIMMING AND PD FILES, WHAT TO DO WITH THEM IS TBD
'''

#%%
'''
ADDITIONAL TRIMMING
'''
#%%
cutoff_datetime = pd.Timestamp('2025-04-16 16:45:00')

# Pass the file number (e.g., '001') and file identifier (e.g., 'c') to access the DataFrame
trim_dataframe(df_files, 'TCCB67', '2311', 'c', panama_codes, cutoff_datetime)

#%%
'''
START OF SECTION: IMPORT AND WRANGLE PD FILES
'''

#%%
# PROVISIONAL DUPLICATES
    # not setup yet but probably want to put these in a separate code
PD_export_path = config["PD_export_path"]
pd_output_folder = config["pd_output_folder"]

# FILE PATHS FROM THE CONFIG
#PD_export_path: "C:/UVI/QAQC stuff/PD_processing/Provisional_Duplicates_2025"
#pd_output_folder: "C:/UVI/QAQC stuff/PD_processing/PD_2025_output"


# set file pattern to pull files
PD_start_file_name = 'PD_*.csv'

#get the list of files matching the PD name
PD_files = glob.glob(os.path.join(PD_folder_path,PD_start_file_name))

# print the files
print(PD_files)

#%% Create dataframes
PD_frames = {}  # Create an empty dictionary to store dataframes

# Iterate over each file path and read CSV files into dataframes
for file in PD_files:
    # Use the base file name (without the directory path) as the key
    file_name = os.path.basename(file).replace('.csv', '')
    
    # Read the file into a DataFrame and store it in the dictionary
    PD_frames[file_name] = pd.read_csv(file)

# Print the dictionary of dataframes
print(PD_frames)

#%%
'''
END OF SECITON: IMPORT AND WRANGLE PD FILES
NEXT SECTION: CUSTOM CHANGES TO PD FILES
'''

#%%
# editing PD_BT_TCBKPT_2402_2410 - remove rows 33 and 34, average the rest
PD_frames['PD_BT_TCBKPT_2402_2410'] = PD_frames['PD_BT_TCBKPT_2402_2410'][~PD_frames['PD_BT_TCBKPT_2402_2410']['#'].isin([33, 34])]
PD_frames['PD_BT_TCBKPT_2402_2410']['Average_temp'] = (PD_frames['PD_BT_TCBKPT_2402_2410']['Temp A'] + PD_frames['PD_BT_TCBKPT_2402_2410']['Temp B'])/2

print(PD_frames['PD_BT_TCBKPT_2402_2410'])

#%%
# edting PD_BT_TCCB08_2403_2411 - use Temp A before 2024-09-09 12:00:00, then average
# Ensure the Date Time column is in datetime format
PD_frames['PD_BT_TCCB08_2403_2411']['Date Time, UTC-04:00'] = pd.to_datetime(
    PD_frames['PD_BT_TCCB08_2403_2411']['Date Time, UTC-04:00']
)

# Define the cutoff date as a datetime object
cutoff_date = pd.Timestamp('2024-09-09 12:00:00')

# Create the Average_temp column with conditional logic
PD_frames['PD_BT_TCCB08_2403_2411']['Average_temp'] = PD_frames['PD_BT_TCCB08_2403_2411'].apply(
    lambda row: row['Temp A'] if row['Date Time, UTC-04:00'] < cutoff_date else (row['Temp A'] + row['Temp B']) / 2,
    axis=1
)

# Verify the results
print(PD_frames['PD_BT_TCCB08_2403_2411'])

#%%
# edting PD_BT_TCFLTC_2402_2410 - Just take the averages
PD_frames['PD_BT_TCFLTC_2402_2410']['Average_temp'] = (PD_frames['PD_BT_TCFLTC_2402_2410']['Temp A'] + PD_frames['PD_BT_TCFLTC_2402_2410']['Temp B'])/2
print(PD_frames['PD_BT_TCFLTC_2402_2410'])


#%%
# editing PD_BT_TCSHCS_2403_2410 - trim all before 2024-03-07 12:30:00
# Ensure the Date Time column is in datetime format
PD_frames['PD_BT_TCSHCS_2403_2410']['Date Time, UTC-04:00'] = pd.to_datetime(
    PD_frames['PD_BT_TCSHCS_2403_2410']['Date Time, UTC-04:00']
)

# Define the cutoff date as a datetime object
cutoff_date = pd.Timestamp('2024-03-07 12:30:00')

# Filter the DataFrame to remove rows before the cutoff date
PD_frames['PD_BT_TCSHCS_2403_2410'] = PD_frames['PD_BT_TCSHCS_2403_2410'][PD_frames['PD_BT_TCSHCS_2403_2410']['Date Time, UTC-04:00'] >= cutoff_date]

# Average temp columns
PD_frames['PD_BT_TCSHCS_2403_2410']['Average_temp'] = (PD_frames['PD_BT_TCSHCS_2403_2410']['Temp A']+PD_frames['PD_BT_TCSHCS_2403_2410']['Temp B'])/2

# Verify the results
print(PD_frames['PD_BT_TCSHCS_2403_2410'])

#%%
'''
END OF SECTION: CUSTOM CHANGES TO PD FILES
NEXT SECTION: EXPORTING PD FILES AS OK_
'''
#%%

# Ensure the export folder exists
os.makedirs(PD_export_path, exist_ok=True)

# Loop through each DataFrame in PD_frames
for file_name, df in PD_frames.items():
    # Replace 'PD_' with 'OK_' in the file name
    new_file_name = file_name.replace('PD_', 'OK_')
    
    # Create the full export path with the modified file name
    export_file_path = os.path.join(PD_export_path, f"{new_file_name}.csv")
    
    # Export the DataFrame to a CSV file
    df.to_csv(export_file_path, index=False)

    # Optionally, print a message confirming the export
    print(f"Exported {new_file_name} to {export_file_path}")


#%%
'''
END OF SECITON: EXPORTING PD FILES AS OK_
NEXT SECTION: OK_ FILES PROCESSING
'''
#%% Get file paths

# Define pattern to match CSV files
Start_file_name = 'OK_*.csv'  # Corrected the pattern to match CSV files

# Use glob to get a list of file paths matching the pattern set in Start_file_name
csv_files = glob.glob(os.path.join(PD_export_path, Start_file_name))

# Print file paths
print(csv_files)
#%% Create dataframes
OK_frames = []  # Create an empty list to store dataframes

# Iterate over each file path and read CSV files into dataframes
for file in csv_files:
    df = pd.read_csv(file)
    OK_frames.append(df)  # Append each dataframe to the list
print(OK_frames)

# %% drop columns and export

# Rename the Average_temp column to Temperature
for df in OK_frames:
    df.rename(columns={'Average_temp': 'Temperature'}, inplace=True)

# Export the files dropping the OK_ from the naming convention and keeping selected columns
for idx, df in enumerate(OK_frames):
    # Drop 'OK_' from the file name
    file_name = os.path.basename(csv_files[idx])[3:]
    # Select desired columns
    selected_columns = ['#', 'Date Time, UTC-04:00', 'Temperature']
    selected_df = df[selected_columns]
    # Export to a new CSV file in the output folder
    selected_df.to_csv(os.path.join(pd_output_folder, file_name), index=False)
    print(f"File '{file_name}' was exported to {pd_output_folder}")