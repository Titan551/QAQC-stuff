
#%% Imports
import os
import pandas as pd
import glob
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
import copy
import matplotlib.dates as mdates
import numpy as np

#%%
'''
SETTING INPUTS AND OUTPUTS
'''
# PATHS
 
# Define folder path where your CSV files are located
folder_path = r'C:\UVI\QAQC stuff\Temp_TCRMP_2025_PBL'

# Read in deployment log metadata sheet
deployment_df = pd.read_csv(r'C:\UVI\QAQC stuff\Temperature_UVI_deployment_log.csv')

# Define the output folder where you want to save the extracted files
output_folder = r"C:\UVI\QAQC stuff\Temp_TCRMP_2025_PBL_output"


# Output for plots
plots_path = r"C:\UVI\QAQC stuff\Temp_TCRMP_2025_PBL_output\graphs"

# Set folder with PD_files
PD_folder_path = r'C:\UVI\QAQC stuff\Temp_TCRMP_2025_PBL_output\Provisional Duplicates'

# Set the export folder path
PD_export_path = r'C:\UVI\QAQC stuff\PD_processing\Provisional_Duplicates_2025'

# Define the provisional duplicate output folder path
pd_output_folder = r'C:\UVI\QAQC stuff\PD_processing\PD_2025_output'


#%%
'''
FILE PATHS AND SITE CODES
'''
#%% Get file paths

# Define pattern to match CSV files
file_pattern = '*.csv'

# Use glob to get a list of file paths matching the pattern set in file_pattern
csv_files = glob.glob(folder_path + '/' + file_pattern)

# Print file paths
print(csv_files)


# %% Generate a list of all site codes

site_codes = ["TCCORB","TCFSHB","TCMERI","TCBKPT","TCBOTB","TCBRWB","TCBKIT",
              "TCCORK","TCCLGE","TCFLTC","TCGB63","TCGMKT","TCHB40","TCHB30",
              "TCHB20","TCMAGB","TCSAVA","TCSHCS","TCSCAP","TCSC35","TCSWAT",
              "TCLSTJ","TCBKIX","TCBX33","TCCB08","TCCB40","TCCB99","TCCB67",
              "TCCSTL","TCEAGR","TCGRPD","TCJCKB","TCKNGC","TCLBEM","TCLB99",
              "TCLB67","TCLBRH","TCMT24","TCMT40","TCSR30","TCSR99","TCSR41",
              "TCSR67","TCSR10","TCSPTH","TCLE67"]
## NOTE: site code TCCB60 was not in the site code metadata

#%% make a list for the panama site codes
panama_codes = ["PCAR04","PCAW10","PDG4X5","PDG20M","PUVCCP","PUVGC",
                "PUVM18","PUVR10","PUVR20","PUVR30","PAUVGC","PCAR03",
                "PCO3M","PCT18M","PDG10M","PIG10M","PIG20M","PSA3M",
                "PSA11M","PUVF10","PUVFLT","PCT3M","PUV3M"]

#%%
'''
DATAFRAMES
'''
#%% UTF 8 CHECK
# Walk through all files in the folder
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.lower().endswith('.csv'):
            file_path = os.path.join(root, file)

            try:
                # Attempt to read with UTF-8
                with open(file_path, 'r', encoding='utf-8') as f:
                    f.read()
                print(f"✅ Already UTF-8: {file_path}")

            except UnicodeDecodeError:
                try:
                    # Try reading with fallback encoding (e.g., Latin-1)
                    df = pd.read_csv(file_path, encoding='latin1')

                    # Save it back in UTF-8
                    df.to_csv(file_path, index=False, encoding='utf-8')
                    print(f"🔁 Converted to UTF-8: {file_path}")

                except Exception as e:
                    print(f"❌ Error processing {file_path}: {e}")


#%%
# Create an empty dictionary to store DataFrames structured by site code, file number, file identifier, and file name
df_files = {}

# Iterate through each CSV file
for csv_file in csv_files:
    # Extract site code, file number, file identifier, and file name from the file name
    file_name = os.path.basename(csv_file).split('.')[0]  # Remove the file extension
    parts = file_name.split('_')
    site_code = parts[1]  # Extract the site code
    file_number = parts[2]  # Extract the file number
    file_identifier = parts[-1] if len(parts) > 3 and parts[-1] != '' else "a" # Extract the file identifier ('a', 'b', etc.), assign 'a' if not present
    
    # Debug print
    print(f"Reading CSV for site: {site_code}, file: {csv_file}")

    # Read CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    
    # Extract the base file name without the extension
    base_file_name = os.path.splitext(os.path.basename(csv_file))[0]
    
    # Check if the site code already exists in the dictionary
    if site_code in df_files:
        # Check if the file number already exists in the dictionary for the site code
        if file_number in df_files[site_code]:
            # If the file number exists, check if the file identifier already exists
            if file_identifier in df_files[site_code][file_number]:
                print(f"Warning: Duplicate file identifier {file_identifier} for site code {site_code} and file number {file_number}. Ignoring.")
            else:
                # If the file identifier doesn't exist, add the DataFrame and file name to the dictionary
                df_files[site_code][file_number][file_identifier] = {'DataFrame': df, 'File Name': base_file_name}
        else:
            # If the file number doesn't exist, create a new dictionary for the file number
            df_files[site_code][file_number] = {file_identifier: {'DataFrame': df, 'File Name': base_file_name}}
    else:
        # If the site code doesn't exist, create a new dictionary for the site code
        df_files[site_code] = {file_number: {file_identifier: {'DataFrame': df, 'File Name': base_file_name}}}
    
    # Check if the site code is valid
    if site_code not in site_codes and site_code not in panama_codes:
        raise ValueError(f"Error: Site code '{site_code}' in {csv_file} is not in the valid site codes or Panama site codes list.")


#%%
'''
PLOT TITLE IDENTIFY
'''
#%%
# Some of the files have "Plot Title" in the first row that offsets what the actual column names are supposed to be
# this code loops through the files, prints those with Plot Title and removes it.
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        for file_identifier, file_info in file_data.items():
            file_name = file_info['File Name']
            df = file_info['DataFrame']

            # Check if any column name contains 'plot title'
            if any('plot title' in col.lower() for col in df.columns):
                print(f"Cleaning File: {file_name}")

                # Drop any rows containing 'plot title' in any cell
                df = df[~df.apply(
                    lambda row: row.astype(str).str.lower().str.contains('plot title').any(),
                    axis=1
                )].reset_index(drop=True)

                # Use the first row as the new header
                new_header = df.iloc[0]
                df = df[1:].reset_index(drop=True)
                df.columns = new_header

                # Store cleaned DataFrame back
                file_info['DataFrame'] = df

                print(f"  Cleaned and reheadered. New shape: {df.shape}")
                print(f"  New Columns: {df.columns.tolist()}")
                print("-" * 50)
            else:
                print('No sites found with Plot Title as a column name')

#%%
'''
PANAMA TIME CONVERSION
'''

#%%
# The local time in Panama is GMT 5 but the loggers are in GMT 4 so this cell loops through the panama files and converts the date time column

# Loop through the df_files dictionary
for site_code, files in df_files.items():
    # Only process if the site code is in the Panama codes
    if site_code in panama_codes:
        for file_number, file_versions in files.items():
            for identifier, file_data in file_versions.items():
                df = file_data['DataFrame']
                
                # Check for the column
                datetime_col = "Date Time, GMT-04:00"
                if datetime_col in df.columns:
                    # Convert to datetime if not already
                    df[datetime_col] = pd.to_datetime(df[datetime_col], errors='coerce')
                    
                    # Subtract 1 hour to convert GMT-4 to GMT-5
                    df[datetime_col] = df[datetime_col] - timedelta(hours=1)

                    # Rename
                    df.rename(columns={datetime_col: "Date Time, GMT-05:00"}, inplace=True)
                    
                else:
                    print(f"Warning: '{datetime_col}' not found in {file_data['File Name']}")


#%%
'''
SIO IDENTIFIER
if sio is in the name, remove it for proper naming convention
'''

#%%
# Iterate through each site code in the dictionary
for site_code, site_data in df_files.items():
    
    # Collect updates to avoid modifying the dictionary while iterating
    updated_files = {}
    
    for file_number, file_data in site_data.items():
        
        for file_identifier, file_info in file_data.items():
            file_name = file_info['File Name']  # Get the file name
            
            # Remove "SIO" from the file name (split by "_")
            file_name_parts = file_name.split("_")
            new_file_name_parts = [part for part in file_name_parts if part != "SIO"]
            new_file_name = "_".join(new_file_name_parts)
            
            # Overwrite the file name in the dictionary
            file_info['File Name'] = new_file_name
            
            # Extract the new file number from the third part of the UPDATED file name
            if len(new_file_name_parts) > 2:
                new_file_number = new_file_name_parts[2]
            else:
                new_file_number = file_number  # Fallback if there's no third part
            
            print(f"Updated File Name: {file_name} → {new_file_name}")
            print(f"Updated File Number: {file_number} → {new_file_number}")

            # Store the updated file data under the new file number
            if new_file_number not in updated_files:
                updated_files[new_file_number] = {}
            updated_files[new_file_number][file_identifier] = file_info

            # Update csv_file_names if it exists and contains this file
            if 'csv_file_names' in globals() and file_name in csv_file_names:
                csv_file_names[csv_file_names.index(file_name)] = new_file_name

            # Rename the actual .csv file in the folder
            old_path = os.path.join(folder_path, file_name + ".csv")
            new_path = os.path.join(folder_path, new_file_name + ".csv")

            if os.path.exists(old_path):
                os.rename(old_path, new_path)
                print(f"Renamed File: {old_path} → {new_path}")
            else:
                print(f"File not found, skipping: {old_path}")

    # Replace the original file data with the updated version
    df_files[site_code] = updated_files

# %% [markdown]
# ## File identifier check:
# This cell reports all the files that have an 'a' identifier that is assigned when imported. If there is a file without an 'a' identifier, that means there is only a 'b' file and will not be processed by the code. <u> This file needs to then be changed to 'a' so the code can process it.<u>
'''
FILE IDENTIFIER
'''
# %%
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        if 'a' not in file_data:
            print(f"Warning: No 'a' identifier found for site code {site_code} and file number {file_number}.")
        else:
            file_name = file_data['a']['File Name']
            print(f"File with 'a' identifier found for site code {site_code} and file number {file_number}: {file_name}")

#%%
# This cell checks if there are 2 files for a site and the time difference between the first measurements for each file. If the difference is greater than 10 minutes then the 'a' file is labeled as 'c' and the 'b' as 'd'.
'''
OFFSET FILE CHECK
'''

#%%
# Iterate through each site code in the dictionary
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        # Check if both 'a' and 'b' identifiers exist, otherwise print message and skip
        if 'a' not in file_data or 'b' not in file_data:
            print(f"Skipping site {site_code}, file {file_number}: Only one file present.")
            continue

        df_a = file_data['a']['DataFrame']
        df_b = file_data['b']['DataFrame']

        # Determine which datetime column to use based on site_code
        if site_code in panama_codes:
            datetime_col = "Date Time, GMT-05:00"
        else:
            datetime_col = "Date Time, GMT-04:00"

        # Check that the column exists in both dataframes
        if datetime_col not in df_a.columns or datetime_col not in df_b.columns:
            print(f"Skipping site {site_code}, file {file_number}: '{datetime_col}' column not found in both files.")
            continue

        # Convert first row to datetime
        try:
            first_time_a = pd.to_datetime(df_a.iloc[0][datetime_col], errors='coerce')
            first_time_b = pd.to_datetime(df_b.iloc[0][datetime_col], errors='coerce')

            # Get only the time part
            first_time_a = first_time_a.time()
            first_time_b = first_time_b.time()

            # Convert to datetime on the same arbitrary date for comparison
            today = datetime.today().date()
            dt_a = datetime.combine(today, first_time_a)
            dt_b = datetime.combine(today, first_time_b)

            # Calculate time difference (only using time of day)
            time_diff = abs(dt_a - dt_b)

        except Exception as e:
            print(f"Error processing site {site_code}, file {file_number}: {e}")
            continue

        # If time difference is greater than 10 minutes, rename identifiers
        if time_diff > timedelta(minutes=10):
            file_data['c'] = file_data.pop('a')
            file_data['d'] = file_data.pop('b')
            print(f"Renamed identifiers for site {site_code}, file {file_number}: 'a' -> 'c', 'b' -> 'd'")
        else:
            print(f"Skipping site {site_code}, file {file_number}: Time difference ({time_diff}) is not greater than 10 minutes.")

# %%
'''
DEPLOYMENT LOG AND FILTER
'''

# %% Filter deployment log metatdata sheet csv

# Extract file names from csv_files
csv_file_names = [os.path.basename(csv_file).split('.')[0] for csv_file in csv_files]

# Filter deployment_df to only include entries that match the file names in csv_files
filtered_deployment_df = deployment_df[deployment_df['Offloaded Filename'].isin(csv_file_names)]

# Print the filtered DataFrame
print(filtered_deployment_df)

# %%
'''
DEPLOYMENT LOG WARNING CHECKS
'''
# %% Find which file names you are processing did not match with the file names in the deployment log.


# Extract file names from csv_files
csv_file_names = [os.path.basename(csv_file).split('.')[0] for csv_file in csv_files]

# Filter deployment_df to only include entries that match the file names in csv_files
matched_files = deployment_df[deployment_df['Offloaded Filename'].isin(csv_file_names)]['Offloaded Filename'].tolist()

# Identify files in csv_files that did not match
unmatched_files = [file_name for file_name in csv_file_names if file_name not in matched_files]

# Print files in csv_files that did not match
print("!!!!!WARNING CHECK:!!!!!")
print("Files in csv_files that did not match:")
print(unmatched_files)
# These file names need to be fixed in the google sheet version and the sheet 
# needs to be redownloaded and the code needs to be run again.

# %% Subset columns from filtered_deployment_df

subset_columns = ['Offloaded Filename','Date In','Time In', 'Date Full', 'Date Out', 'Time Out']
filtered_deployment_df = filtered_deployment_df[subset_columns]
print(filtered_deployment_df)

#%% Check to see if there is a "?" in the "Time In" or the "Time Out" Columns
# May want to expand this to account for other instances of Incorrect metadata inputs
# Convert columns to strings
filtered_deployment_df['Time In'] = filtered_deployment_df['Time In'].astype(str)
filtered_deployment_df['Time Out'] = filtered_deployment_df['Time Out'].astype(str)

# Filter rows containing "?" in either Time In or Time Out
rows_with_question_mark = filtered_deployment_df[filtered_deployment_df['Time In'].str.contains('\?') | filtered_deployment_df['Time Out'].str.contains('\?')]

# Print the value of Offloaded Filename where there is a "?" in Time In or Time Out
if not rows_with_question_mark.empty:
    print("!!!!!WARNING CHECK!!!!!!:"
           "'?' file row", rows_with_question_mark['Offloaded Filename'].values[0],
           "May want to process file separately")


#%% Convert back to datetime format
# Convert Time In and Time Out columns to datetime format
filtered_deployment_df['Time In'] = pd.to_datetime(filtered_deployment_df['Time In'], format='%H:%M:%S', errors='coerce')
filtered_deployment_df['Time Out'] = pd.to_datetime(filtered_deployment_df['Time Out'], format='%H:%M:%S', errors='coerce')

#%% Convert Date In and Date Out columns to datetime format
filtered_deployment_df['Date In'] = pd.to_datetime(filtered_deployment_df['Date In'])
filtered_deployment_df['Date Out'] = pd.to_datetime(filtered_deployment_df['Date Out'])



print(filtered_deployment_df.dtypes)  # Check the data types after conversion


# %%
# Combine the date and time columns for Date In Time In
filtered_deployment_df['Date In Time In'] = pd.to_datetime(filtered_deployment_df['Date In'].astype(str) + ' ' + filtered_deployment_df['Time In'].astype(str))

# Combine the date and time columns for Date Out Time Out
filtered_deployment_df['Date Out Time Out'] = pd.to_datetime(filtered_deployment_df['Date Out'].astype(str) + ' ' + filtered_deployment_df['Time Out'].astype(str))

# Drop the separate Date In, Time In, Date Out, and Time Out columns if needed
#filtered_deployment_df.drop(columns=['Date In', 'Time In', 'Date Out', 'Time Out'], inplace=True)

print(filtered_deployment_df)

print(filtered_deployment_df['Date In Time In'], filtered_deployment_df['Date Out Time Out'])

# %% [markdown]
# ### Problematic rows
# If the above cell block returns and error, this means that there are indiscrepencies in the data, likely the deployment log. Input the postion number in the code below that the error message states and it will tell you where the error occurs in the data.
# <b>Once the above code runs without an error, then you don't need to run this cell!

#problematic_row = filtered_deployment_df.iloc[0] #position number
#print(problematic_row)

# %%
'''
DEPLOYMENT DATA DICTIONARY
'''
#%% Make a dictonary for each record contained in the filtered_deployment_df

# Create a dictionary to hold entries based on Offloaded Filename
deployment_data_dict = {}

# Iterate through DataFrame rows
for index, row in filtered_deployment_df.iterrows():
    file_info = {
        'Date In': row['Date In'], #From here
        'Time In': row['Time In'],
        'Date Full': row['Date Full'],
        'Date Out': row['Date Out'],
        'Time Out': row['Time Out'], #To here would need to be commented out if dropped in previous cell
        'Date In Time In': row['Date In Time In'],
        'Date Out Time Out': row['Date Out Time Out'],
        'Offloaded Filename': row['Offloaded Filename']
    }
    # Append file info to the dictionary using Offloaded Filename as key
    deployment_data_dict[row['Offloaded Filename']] = file_info

# Print the created dictionary
for filename, file_info in deployment_data_dict.items():
    print(f"Offloaded Filename: {filename}")
    print(f"File Info: {file_info}")

# %%
'''
MATCH DATETIMES AND PLOT PRE TRIMMED
'''
#%% Change Date Times in deployment_data_dict to match the times in df_files

# Iterate through deployment_data_dict
for filename, file_info in deployment_data_dict.items():
    # Convert 'Date In Time Out' and 'Date Out Time Out' to the specified format
    file_info['Date In Time In'] = file_info['Date In Time In'].strftime('%m/%d/%y %H:%M:%S')
    file_info['Date Out Time Out'] = file_info['Date Out Time Out'].strftime('%m/%d/%y %H:%M:%S')

# Now the values for 'Date In Time Out' and 'Date Out Time Out' are in the specified time format

# %%
# Loop through df_files and plot graphs for each DataFrame
for site_code, site_data in df_files.items():
    print({site_code})
    for file_number, file_data in site_data.items():
        for file_identifier, file_info in file_data.items():
            # Get the DataFrame
            df = file_info['DataFrame']
            
            if site_code in panama_codes:
                date_column = "Date Time, GMT-05:00"
            else:
                date_column = "Date Time, GMT-04:00"
            
            print(df[date_column].dtype)
            print(df[date_column].head())

            # Convert the date column to datetime objects, specifying the format
            df[date_column] = pd.to_datetime(df[date_column], format='%m/%d/%y %H:%M:%S')

            # Plot the temperature over time
            plt.figure(figsize=(12, 6))
            plt.plot(df[date_column], df['Temp, °C'], color='blue', marker='o', linestyle='-')
            plt.title(f'Temperature Over Time for {site_code}_{file_number}_{file_identifier}')
            plt.xlabel('Date Time')
            plt.ylabel('Temp, °C')  # Modified label
            plt.grid(True)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()


# %%
'''
TRIMMING
'''
#%% Trim Data Based on Date Time
# The data is being trimmed to the closest related start and endpoint 

# Convert timestamps to datetime objects in deployment_data_dict
for filename, file_info in deployment_data_dict.items():
    # Check if the values are already strings
    if isinstance(file_info['Date In Time In'], str):
        file_info['Date In Time In'] = datetime.strptime(file_info['Date In Time In'], '%m/%d/%y %H:%M:%S')
    if isinstance(file_info['Date Out Time Out'], str):
        file_info['Date Out Time Out'] = datetime.strptime(file_info['Date Out Time Out'], '%m/%d/%y %H:%M:%S')


# %%
# Trim the data in each DataFrame based on the specified time range
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        for file_identifier, file_info in file_data.items():
            df = file_info['DataFrame']
            
            if site_code in panama_codes:
                date_column = "Date Time, GMT-05:00"
            else:
                date_column = "Date Time, GMT-04:00"
            
            # Convert the date column to datetime objects, specifying the format
            df[date_column] = pd.to_datetime(df[date_column], format='%m/%d/%y %H:%M:%S')
            
            # Filter the DataFrame based on the specified time range
            df = df[(df[date_column] >= deployment_data_dict[file_info['File Name']]['Date In Time In']) &
                    (df[date_column] <= deployment_data_dict[file_info['File Name']]['Date Out Time Out'])]
            
            # Update the DataFrame in df_files
            df_files[site_code][file_number][file_identifier]['DataFrame'] = df

# %%
# Loop through each site code, file number, and file identifier in df_files
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        for file_identifier, file_info in file_data.items():
            # Get the DataFrame for the current file
            df = file_info['DataFrame']
            
            # Reduce the number of start points 4 and end points by 5 on each end of the DataFrame
            trimmed_df = df.iloc[4:-5]
            
            # Update the DataFrame in df_files
            df_files[site_code][file_number][file_identifier]['DataFrame'] = trimmed_df

# %%
'''
LENTGHS CHECKING
'''
#%% Checks to see if the "a" and "b" files and the "c" and "d" files have the same number of data points

# Iterate through each site code
for site_code, file_numbers in df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        # Check if there are multiple file identifiers for the file number
        if len(identifiers) > 1:
            # Get the number of rows for each DataFrame
            num_rows = {identifier: info['DataFrame'].shape[0] for identifier, info in identifiers.items()}
            
            # Check if files associated with the file number have different numbers of data points
            if len(set(num_rows.values())) != 1:
                print(f"Site code: {site_code}, File number: {file_number} have files with different numbers of data points:")
                for identifier, count in num_rows.items():
                    print(f"  - {identifier}: {count} data points")
            else:
                print(f"Site code: {site_code}, File number: {file_number} have files with the same number of data points: {next(iter(num_rows.values()))}.")


# %%
'''
CALCULATIONS CHECK (ONLY FOR DUPLICATE FILES)
'''
#%% Check to see if there is a .2 degrees difference then average the two columns and record the site codes where this occured 
# Also take the average between the two temperature columns if the difference is less than or equal to .2 degrees.
# Use the new average temperature column as the temperature column 
# Iterate through each site code
for site_code, file_numbers in df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        # Check if there are both 'a' and 'b' files for the current site code and file number
        if 'a' in identifiers and 'b' in identifiers:
            # Get the 'a' and 'b' dataframes
            df_a = identifiers['a']['DataFrame']
            df_b = identifiers['b']['DataFrame']
            
            # Check if the temperature columns exist in both dataframes
            if 'Temp, °C' in df_a.columns and 'Temp, °C' in df_b.columns:
                # Calculate the temperature difference
                df_a['Temperature_Difference'] = (df_a['Temp, °C'] - df_b['Temp, °C'])
            else:
                print(f"Temperature columns not found for Site: {site_code}, File Number: {file_number}")
        else:
            print(f"Only one file for Site: {site_code}, File Number: {file_number}, so averaging could not occur")




# %%
'''
CALCULATIONS
'''
#%%
# Create an empty dictionary to store calculations
calculations = {}

# Iterate through each site code
for site_code, file_numbers in df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        # Check if the 'a' identifier exists for the current site code and file number
        if 'a' in identifiers:
            # Get the 'a' dataframe
            df_a = identifiers['a']['DataFrame']
            
            # Check if the 'Temperature_Difference' column exists in the dataframe
            if 'Temperature_Difference' in df_a.columns:
                # Check for Temperature_Difference > 0.4
                high_difference = df_a[df_a['Temperature_Difference'] > 0.4]
                
                # Check for Temperature_Difference > 0.2 with flag count > 68
                moderate_difference = df_a[df_a['Temperature_Difference'] > 0.2]
                
                # If either condition is met, add to calculations
                if not high_difference.empty or len(moderate_difference) >= 68:
                    # Get the file name associated with the 'a' dataframe
                    file_name = identifiers['a']['File Name']
                    
                    # Store site code, file number, and file name in the calculations dictionary
                    calculations[(site_code, file_number)] = file_name
            #else:
            #    print(f"Temperature_Difference column not found for Site: {site_code}, File Number: {file_number}")

print()
print('These are the files that need to be labeled as calculations:')
for key, value in calculations.items():
    print(key, value)


#%%
'''
CALCULATION COMPARISONS
'''
# %%
calc_df = df_files

# %%
# Initialize an empty dictionary to store DataFrames corresponding to calculations
calc_df_files = {}

# Iterate through each key-value pair in the calculations dictionary
for (site_code, file_number), file_name in calculations.items():
    # Check if the site code exists in calc_df dictionary
    if site_code in calc_df:
        # Check if the file number exists for the site code
        if file_number in calc_df[site_code]:
            for identifier in ['a', 'b']:  # Iterate through both 'a' and 'b' identifiers
                # Extract the DataFrame corresponding to the identifier from df_files dictionary
                df = calc_df[site_code][file_number].get(identifier)
                if df is not None:
                    # Create a nested dictionary entry in calc_df_files
                    if site_code not in calc_df_files:
                        calc_df_files[site_code] = {}
                    if file_number not in calc_df_files[site_code]:
                        calc_df_files[site_code][file_number] = {}
                    calc_df_files[site_code][file_number][identifier] = df
        else:
            print(f"File number {file_number} not found for site code {site_code} in calc_df.")
    else:
        print(f"Site code {site_code} not found in calc_df.")

# Now, calc_df_files will contain the DataFrames corresponding to the files listed in the calculations dictionary, including both 'a' and 'b' identifiers.

# %% COMPARISON COLUMNS
# Iterate through each site code
for site_code, file_numbers in calc_df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        if 'a' in identifiers:
            # Get the dataframe calc_a
            calc_a = identifiers['a']['DataFrame']
            calc_b = identifiers['b']['DataFrame']
            
            # Add the temp column from b to a and rename both
            calc_a["Temp A"] = calc_a.loc[:, "Temp, °C"]
            calc_a["Temp B"] = calc_b.loc[:, "Temp, °C"]
            
            #Add the average column
            calc_a["Average_temp"] = (calc_a['Temp A'] + calc_a['Temp B'])/2

            # Add a flag column with "FLAG" if the condition is True, else blank
            calc_a['Flag'] = calc_a["Temperature_Difference"].apply(lambda x: "FLAG" if x > 0.2 else "")
            

            # Print calc_a
            print(f"DataFrame for Site Code: {site_code}, File Number: {file_number} (calc_a)")
            print(calc_a)
            print("\n")

# %% 
'''
TRUE FLAGS
'''
# %%
# Iterate through each site code
for site_code, file_numbers in calc_df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        if 'a' in identifiers:
            # Get the dataframe calc_a
            calc_a = identifiers['a']['DataFrame']
            
            # Count the number of 'True' values in the 'Flag' column
            true_count = calc_a['Flag'].value_counts().get('FLAG', 0)
            
            # Initialize variables for temperature difference reporting
            temp_diff_count = 0
            temp_diff_values = []
            
            # Check if the 'Temperature_Difference' column exists
            if 'Temperature_Difference' in calc_a.columns:
                # Get the count of Temperature_Difference > 0.4
                temp_diff_count = (calc_a['Temperature_Difference'] > 0.4).sum()
                
                # Extract and round the values of Temperature_Difference > 0.4
                temp_diff_values = calc_a.loc[
                    calc_a['Temperature_Difference'] > 0.4, 'Temperature_Difference'
                ].round(4).tolist()
            
            # Print the results
            print(f"{site_code} {file_number}, Number of 'True' values flagged: {true_count}, Temperature Difference > 0.4 Count: {temp_diff_count}")
            if temp_diff_values:
                print(f"  Values of Temperature Difference > 0.4: {temp_diff_values}")

# %%
'''
OFFLOAD CALCULATIONS
'''

# %%

# Iterate through each site code
for site_code, file_numbers in calc_df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        # Check if 'a' identifier exists
        if 'a' in identifiers:
            # Get the dataframe calc_a
            calc_a = identifiers['a']['DataFrame']
            
            # Get the year and month of the first and last data points
            first_data_point = calc_a['Date Time, GMT-04:00'].iloc[0]
            last_data_point = calc_a['Date Time, GMT-04:00'].iloc[-1]
            year_month_first = first_data_point.strftime("%y%m")  # Using last two digits of the year
            year_month_last = last_data_point.strftime("%y%m")    # Using last two digits of the year
            
            # Construct the base file name
            base_file_name = f"BT_{site_code}_{year_month_first}_{year_month_last}"
            
            # Construct the output file name for 'a' file
            output_file_name = f"PD_{base_file_name}.csv"
            output_file_path = os.path.join(PD_folder_path, output_file_name)
            
            # Drop unnecessary columns from calc_a
            calc_a = calc_a[['#', 'Date Time, GMT-04:00', 'Temp A', 'Temp B', 'Temperature_Difference','Average_temp','Flag']]
            
            # rename GMT to UTC
            calc_a.rename(columns={'Date Time, GMT-04:00': 'Date Time, UTC-04:00'}, inplace=True)

            # Save the 'a' DataFrame to CSV
            calc_a.to_csv(output_file_path, index=False)
            
            print(f"File saved: Site: {site_code}, File Number: {file_number}, Path: {output_file_path}")
        else:
            print(f"No 'a' version found for Site: {site_code}, File Number: {file_number}")


# %%
'''
AVERAGING AND NAN FLAGS
'''
#%% Check to see if there is a .2 degrees difference then average the two columns and record the site codes where this occured 
# Also take the average between the two temperature columns if the difference is less than or equal to .2 degrees.
# Use the new average temperature column as the temperature column 
# Iterate through each site code
for site_code, file_numbers in df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        # Check if there are both 'a' and 'b' files for the current site code and file number
        if 'a' in identifiers and 'b' in identifiers:
            # Get the 'a' and 'b' dataframes
            df_a = identifiers['a']['DataFrame']
            df_b = identifiers['b']['DataFrame']
            
            # Check if the temperature columns exist in both dataframes
            if 'Temp, °C' in df_a.columns and 'Temp, °C' in df_b.columns:
                # Calculate the average temperature between the two temperature columns of "a" and "b" if the temperature difference is .2 or below
                df_a['Average_Temperature'] = df_a.apply(
                    lambda row: (row['Temp, °C'] + df_b.loc[row.name,'Temp, °C']) / 2 if row['Temperature_Difference'] <= 0.2 else None,
                    axis=1
                )
                # Drop the old 'Temp, °C' column to replace with new average column
                df_a.drop(columns=['Temp, °C'], inplace=True)
                
                # Rename the Average Temperature column
                df_a.rename(columns={'Average_Temperature': 'Temp, °C'}, inplace=True)
                
            else:
                print(f"Temperature columns not found for Site: {site_code}, File Number: {file_number}")
        else:
            print(f"Only one file for Site: {site_code}, File Number: {file_number}, so averaging could not occur")


#%% Print the number of NaNs to show the number of points that are above a .2 temperature difference
# The number of NaNs represents the number of points where the temperature difference was above .2

# Iterate through each file in calculations
for (site_code, file_number), file_name in calculations.items():
    # Get the 'a' dataframe associated with the file
    df_a = df_files[site_code][file_number]['a']['DataFrame']

    # Check if the dataframe and the 'Temp, °C' column exist
    if df_a is not None and 'Temp, °C' in df_a.columns:
        # Count the number of NaNs in the 'Temp, °C column
        nan_count = df_a['Temp, °C'].isna().sum()

        # Print the file details and the NaN count
        print(f"For file {file_name}") 
        print(f"{nan_count} is the number of points where the temperature difference was above .2°C")
        print()


# %%
'''
DROP COLUMNS
'''
#%% Drop columns
# Iterate through each site code
for site_code, file_numbers in df_files.items():
    # Determine the correct date column based on whether the site is in Panama codes
    if site_code in panama_codes:
        date_col = 'Date Time, GMT-05:00'
    else:
        date_col = 'Date Time, GMT-04:00'

    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        # Check if there are both 'a' and 'b' files for the current site code and file number
        if 'a' in identifiers:
            # Get the 'a' dataframe
            df_a = identifiers['a']['DataFrame']

            # Drop all columns except '#', date_col, and 'Temp, °C'
            columns_to_keep = ['#', date_col, 'Temp, °C']
            df_a = df_a[columns_to_keep]
            # Rename columns if needed (uncomment the next line if desired)
            # df_a = df_a.rename(columns={'#': 'number', date_col: 'Date Time', 'Temp, °C': 'Temp C'})

            # Update the dataframe in df_files
            df_files[site_code][file_number]['a']['DataFrame'] = df_a

            if 'b' in identifiers:
                # Get the 'b' dataframe
                df_b = identifiers['b']['DataFrame']

                # Drop all columns except '#', date_col, and 'Temp, °C'
                df_b = df_b[columns_to_keep]

                # Update the dataframe in df_files
                df_files[site_code][file_number]['b']['DataFrame'] = df_b

                if 'c' in identifiers:
                    # Get the 'c' dataframe
                    df_c = identifiers['c']['DataFrame']

                    # Drop all columns except '#', date_col, and 'Temp, °C'
                    df_c = df_c[columns_to_keep]

                    # Update the dataframe in df_files
                    df_files[site_code][file_number]['c']['DataFrame'] = df_c
                    
                    if 'd' in identifiers:
                        # Get the 'd' dataframe
                        df_d = identifiers['d']['DataFrame']

                        # Drop all columns except '#', date_col, and 'Temp, °C'
                        df_d = df_d[columns_to_keep]

                        # Update the dataframe in df_files
                        df_files[site_code][file_number]['d']['DataFrame'] = df_d

# %%
'''
PLOTTING (AVERAGED OR SINGLE FILES)
'''

#%% Individual plots

#Get the DataFrame
# df = df_files['TCBKIT']['2404']['a']['DataFrame']

# # # Convert the 'Date Time, GMT-04:00' column to datetime format
# df['Date Time, GMT-04:00'] = pd.to_datetime(df['Date Time, GMT-04:00'])


# # # Plot the temperature over time
# plt.figure(figsize=(12, 6))
# plt.plot(df['Date Time, GMT-04:00'], df['Temp, °C'], color='blue', marker='o', linestyle='-')
# plt.title('Temperature Over Time')
# plt.xlabel('Date Time')
# plt.ylabel('Temp, °C')  # Modified label
# plt.grid(True)
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.show()


#%% Loop through df_files and plot graphs for each DataFrame where time is displayed as months

# Loop through df_files and plot graphs for each DataFrame
for site_code, site_data in df_files.items():
    # Determine the correct date column based on whether the site is in Panama codes
    if site_code in panama_codes:
        date_col = 'Date Time, GMT-05:00'
    else:
        date_col = 'Date Time, GMT-04:00'

    for file_number, file_data in site_data.items():
        for file_identifier, file_info in file_data.items():
            # Get the DataFrame
            df = file_info['DataFrame']

            # Convert the date column to datetime format
            df.loc[:, date_col] = pd.to_datetime(df[date_col])

            # Plot the temperature over time
            plt.figure(figsize=(12, 6))
            plt.plot(df[date_col], df['Temp, °C'], color='blue', marker='o', linestyle='-')
            plt.title(f'Temperature Over Time - Site: {site_code}, File Number: {file_number}, Identifier: {file_identifier}')
            plt.xlabel('Date Time')
            plt.ylabel('Temp, °C')
            plt.grid(True)
            
            # Set x-axis ticks to display month names at regular intervals
            # first_day_of_month_indices = df.index[df[date_col].dt.day == 1]
            # plt.xticks(first_day_of_month_indices, [dt.strftime('%b') for dt in df.loc[first_day_of_month_indices, date_col]], rotation=45, fontdict={'family': 'sans-serif', 'size': 25, 'style': 'normal'})

            plt.tight_layout()
            plt.show()

#%%
'''
ADDITIONAL TRIMMING
'''
#%%
def trim_dataframe(df_files, site_code, file_number, file_identifier, panama_codes, cutoff_datetime):
    # Check if the file identifier exists for the specified site code and file number
    if file_identifier not in df_files[site_code][file_number]:
        raise KeyError(f"'{file_identifier}' not found for site code '{site_code}' and file number '{file_number}'")
    
    # Access the DataFrame using the site_code, file_number, and file_identifier
    df = df_files[site_code][file_number][file_identifier]['DataFrame']
    
    # Determine the correct date column based on the site code
    date_col = 'Date Time, GMT-05:00' if site_code in panama_codes else 'Date Time, GMT-04:00'
    
    # Trim the DataFrame based on the cutoff datetime
    # > for front end. < for back end.
    trimmed_df = df[df[date_col] <= cutoff_datetime]
    
    # Assign the trimmed DataFrame back to the dictionary
    df_files[site_code][file_number][file_identifier]['DataFrame'] = trimmed_df

cutoff_datetime = pd.Timestamp('2025-04-16 16:45:00')

# Pass the file number (e.g., '001') and file identifier (e.g., 'c') to access the DataFrame
trim_dataframe(df_files, 'TCCB67', '2311', 'c', panama_codes, cutoff_datetime)


# %%
'''
OFFSET FILES MERGE
'''
#%%
# Initialize the dictionary for storing merged DataFrames
merged_dict = {}

# Iterate through each site code
for site_code, file_numbers in df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        # Check if 'c' and 'd' identifiers exist
        if 'c' in identifiers and 'd' in identifiers:
            df_c = identifiers['c']['DataFrame']
            df_d = identifiers['d']['DataFrame']

            # Determine correct datetime column
            if site_code in panama_codes:
                date_col = "Date Time, GMT-05:00"
            else:
                date_col = "Date Time, GMT-04:00"

            # Skip if expected datetime column is missing
            if date_col not in df_c.columns or date_col not in df_d.columns:
                print(f"Skipping merge for {site_code}, file {file_number}: Missing expected datetime column.")
                continue

            # Convert datetime columns to datetime type
            df_c[date_col] = pd.to_datetime(df_c[date_col], errors='coerce')
            df_d[date_col] = pd.to_datetime(df_d[date_col], errors='coerce')

            # Rename Temp columns
            df_c = df_c[['#', date_col, 'Temp, °C']].rename(columns={'Temp, °C': 'Temp_c'})
            df_d = df_d[['#', date_col, 'Temp, °C']].rename(columns={'Temp, °C': 'Temp_d'})

            # Merge on datetime column
            merged_df = pd.merge(df_c, df_d, on=date_col, how='outer', suffixes=('_c', '_d'))

            # Combine temp columns
            merged_df['Temp, °C'] = merged_df['Temp_c'].combine_first(merged_df['Temp_d'])

            # Merge hash column variations
            if '#_c' in merged_df.columns and '#_d' in merged_df.columns:
                merged_df['#'] = merged_df['#_c'].combine_first(merged_df['#_d'])
                merged_df = merged_df.drop(columns=['#_c', '#_d'])
            elif '#_c' in merged_df.columns:
                merged_df['#'] = merged_df['#_c']
                merged_df = merged_df.drop(columns=['#_c'])
            elif '#_d' in merged_df.columns:
                merged_df['#'] = merged_df['#_d']
                merged_df = merged_df.drop(columns=['#_d'])
            elif '#_x' in merged_df.columns and '#_y' in merged_df.columns:
                merged_df['#'] = merged_df['#_x'].combine_first(merged_df['#_y'])
                merged_df = merged_df.drop(columns=['#_x', '#_y'])
            elif '#_x' in merged_df.columns:
                merged_df['#'] = merged_df['#_x']
                merged_df = merged_df.drop(columns=['#_x'])
            elif '#_y' in merged_df.columns:
                merged_df['#'] = merged_df['#_y']
                merged_df = merged_df.drop(columns=['#_y'])

            # Final cleanup
            merged_df = merged_df[['#', date_col, 'Temp, °C']]

            # Store the result
            merged_dict[site_code] = merged_df
            print(f"Merged Data for Site: {site_code}, File Number: {file_number}")

# Loop through the merged_dict and reassign the '#' column to be sequential
for site_code, merged_df in merged_dict.items():
    # Convert to numeric in case of any string values or NaNs
    merged_df['#'] = pd.to_numeric(merged_df['#'], errors='coerce')

    # Drop any NaN values that might have been introduced
    merged_df = merged_df.dropna(subset=['#']).reset_index(drop=True)

    # Ensure we start numbering from the first valid value
    start_value = int(merged_df['#'].iloc[0]) if not merged_df.empty else 1
    
    # Assign sequential numbering starting from start_value
    merged_df['#'] = range(start_value, start_value + len(merged_df))
    
    # Update the dictionary
    merged_dict[site_code] = merged_df

    print(f"Updated '#' column for Site: {site_code}")

#%%
'''
OFFSET PLOT COMPARISONS
'''
#%% AGREEMENT PLOTS
# This code loops through the c and d files and makes agreement plots for them. If the number of points above and below for blue (c) 
# doesn't match the number of points below and above for red (d) then there is a drift between logger measurements

# Create a deep copy of df_files to avoid modifying the original data
offset_compare = copy.deepcopy(df_files)

# Loop through offset_compare dictionary
for site_code, site_data in offset_compare.items():
    # Determine the correct date column based on whether the site is in Panama codes
    if site_code in panama_codes:
        date_col = 'Date Time, GMT-05:00'
    else:
        date_col = 'Date Time, GMT-04:00'

    for file_number, file_data in site_data.items():
        df_c = None
        df_d = None

        # Extract c and d dataframes
        for file_identifier, file_info in file_data.items():
            df = file_info['DataFrame']
            df[date_col] = pd.to_datetime(df[date_col])  # Use dynamic date column

            if file_identifier == 'c':
                df_c = df
            elif file_identifier == 'd':
                df_d = df

        if df_c is not None and df_d is not None:
            # Trim to shortest length
            min_len = min(len(df_c), len(df_d))
            temp_c = df_c['Temp, °C'].iloc[:min_len].reset_index(drop=True)
            temp_d = df_d['Temp, °C'].iloc[:min_len].reset_index(drop=True)
            time = df_c[date_col].iloc[:min_len].reset_index(drop=True)

            # Initialize counters for points above and below the 1:1 line
            blue_above = 0
            blue_below = 0
            red_above = 0
            red_below = 0

            # Loop through the points and compare with the 1:1 line
            for c_temp, d_temp in zip(temp_c, temp_d):
                if c_temp > d_temp:  # Blue point above 1:1 line
                    blue_above += 1
                elif c_temp < d_temp:  # Blue point below 1:1 line
                    blue_below += 1
                
                if d_temp > c_temp:  # Red point above 1:1 line
                    red_above += 1
                elif d_temp < c_temp:  # Red point below 1:1 line
                    red_below += 1

            # Print the counts
            print(f"Site: {site_code}, File Number: {file_number}")
            print(f"  Blue points above 1:1 line: {blue_above}")
            print(f"  Blue points below 1:1 line: {blue_below}")
            print(f"  Red points above 1:1 line: {red_above}")
            print(f"  Red points below 1:1 line: {red_below}")

            # Create scatter plot
            plt.figure(figsize=(8, 8))

            # Scatter plot for logger 'c' in blue
            plt.scatter(temp_c, temp_d, c='blue', s=10, alpha=0.7, label='Logger c')

            # Scatter plot for logger 'd' in red
            plt.scatter(temp_d, temp_c, c='red', s=10, alpha=0.7, label='Logger d')

            # Add 1:1 reference line
            min_temp = min(temp_c.min(), temp_d.min())
            max_temp = max(temp_c.max(), temp_d.max())
            plt.plot([min_temp, max_temp], [min_temp, max_temp], color='black', linestyle='--', label='1:1 Line')

            # Labels and title
            plt.xlabel('Logger c Temp (°C)')
            plt.ylabel('Logger d Temp (°C)')
            plt.title(f'Temperature Agreement Over Time - Site: {site_code}, File: {file_number}')
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            plt.show()


#%%
'''
PLOTTING (MERGED)
'''

#%% Loop through merged_dict and plot graphs for each DataFrame where time is displayed as months
# Loop through each site in merged_dict
for site_code, merged_df in merged_dict.items():
    # Determine the correct date column based on whether the site is in Panama codes
    if site_code in panama_codes:
        date_col = 'Date Time, GMT-05:00'
    else:
        date_col = 'Date Time, GMT-04:00'
    
    # Convert the Date column to datetime format
    merged_df[date_col] = pd.to_datetime(merged_df[date_col])
    
    # Create the plot
    plt.figure(figsize=(10, 5))
    plt.plot(merged_df[date_col], merged_df['Temp, °C'], label=f'Site {site_code}', color='b')
    
    # Formatting
    plt.xlabel('Month')
    plt.ylabel('Temperature (°C)')
    plt.title(f'Temperature Time Series for {site_code}')
    plt.xticks(rotation=45)
    
    # Set x-axis to show month ticks
    plt.gca().xaxis.set_major_locator(plt.matplotlib.dates.MonthLocator())  # Show month ticks
    plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%b %Y'))  # Format as "Jan 2024"
    
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    # Show or save the plot
    plt.show()  # Change to plt.savefig(f"{site_code}_temp_plot.png") to save


# %%
'''
START AND END TIME CHECKS
'''
# # %% Individual start time end time checks 
# print("DataFrame Start Time:", df_files['TCBKIT']['2310']['a']['DataFrame']['Date Time, GMT-04:00'].iloc[0])
# print("Deployment Data Start Time:", deployment_data_dict['BT_TCBKIT_2310_']['Date In Time In'])

# # Print end time
# print("DataFrame End Time:", df_files['TCBKIT']['2310']['a']['DataFrame']['Date Time, GMT-04:00'].iloc[-1])
# print("Deployment Data End Time:", deployment_data_dict['BT_TCBKIT_2310_']['Date Out Time Out'])

# # Then, check if the extracted timestamps match the expected start and end times.

# %% Loop for start time end time checks
# List to store the names of empty DataFrames
empty_dataframes = []

# Loop through each site code, file number, and file identifier in df_files
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        for file_identifier, file_info in file_data.items():
            # Get the DataFrame for the current file
            df = file_info['DataFrame']
            
            # Determine the correct date column based on the time zone for the site
            if site_code in panama_codes:
                date_col = 'Date Time, GMT-05:00'
            else:
                date_col = 'Date Time, GMT-04:00'
            
            if not df.empty:  # Check if the DataFrame is not empty
                # Print the start and end times for the current DataFrame
                print(f"DataFrame Start Time ({site_code}_{file_number}_{file_identifier}):", df[date_col].iloc[0])
                print(f"DataFrame End Time ({site_code}_{file_number}_{file_identifier}):", df[date_col].iloc[-1])

                # Get the corresponding offloaded file name
                offloaded_file_name = file_info['File Name']
                
                # Retrieve deployment data using the offloaded file name
                deployment_data = deployment_data_dict.get(offloaded_file_name)
                
                if deployment_data:
                    # Print the start and end times from deployment data
                    print(f"Deployment Data Start Time ({offloaded_file_name}):", deployment_data['Date In Time In'])
                    print(f"Deployment Data End Time ({offloaded_file_name}):", deployment_data['Date Out Time Out'])
                else:
                    print(f"No deployment data found for {offloaded_file_name}")

                print()  # Add an empty line for better readability
            else:
                # If DataFrame is empty, append its name to the list
                empty_dataframes.append(f"{site_code}_{file_number}_{file_identifier}")

# Print names of empty DataFrames
if empty_dataframes:
    print("Names of empty DataFrames:")
    for name in empty_dataframes:
        print(name)
else:
    print("No empty DataFrames found.")


#%%
'''
TRIMMED FILE DATES (HELPS FOR METADATA)
'''
# %%
# Iterate through df_files to extract and save the "a" version of the files
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        if 'a' in file_data:  # Check if "a" version exists for the file
            # Extract the DataFrame for the "a" version
            df_a = file_data['a']['DataFrame']
            
            # Determine the correct date column based on the time zone for the site
            if site_code in panama_codes:
                date_col = 'Date Time, GMT-05:00'
            else:
                date_col = 'Date Time, GMT-04:00'
            
            # Convert the 'Date Time' column to datetime, based on the correct timezone
            df_a[date_col] = pd.to_datetime(df_a[date_col])
            
            # Get the year and month of the first and last data points
            first_data_point = df_a[date_col].iloc[0]
            last_data_point = df_a[date_col].iloc[-1]
            year_month_first = first_data_point.strftime("%y %m %d")  # Using last two digits of the year
            year_month_last = last_data_point.strftime("%y %m %d")    # Using last two digits of the year
            
            # Construct the base file name
            base_file_name = f"BT_{site_code}_{year_month_first}_{year_month_last}"
            
            # Check if the file is identified as a calculation file
            if (site_code, file_number) in calculations:
                output_file_name = f"{base_file_name}_calculations.csv"
            else:
                output_file_name = f"{base_file_name}.csv"
            
            print(f"Saving file: {output_file_name}")
            # Code to save the DataFrame, e.g. df_a.to_csv(output_file_name)

# Iterate through merged_dict to extract and save the DataFrames
for site_code, df in merged_dict.items():  # 'df' is directly the merged DataFrame
    # Determine the correct date column based on the time zone for the site
    if site_code in panama_codes:
        date_col = 'Date Time, GMT-05:00'
    else:
        date_col = 'Date Time, GMT-04:00'
    
    # Convert the 'Date Time' column to datetime, based on the correct timezone
    df[date_col] = pd.to_datetime(df[date_col])
    
    # Get the year and month of the first and last data points
    first_data_point = df[date_col].iloc[0]
    last_data_point = df[date_col].iloc[-1]
    year_month_first = first_data_point.strftime("%y %m %d")  # Using last two digits of the year
    year_month_last = last_data_point.strftime("%y %m %d")    # Using last two digits of the year
    
    # Construct the base file name
    base_file_name = f"BT_{site_code}_{year_month_first}_{year_month_last}"
    
    # Print the output file name (or save it accordingly)
    print(f"Saving file: {base_file_name}.csv")



#%%
'''
OFFLOAD FILES
'''

# %% [markdown]
# ### Offload loop: a and merged data
# The code below is to test for files that were named using 'a' or 'merged' identifiers. It will loop through the .csv files and export them if they match those identifiers.
#%%

# Iterate through df_files to extract and save the "a" version of the files
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        if 'a' in file_data:  # Check if "a" version exists for the file
            # Extract the DataFrame for the "a" version
            df_a = file_data['a']['DataFrame']
            
            # Determine the correct date column based on the time zone for the site
            if site_code in panama_codes:
                # If it's a Panama site, use GMT-05:00
                date_col = 'Date Time, GMT-05:00'
                new_date_col = 'Date Time, UTC-05:00'
            else:
                # Otherwise, use GMT-04:00
                date_col = 'Date Time, GMT-04:00'
                new_date_col = 'Date Time, UTC-04:00'
            
            # Rename the Average Temperature column
            df_a.rename(columns={'Temp, °C': 'Temperature'}, inplace=True)

            # Check if the date column exists before renaming
            if date_col in df_a.columns:
                df_a.rename(columns={date_col: new_date_col}, inplace=True)
            else:
                print(f"Warning: {date_col} not found in DataFrame for {site_code}. Skipping rename.")

            # Ensure the column is renamed properly
            if new_date_col not in df_a.columns:
                print(f"Error: {new_date_col} column does not exist after renaming for {site_code}.")
                continue  # Skip this file if the rename failed
            
            # Get the year and month of the first and last data points
            first_data_point = df_a[new_date_col].iloc[0]
            last_data_point = df_a[new_date_col].iloc[-1]
            year_month_first = first_data_point.strftime("%y%m")  # Using last two digits of the year
            year_month_last = last_data_point.strftime("%y%m")    # Using last two digits of the year
            
            # Construct the base file name
            base_file_name = f"BT_{site_code}_{year_month_first}_{year_month_last}"
            
            # set output path
            output_file_name = f"{base_file_name}.csv"
            output_file_path = os.path.join(output_folder, output_file_name)

            # Save the DataFrame to CSV
            df_a.to_csv(output_file_path, index=False)
            
            print(f"File saved: {output_file_path}")

# Iterate through merged_dict to save merged DataFrames with "_merged" suffix
for site_code, merged_df in merged_dict.items():
    
    # Determine the correct date column based on the time zone for the site
    if site_code in panama_codes:
        # If it's a Panama site, use GMT-05:00
        date_col = 'Date Time, GMT-05:00'
        new_date_col = 'Date Time, UTC-05:00'
    else:
        # Otherwise, use GMT-04:00
        date_col = 'Date Time, GMT-04:00'
        new_date_col = 'Date Time, UTC-04:00'
    
    # Rename the columns for the merged DataFrame
    merged_df.rename(columns={'Temp, °C': 'Temperature'}, inplace=True)

    # Check if the date column exists before renaming
    if date_col in merged_df.columns:
        merged_df.rename(columns={date_col: new_date_col}, inplace=True)
    else:
        print(f"Warning: {date_col} not found in DataFrame for {site_code}. Skipping rename.")
    
    # Ensure the column is renamed properly
    if new_date_col not in merged_df.columns:
        print(f"Error: {new_date_col} column does not exist after renaming for {site_code}.")
        continue  # Skip this file if the rename failed

    # Get the year and month of the first and last data points for merged data
    first_data_point = merged_df[new_date_col].iloc[0]
    last_data_point = merged_df[new_date_col].iloc[-1]
    year_month_first = first_data_point.strftime("%y%m")  # Using last two digits of the year
    year_month_last = last_data_point.strftime("%y%m")    # Using last two digits of the year
    
    # Construct the base file name
    base_file_name = f"BT_{site_code}_{year_month_first}_{year_month_last}_merged"
    
    # Define the output file path
    output_file_name = f"{base_file_name}.csv"
    output_file_path = os.path.join(output_folder, output_file_name)
    
    # Save the merged DataFrame to CSV
    merged_df.to_csv(output_file_path, index=False)
    
    print(f"Merged File saved: {output_file_path}")



# %% [markdown]
# ### Offloading plots:
# Code below reads in the newly offloaded csv files, converts the date time, and plots the temperature over time for each .csv file then exports them to a folder.
# <li><u>To save the plots, you will need to create a directory (a folder). Make sure to update that folder path in the save_dir = part of the cell!</u>

# %%
'''
OFFLOADING PLOTS
'''
#%% Using new offloaded files create plots and save plots to a folder

# Define pattern to match CSV files
file_pattern = '*.csv'

# Use glob to get a list of file paths matching the pattern set in file_pattern
exported_csv_files = glob.glob(output_folder + '/' + file_pattern)

# Loop through each CSV file
for csv_file in exported_csv_files:
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)
    
    # Determine the correct date column based on the time zone for the site
    site_code = os.path.basename(csv_file).split('_')[1]  # Assuming site code is in the second part of the file name
    
    if site_code in panama_codes:
        # If it's a Panama site, use UTC-05:00
        date_col = 'Date Time, UTC-05:00'
        new_date_col = 'Date Time, UTC-05:00'
    else:
        # Otherwise, use UTC-04:00
        date_col = 'Date Time, UTC-04:00'
        new_date_col = 'Date Time, UTC-04:00'
    
    # Check if the date column exists before renaming
    if date_col in df.columns:
        df.rename(columns={date_col: new_date_col}, inplace=True)
    else:
        print(f"Warning: {date_col} not found in DataFrame for {site_code}. Skipping rename.")

    # Ensure the column is renamed properly
    if new_date_col not in df.columns:
        print(f"Error: {new_date_col} column does not exist after renaming for {site_code}. Skipping this file.")
        continue  # Skip this file if the rename failed
    
    # Convert the 'Date Time, UTC-05:00' or 'Date Time, UTC-04:00' column to datetime format
    try:
        df[new_date_col] = pd.to_datetime(df[new_date_col], format='%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Error converting date for {site_code} in file {csv_file}: {e}")
        continue  # Skip this file if the datetime conversion fails

    # Plot the data
    plt.figure(figsize=(12, 6))
    plt.plot(df[new_date_col], df['Temperature'], color='blue', marker='o', linestyle='-')
    plt.title(f'Temperature Over Time for Site: {site_code}')
    plt.xlabel('Date Time')
    plt.ylabel('Temp, °C')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Extract file name from the file path
    file_name = os.path.basename(csv_file)
    
    # Define the file name for the plot
    plot_file_name = os.path.splitext(file_name)[0] + '_plot.png'
    
    # Save the plot
    plt.savefig(os.path.join(plots_path, plot_file_name))
    
    # Show the plot (optional)
    plt.show()


#%%
'''
START OF SECTION: IMPORT AND WRANGLE PD FILES
'''

#%%

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



    def save_offload_files(df_files, merged_dict, panama_codes, output_folder, calculations):
    """
    Saves 'a' version files and merged files as CSVs to the output_folder.
    Skips files that were already part of `calculations`.
    Adjusts date columns and temperature column names for saving.
    """
    # Save 'a' files
    for site_code, site_data in df_files.items():
        for file_number, file_data in site_data.items():
            # ⛔️ Skip if this file was already handled in calculations
            if (site_code, file_number) in calculations:
                continue

            if 'a' in file_data:
                df_a = file_data['a']['DataFrame'].copy()

                # Handle time zone
                if site_code in panama_codes:
                    date_col = 'Date Time, GMT-05:00'
                    new_date_col = 'Date Time, UTC-05:00'
                else:
                    date_col = 'Date Time, GMT-04:00'
                    new_date_col = 'Date Time, UTC-04:00'

                df_a.rename(columns={'Temp, °C': 'Temperature'}, inplace=True)
                if date_col in df_a.columns:
                    df_a.rename(columns={date_col: new_date_col}, inplace=True)
                else:
                    print(f"Warning: {date_col} not found in DataFrame for {site_code}. Skipping rename.")
                    continue

                if new_date_col not in df_a.columns:
                    print(f"Error: {new_date_col} column missing after renaming for {site_code}. Skipping file.")
                    continue

                first_date = df_a[new_date_col].iloc[0]
                last_date = df_a[new_date_col].iloc[-1]
                year_month_first = first_date.strftime("%y%m")
                year_month_last = last_date.strftime("%y%m")

                base_name = f"BT_{site_code}_{year_month_first}_{year_month_last}"
                filename = f"{base_name}.csv"
                filepath = os.path.join(output_folder, filename)

                df_a.to_csv(filepath, index=False)
                print(f"File saved: {filepath}")

    # Save merged files
    for site_code, merged_df in merged_dict.items():
        df = merged_df.copy()

        if site_code in panama_codes:
            date_col = 'Date Time, GMT-05:00'
            new_date_col = 'Date Time, UTC-05:00'
        else:
            date_col = 'Date Time, GMT-04:00'
            new_date_col = 'Date Time, UTC-04:00'

        df.rename(columns={'Temp, °C': 'Temperature'}, inplace=True)
        if date_col in df.columns:
            df.rename(columns={date_col: new_date_col}, inplace=True)
        else:
            print(f"Warning: {date_col} not found in DataFrame for {site_code}. Skipping rename.")
            continue

        if new_date_col not in df.columns:
            print(f"Error: {new_date_col} column missing after renaming for {site_code}. Skipping file.")
            continue

        first_date = df[new_date_col].iloc[0]
        last_date = df[new_date_col].iloc[-1]
        year_month_first = first_date.strftime("%y%m")
        year_month_last = last_date.strftime("%y%m")

        base_name = f"BT_{site_code}_{year_month_first}_{year_month_last}_merged"
        filename = f"{base_name}.csv"
        filepath = os.path.join(output_folder, filename)

        df.to_csv(filepath, index=False)
        print(f"Merged File saved: {filepath}")
