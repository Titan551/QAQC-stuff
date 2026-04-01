
#%% Imports
import os
import pandas as pd
import glob
import matplotlib.pyplot as plt
from datetime import datetime

#%% Get file paths
 
# Define folder path where your CSV files are located
folder_path = r'C:\UVI\QAQC stuff\Temp_TCRMP_2024_Working Folder'

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
              "TCSR67","TCSR10","TCSPTH"]
## NOTE: site code TCCB60 was not in the site code metadata

#%% Generate dataframes that can be called through a nested dictonary structure
# This creates a nested dictionary that can handle situations when site codes 
# have multiple different start times and multiple files i.e. 'a' and 'b' files

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

# Accessing the DataFrames by site code, file number, file identifier, and file name
# For example, to access the DataFrame and file name for site code 'TCBKPT', file number '2209', and file identifier 'a'
#if 'TCSR41' in df_files and '2210' in df_files['TCSR41'] and 'a' in df_files['TCSR41']['2210']:
    #print("File Name:", df_files['TCSR41']['2210']['a']['File Name'])
    #print("DataFrame:")
    #print(df_files['TCSR41']['2210']['a']['DataFrame'])
#else:
    #print("DataFrame not found for site code 'TCSR41', file number '2210', and file identifier 'a'")

#print(df_files)

# %%
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        if 'a' not in file_data:
            print(f"Warning: No 'a' identifier found for site code {site_code} and file number {file_number}. Check if there's a _b file and change it")
        else:
            file_name = file_data['a']['File Name']
            print(f"File with 'a' identifier found for site code {site_code} and file number {file_number}: {file_name}")



# %% Read in deployment log metadata sheet
deployment_df = pd.read_csv(r'C:\UVI\QAQC stuff\Temperature_UVI_deployment_log.csv')

# %% Filter deployment log metatdata sheet csv

# Extract file names from csv_files
csv_file_names = [os.path.basename(csv_file).split('.')[0] for csv_file in csv_files]

# Filter deployment_df to only include entries that match the file names in csv_files
filtered_deployment_df = deployment_df[deployment_df['Offloaded Filename'].isin(csv_file_names)]

# Print the filtered DataFrame
print(filtered_deployment_df)

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

#%%
# Combine the date and time columns for Date In Time In
filtered_deployment_df['Date In Time In'] = pd.to_datetime(filtered_deployment_df['Date In'].astype(str) + ' ' + filtered_deployment_df['Time In'].astype(str))

# Combine the date and time columns for Date Out Time Out
filtered_deployment_df['Date Out Time Out'] = pd.to_datetime(filtered_deployment_df['Date Out'].astype(str) + ' ' + filtered_deployment_df['Time Out'].astype(str))

# Drop the separate Date In, Time In, Date Out, and Time Out columns if needed
#filtered_deployment_df.drop(columns=['Date In', 'Time In', 'Date Out', 'Time Out'], inplace=True)

print(filtered_deployment_df)

print(filtered_deployment_df['Date In Time In'], filtered_deployment_df['Date Out Time Out'])

# %%
# If you run into an error, have this print the problematic row to help identify the error in the deployment log
problematic_row = filtered_deployment_df.iloc[34] #position number
print(problematic_row)

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
# Example deployment data call
#deployment_data_dict["BT_TCSR41_2210_a"]

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
    for file_number, file_data in site_data.items():
        for file_identifier, file_info in file_data.items():
            # Get the DataFrame
            df = file_info['DataFrame']
            
            # Convert the 'Date Time, GMT-04:00' column to datetime format
            df['Date Time, GMT-04:00'] = pd.to_datetime(df['Date Time, GMT-04:00'])

            # Plot the temperature over time
            plt.figure(figsize=(12, 6))
            plt.plot(df.index, df['Temp, °C'], color='blue', marker='o', linestyle='-')
            plt.title('Temperature Over Time')
            plt.xlabel('Date Time')
            plt.ylabel('Temp, °C')  # Modified label
            plt.grid(True)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()

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
            date_column = 'Date Time, GMT-04:00'  # Assuming this is the column containing timestamps
            
            # Convert the date column to datetime objects, specifying the format
           # df[date_column] = pd.to_datetime(df[date_column], format='%m/%d/%Y %H:%M')
            df[date_column] = pd.to_datetime(df[date_column], format='%m/%d/%y %H:%M:%S')
            
            # Filter the DataFrame based on the specified time range
            df = df[(df[date_column] >= deployment_data_dict[file_info['File Name']]['Date In Time In']) &
                    (df[date_column] <= deployment_data_dict[file_info['File Name']]['Date Out Time Out'])]
            
            # Update the DataFrame in df_files
            df_files[site_code][file_number][file_identifier]['DataFrame'] = df

#%% Trim data down on both ends by an hour THIS SECTION CAN BE COMMENTED OUT IF FURTHER TRIMMING IS NOT REQUIRED.
# This is to eliminate any data errors from the removal and retrevial from the water.
# In terms of data processing it may be better to eliminate human error and just inerpolate these points
# When connecting the data to previous data. 

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

#%% Checks to see if the "a" and "b" files and the "c" and "d" files have the same number of data points

# Iterate through each site code
for site_code, file_numbers in df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        # Check if there are multiple file identifiers for the file number
        if len(identifiers) > 1:
            # Get the DataFrames associated with the current site code, file number, and identifiers
            data_frames = [info['DataFrame'] for identifier, info in identifiers.items()]
            
            # Get the number of rows for each DataFrame
            num_rows = [df.shape[0] for df in data_frames]
            
            # Check if files associated with the file number have different numbers of data points
            if len(set(num_rows)) != 1:
                print(f"Site code: {site_code}, File number: {file_number} have files with different numbers of data points.")
            else:
                print(f"Site code: {site_code}, File number: {file_number} have files with the same number of data points: {num_rows[0]}.")

# %%
#calls a dataframe
#print(df_files['TCBKIX']['2210']['a'])

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
                df_a['Temperature_Difference'] = abs(df_a['Temp, °C'] - df_b['Temp, °C'])
            else:
                print(f"Temperature columns not found for Site: {site_code}, File Number: {file_number}")
        else:
            print(f"Only one file for Site: {site_code}, File Number: {file_number}, so averaging could not occur")

#%% If dataframes are 'c' and 'd' then merge the dataframes on the 'Date Time, GMT-04:00'
# Iterate through each site code
for site_code, file_numbers in df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        # Check if 'c' and 'd' identifiers exist for the current site code and file number
        if 'c' in identifiers and 'd' in identifiers:
            # Get the 'c' and 'd' DataFrames
            df_c = identifiers['c']['DataFrame']
            df_d = identifiers['d']['DataFrame']
            
            # Check if the 'Date Time, GMT-04:00' column exists in both DataFrames
            if 'Date Time, GMT-04:00' in df_c.columns and 'Date Time, GMT-04:00' in df_d.columns:
                # Merge the DataFrames on the 'Date Time, GMT-04:00' column
                merged_df = pd.merge(df_c, df_d, on='Date Time, GMT-04:00', how='outer', suffixes=('_c', '_d'))
                
                # Add the merged DataFrame to df_files under a new identifier 'merged'
                df_files[site_code][file_number]['merged'] = {'DataFrame': merged_df, 'File Name': 'merged'}
                
                # Optionally, you can drop the 'c' and 'd' DataFrames if needed
                # del df_files[site_code][file_number]['c']
                # del df_files[site_code][file_number]['d']
            else:
                print(f"'Date Time, GMT-04:00' column not found in 'c' or 'd' DataFrame for Site: {site_code}, File Number: {file_number}")
        else:
            print(f"Only one file for Site: {site_code}, File Number: {file_number}, so merging could not occur")

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
                # Filter rows where 'Temperature_Difference' is above 0.2
                above_threshold = df_a[df_a['Temperature_Difference'] > 0.2]
                
                # Check if there are any rows above the threshold
                if not above_threshold.empty:
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

# %%
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

            #Add a boolean flag column
            calc_a['Flag'] = calc_a["Temperature_Difference"] > 0.2
            

            # Print calc_a
            print(f"DataFrame for Site Code: {site_code}, File Number: {file_number} (calc_a)")
            print(calc_a)
            print("\n")

# %%
# Iterate through each site code
for site_code, file_numbers in calc_df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        if 'a' in identifiers:
            # Get the dataframe calc_a
            calc_a = identifiers['a']['DataFrame']
            
            # Count the number of 'True' values in the filtered DataFrame
            true_count = calc_a['Flag'].astype(str).value_counts().get('True',0)
            print(f"{site_code} {file_number}, Number of 'True' values flagged: {true_count}")

# %%
# Define the output folder where you want to save the extracted files
output_folder = r"C:\UVI\QAQC stuff\Temp_TCRMP_2023_Output"

# Define the calculations folder within the output folder
calculations_folder = os.path.join(output_folder, "Provisional Duplicates")

# Create the calculations folder if it doesn't exist
if not os.path.exists(calculations_folder):
    os.makedirs(calculations_folder)

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
            output_file_path = os.path.join(calculations_folder, output_file_name)
            
            # Drop unnecessary columns from calc_a
            calc_a = calc_a[['#', 'Date Time, GMT-04:00', 'Temp A', 'Temp B', 'Temperature_Difference','Average_temp','Flag']]
            
            # Save the 'a' DataFrame to CSV
            calc_a.to_csv(output_file_path, index=False)
            
            print(f"File saved: Site: {site_code}, File Number: {file_number}, Path: {output_file_path}")
        else:
            print(f"No 'a' version found for Site: {site_code}, File Number: {file_number}")

# %%
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

#%% Drop columns
# Iterate through each site code
for site_code, file_numbers in df_files.items():
    # Iterate through each file number
    for file_number, identifiers in file_numbers.items():
        # Check if there are both 'a' and 'b' files for the current site code and file number
        if 'a' in identifiers:
            # Get the 'a' dataframe
            df_a = identifiers['a']['DataFrame']
            
            # Drop all columns except '#' 'Date Time, GMT-04:00' and 'Temp, °C'
            columns_to_keep = ['#', 'Date Time, GMT-04:00', 'Temp, °C']
            df_a = df_a[columns_to_keep]
            #df_a = df_a.rename(columns={'#': 'number','Date Time, GMT-04:00': 'Date Time GMT-04:00','Temp, °C': 'Temp C'})
            
            # Update the dataframe in df_files
            df_files[site_code][file_number]['a']['DataFrame'] = df_a

            if 'b' in identifiers:
                # Get the 'b' dataframe
                df_b = identifiers['b']['DataFrame']

                # Drop all columns except '#', 'Date Time, GMT-04:00' and 'Temp, °C'
                df_b = df_b[columns_to_keep]

                # Update the dataframe in df_files
                df_files[site_code][file_number]['b']['DataFrame'] = df_b

                if 'c' in identifiers:
                    # Get the 'c' dataframe
                    df_c = identifiers['c']['DataFrame']

                    # Drop all columns except '#', 'Date Time, GMT-04:00' and 'Temp, °C'
                    df_c = df_c[columns_to_keep]

                    # Update the dataframe in df_files
                    df_files[site_code][file_number]['c']['DataFrame'] = df_c
                    
                    if 'd' in identifiers:
                        # Get the 'd' dataframe
                        df_d = identifiers['d']['DataFrame']

                        # Drop all columns except '#', 'Date Time, GMT-04:00' and 'Temp, °C'
                        df_d = df_d[columns_to_keep]

                        # Update the dataframe in df_files
                        df_files[site_code][file_number]['d']['DataFrame'] = df_d

#%% Individual plots

# # Get the DataFrame
#df = df_files['TCSPTH']['2311']['a']['DataFrame']

# # Convert the 'Date Time, GMT-04:00' column to datetime format
#df['Date Time, GMT-04:00'] = pd.to_datetime(df['Date Time, GMT-04:00'])


# # Plot the temperature over time
#plt.figure(figsize=(12, 6))
#plt.plot(df.index, df['Temp, °C'], color='blue', marker='o', linestyle='-')
#plt.title('Temperature Over Time')
#plt.xlabel('Date Time')
#plt.ylabel('Temp, °C')  # Modified label
#plt.grid(True)
#plt.xticks(rotation=45)
#plt.tight_layout()
#plt.show()

#%% Loop all plots without altering time to display as months -- Runs Faster

# # Loop through df_files and plot graphs for each DataFrame
# for site_code, site_data in df_files.items():
#     for file_number, file_data in site_data.items():
#         for file_identifier, file_info in file_data.items():
#             # Get the DataFrame
#             df = file_info['DataFrame']

#             # Convert the 'Date Time, GMT-04:00' column to datetime format
#             #df['Date Time, GMT-04:00'] = pd.to_datetime(df['Date Time, GMT-04:00'])
            
#             # This was changed to deal with a error caused by modify a slice of a dataframe
#             # This iloc explicitly sets values in the dataframe 
#             df.loc[:, 'Date Time, GMT-04:00'] = pd.to_datetime(df['Date Time, GMT-04:00'])

#             # Plot the temperature over time
#             plt.figure(figsize=(12, 6))
#             plt.plot(df.index, df['Temp, °C'], color='blue', marker='o', linestyle='-')
#             plt.title(f'Temperature Over Time - Site: {site_code}, File Number: {file_number}, Identifier: {file_identifier}')
#             plt.xlabel('Date Time')
#             plt.ylabel('Temp, °C')
#             plt.grid(True)
#             plt.xticks(rotation=45)
#             plt.tight_layout()
#             plt.show()

#%% Loop through df_files and plot graphs for each DataFrame where time is displayed as months

# Loop through df_files and plot graphs for each DataFrame
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        for file_identifier, file_info in file_data.items():
            # Get the DataFrame
            df = file_info['DataFrame']

            # Convert the 'Date Time, GMT-04:00' column to datetime format
            df.loc[:, 'Date Time, GMT-04:00'] = pd.to_datetime(df['Date Time, GMT-04:00'])

            # Plot the temperature over time
            plt.figure(figsize=(12, 6))
            plt.plot(df.index, df['Temp, °C'], color='blue', marker='o', linestyle='-')
            plt.title(f'Temperature Over Time - Site: {site_code}, File Number: {file_number}, Identifier: {file_identifier}')
            plt.xlabel('Date Time')
            plt.ylabel('Temp, °C')
            plt.grid(True)
            
            # Set x-axis ticks to display month names at regular intervals
            first_day_of_month_indices = df.index[df['Date Time, GMT-04:00'].dt.day == 1]
            plt.xticks(first_day_of_month_indices, [dt.strftime('%b') for dt in df.loc[first_day_of_month_indices, 'Date Time, GMT-04:00']], rotation=45, fontdict={'family': 'sans-serif', 'size': 25, 'style': 'normal'})
            
            plt.tight_layout()
            plt.show()

# %% Individual start time end time checks 
#print("DataFrame Start Time:", df_files['TCBKIT']['2310']['a']['DataFrame']['Date Time, GMT-04:00'].iloc[0])
#print("Deployment Data Start Time:", deployment_data_dict['BT_TCBKIT_2310_']['Date In Time In'])

# Print end time
#print("DataFrame End Time:", df_files['TCBKIT']['2310']['a']['DataFrame']['Date Time, GMT-04:00'].iloc[-1])
#print("Deployment Data End Time:", deployment_data_dict['BT_TCBKIT_2310_']['Date Out Time Out'])

# Then, check if the extracted timestamps match the expected start and end times.

# %% Loop for start time end time checks
# List to store the names of empty DataFrames
empty_dataframes = []

# Loop through each site code, file number, and file identifier in df_files
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        for file_identifier, file_info in file_data.items():
            # Get the DataFrame for the current file
            df = file_info['DataFrame']
            
            if not df.empty:  # Check if the DataFrame is not empty
                # Print the start and end times for the current DataFrame
                print(f"DataFrame Start Time ({site_code}_{file_number}_{file_identifier}):", df['Date Time, GMT-04:00'].iloc[0])
                print(f"DataFrame End Time ({site_code}_{file_number}_{file_identifier}):", df['Date Time, GMT-04:00'].iloc[-1])

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

# %%
# Iterate through df_files to extract and save the "a" version of the files
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        if 'a' in file_data:  # Check if "a" version exists for the file
            # Extract the DataFrame for the "a" version
            df_a = file_data['a']['DataFrame']
            
            # Get the year and month of the first and last data points
            first_data_point = df_a['Date Time, GMT-04:00'].iloc[0]
            last_data_point = df_a['Date Time, GMT-04:00'].iloc[-1]
            year_month_first = first_data_point.strftime("%y %m %d")  # Using last two digits of the year
            year_month_last = last_data_point.strftime("%y %m %d")    # Using last two digits of the year
            
            # Construct the base file name
            base_file_name = f"BT_{site_code}_{year_month_first}_{year_month_last}"
            
            # Check if the file is identified as a calculation file
            if (site_code, file_number) in calculations:
                output_file_name = f"{base_file_name}_calculations.csv"
            else:
                output_file_name = f"{base_file_name}.csv"
            
            print(f"{output_file_name}")
        else:
            print(f"No 'a' version found for Site: {site_code}, File Number: {file_number}")

# %%
# Define the output folder where you want to save the extracted files
output_folder = r"C:\UVI\QAQC stuff\Temp_TCRMP_2024_Output"

# Define the internal calculations folder within the output folder
internal_calculations_folder = os.path.join(output_folder, "internal_calculations")

# Create the calculations folder if it doesn't exist
if not os.path.exists(internal_calculations_folder):
    os.makedirs(internal_calculations_folder)

# Iterate through df_files to extract and save the "a" version of the files
for site_code, site_data in df_files.items():
    for file_number, file_data in site_data.items():
        if 'a' in file_data:  # Check if "a" version exists for the file
            # Extract the DataFrame for the "a" version
            df_a = file_data['a']['DataFrame']
            
            # Get the year and month of the first and last data points
            first_data_point = df_a['Date Time, GMT-04:00'].iloc[0]
            last_data_point = df_a['Date Time, GMT-04:00'].iloc[-1]
            year_month_first = first_data_point.strftime("%y%m")  # Using last two digits of the year
            year_month_last = last_data_point.strftime("%y%m")    # Using last two digits of the year
            
            # Construct the base file name
            base_file_name = f"BT_{site_code}_{year_month_first}_{year_month_last}"
            
            # Check if the file is identified as a calculation file
            if (site_code, file_number) in calculations:
                output_file_name = f"{base_file_name}_internal_calculations.csv"
                output_file_path = os.path.join(internal_calculations_folder, output_file_name)
            else:
                output_file_name = f"{base_file_name}.csv"
                output_file_path = os.path.join(output_folder, output_file_name)
            
            # Save the DataFrame to CSV
            df_a.to_csv(output_file_path, index=False)
            
            print(f"File saved: {output_file_path}")
        else:
            print(f"No 'a' version found for Site: {site_code}, File Number: {file_number}")

# %% This is a TEST to handle exporting both the "a" and "merged" identifiers
# # Iterate through df_files to extract and save the "a" and "merged" versions of the files
# for site_code, site_data in df_files.items():
#     for file_number, file_data in site_data.items():
#         for identifier, data_info in file_data.items():
#             if identifier in ['a', 'merged']:  # Check if the identifier is either 'a' or 'merged'
#                 # Extract the DataFrame for the current identifier
#                 df = data_info['DataFrame']
                
#                 # Get the year and month of the first and last data points
#                 first_data_point = df['Date Time, GMT-04:00'].iloc[0]
#                 last_data_point = df['Date Time, GMT-04:00'].iloc[-1]
#                 year_month_first = first_data_point.strftime("%y%m")  # Using last two digits of the year
#                 year_month_last = last_data_point.strftime("%y%m")    # Using last two digits of the year
                
#                 # Construct the base file name
#                 base_file_name = f"BT_{site_code}_{year_month_first}_{year_month_last}"
                
#                 # Check if the file is identified as a calculation file
#                 if (site_code, file_number) in calculations:
#                     output_file_name = f"{base_file_name}_calculations.csv"
#                 else:
#                     output_file_name = f"{base_file_name}.csv"
                
#                 output_file_path = os.path.join(output_folder, output_file_name)
                
#                 # Save the DataFrame to CSV
#                 df.to_csv(output_file_path, index=False)
                
#                 print(f"File saved: {output_file_path}")
#             else:
#                 print(f"No 'a' or 'merged' version found for Site: {site_code}, File Number: {file_number}")

#%% Using new offloaded files create plots and save plots to a folder
 
exported_folder_path = r'C:\UVI\QAQC stuff\Temp_TCRMP_2024_Output'

# Define pattern to match CSV files
file_pattern = '*.csv'

# Use glob to get a list of file paths matching the pattern set in file_pattern
exported_csv_files = glob.glob(exported_folder_path + '/' + file_pattern)

# Loop through each CSV file
for csv_file in exported_csv_files:
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)
    
    df['Date Time, GMT-04:00'] = pd.to_datetime(df['Date Time, GMT-04:00'], format='%Y-%m-%d %H:%M:%S')

    # Convert the 'Date Time, GMT-04:00' column to datetime format
    #df.loc[:, 'Date Time, GMT-04:00'] = pd.to_datetime(df['Date Time, GMT-04:00'])
    df['Date Time, GMT-04:00'] = pd.to_datetime(df['Date Time, GMT-04:00'])
    # Plot the data
    plt.figure(figsize=(12, 6))
    plt.plot(df['Date Time, GMT-04:00'], df['Temp, °C'], color='blue', marker='o', linestyle='-')
    plt.title('Temperature Over Time')
    plt.xlabel('Date Time')
    plt.ylabel('Temp, °C')
    plt.grid(True)
    
    # Set x-axis ticks to display month names at regular intervals
    # first_day_of_month_indices = df.index[df['Date Time, GMT-04:00'].dt.day == 1]
    # plt.xticks(first_day_of_month_indices, [dt.strftime('%b') for dt in df.loc[first_day_of_month_indices, 'Date Time, GMT-04:00']], rotation=45, fontdict={'family': 'sans-serif', 'size': 25, 'style': 'normal'})
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Extract file name from the file path
    file_name = os.path.basename(csv_file)
    
    # Define the file name for the plot
    plot_file_name = os.path.splitext(file_name)[0] + '_plot.png'
    
    save_dir = r"C:\UVI\QAQC stuff\Temp_TCRMP_2024_Output\graphs"
    # Save the plot
    plt.savefig(os.path.join(save_dir, plot_file_name))
    
    # Show the plot (optional)
    # plt.show()

#%%
# # Loop through df_files and plot graphs for each DataFrame where time is displayed as year and month
# for site_code, site_data in df_files.items():
#     for file_number, file_data in site_data.items():
#         for file_identifier, file_info in file_data.items():
#             # Check if the file_identifier starts with 'a' or is 'merged'
#             if file_identifier.startswith('a') or file_identifier == 'merged':
#                 # Get the DataFrame
#                 df = file_info['DataFrame']

#                 # Convert the 'Date Time, GMT-04:00' column to datetime format
#                 df.loc[:, 'Date Time, GMT-04:00'] = pd.to_datetime(df['Date Time, GMT-04:00'])

#                 # Plot the temperature over time
#                 plt.figure(figsize=(12, 6))
#                 plt.plot(df.index, df['Temp, °C'], color='blue', marker='o', linestyle='-')
#                 plt.title(f'Temperature Over Time - Site: {site_code}, File Number: {file_number}, Identifier: {file_identifier}')
#                 plt.xlabel('Date Time')
#                 plt.ylabel('Temp, °C')
#                 plt.grid(True)
                
#                 # Set x-axis ticks to display month names at regular intervals
#                 first_day_of_month_indices = df.index[df['Date Time, GMT-04:00'].dt.day == 1]
#                 plt.xticks(first_day_of_month_indices, [dt.strftime('%b') for dt in df.loc[first_day_of_month_indices, 'Date Time, GMT-04:00']], rotation=45, fontdict={'family': 'sans-serif', 'size': 25, 'style': 'normal'})
                
#                 plt.tight_layout()
                
#                 # Specify the directory to save the images
#                 save_dir = r"C:\Users\900094088\Documents\Oceanography\SOPs\HOBO SOPs\Export_folder\graphs"
#                 if not os.path.exists(save_dir):
#                     os.makedirs(save_dir)
                
#                 # Define the file name
#                 file_name = f"temp_over_time_{site_code}_{file_number}_{file_identifier}.png"
                
#                 # Save the plot as an image file
#                 plt.savefig(os.path.join(save_dir, file_name), dpi=300)
                
#                 # Close the plot to free up memory
#                 plt.close()



