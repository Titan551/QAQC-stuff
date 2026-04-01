# QAQC_Aggregate_V2

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

os.chdir(r'C:\UVI\QAQC stuff')

from QAQC_HELPER_FUNCTIONS import (
    get_csv_files,
    get_usvi_site_codes,
    get_panama_site_codes,
    ensure_utf8_encoding,
    load_structured_dataframes,
    clean_plot_title_headers,
    convert_panama_times,
    normalize_sio_file_names,
    report_missing_a_identifiers,
    reassign_offset_identifiers,
    filter_deployment_log,
    check_unmatched_filenames,
    validate_time_columns,
    convert_deployment_log_datetime,
    create_deployment_data_dict,
    format_deployment_datetimes,
    plot_pre_trimmed,
    parse_deployment_datetime_strings,
    trim_dataframes_by_date,
    final_trim_dataframe_edges,
    check_data_lengths,
    compute_temperature_difference,
    identify_calculations,
    build_calc_df_subset,
    add_comparison_columns,
    report_flags,
    save_flagged_files,
    average_temperature_if_close,
    report_nan_temperature_differences,
    drop_extra_columns,
    plot_temperature_time_series,
    merge_offset_files,
    plot_offset_agreement,
    plot_merged_temperatures,
    print_start_end_times,
    generate_trimmed_filenames,
    save_offload_files,
    create_and_save_offload_plots,
    trim_dataframe
)


#%%
# INPUTS
 
# Define folder path where your CSV files are located
folder_path = r'C:\UVI\QAQC stuff\Temp_TCRMP_2025_PBL'

# Depolyment log file path
deployment_log_path = r'C:\UVI\QAQC stuff\Temperature_UVI_deployment_log.csv'

# folder path for plotting
exported_folder_path = r'C:\UVI\QAQC stuff\Temp_TCRMP_2025_PBL_output'

# Set folder with PD_files
PD_folder_path = r'C:\UVI\QAQC stuff\Temp_TCRMP_2025_Output\Provisional Duplicates'

# OUTPUTS
# Define the output folder where you want to save the extracted files
output_folder = r"C:\UVI\QAQC stuff\Temp_TCRMP_2025_output"

# Define the calculations folder within the output folder
calculations_folder = os.path.join(output_folder, "Provisional Duplicates")

# Output for plots
save_dir = r"C:\UVI\QAQC stuff\Temp_TCRMP_2025_PBL_output\graphs"
# Create the directory if it doesn't exist
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

# Set the export folder path
PD_export_path = r'C:\UVI\QAQC stuff\PD_processing\Provisional_Duplicates_2025'

# Define the provisional duplicate output folder path
pd_output_folder = r'C:\UVI\QAQC stuff\PD_processing\PD_2025_output'

#%%
# 1. Get CSV files in folder_path
csv_files = get_csv_files(folder_path)

# 2. Get site codes lists
usvi_codes = get_usvi_site_codes()
panama_codes = get_panama_site_codes()

# 3. Ensure UTF-8 encoding for all CSV files in folder_path
ensure_utf8_encoding(folder_path)

# 4. Load CSVs into nested dict structure
df_files = load_structured_dataframes(csv_files, usvi_codes, panama_codes)

# 5. Clean plot title headers in df_files if needed
clean_plot_title_headers(df_files)

# 6. Convert Panama site times (GMT-04:00 to GMT-05:00)
convert_panama_times(df_files, panama_codes)

# 7. Normalize SIO file names, rename files in folder_path
normalize_sio_file_names(df_files, folder_path)

# 8. Report missing 'a' identifiers in df_files
report_missing_a_identifiers(df_files)

# 9. Reassign offset identifiers 'a' and 'b' to 'c' and 'd' if needed
reassign_offset_identifiers(df_files, panama_codes)

# 10. Import and Filter deployment log to only files matching CSVs
deployment_df = pd.read_csv(deployment_log_path)

filtered_deployment_df, csv_file_names = filter_deployment_log(deployment_df, csv_files)

# 11. Check unmatched filenames in deployment log vs CSV files
check_unmatched_filenames(filtered_deployment_df, csv_file_names)

# 12. Validate time columns in filtered deployment log
validated_deployment_df = validate_time_columns(filtered_deployment_df)

# 13. Convert deployment log date/time columns
converted_deployment_df = convert_deployment_log_datetime(validated_deployment_df)

# 14. Create a deployment data dictionary for quick lookups
deployment_data_dict = create_deployment_data_dict(converted_deployment_df)

# 15. Format deployment datetime strings for readability
format_deployment_datetimes(deployment_data_dict)

# 16. Plot pre-trimmed data for QC, saving plots in save_dir
plot_pre_trimmed(df_files, panama_codes, save_dir)

# 17. Parse deployment datetime strings back into datetime objects (after formatting)
parse_deployment_datetime_strings(deployment_data_dict)

# 18. Trim df_files by deployment date/time ranges
trim_dataframes_by_date(df_files, deployment_data_dict, panama_codes)

# 19. Final trim edges of each dataframe (defaults: drop first 4 and last 5 rows)
final_trim_dataframe_edges(df_files)

# 20. Check if pairs ('a' & 'b', 'c' & 'd', etc.) have same data lengths
check_data_lengths(df_files)

# 21. Compute temperature difference between 'a' and 'b' files
compute_temperature_difference(df_files)

# 22. Identify calculations (sites/files needing attention due to temp diffs)
calculations = identify_calculations(df_files)

# 23. Build subset dict for those calculations
calc_df_files = build_calc_df_subset(df_files, calculations)

# 24. Add comparison columns (Temp A, Temp B, Average, Flag) to 'a' dfs
add_comparison_columns(calc_df_files)

# 25. Report flagged temperature differences
report_flags(calc_df_files)

# 26. Save flagged files as CSV to your calculations_folder output
save_flagged_files(calc_df_files, calculations_folder)

# 27. Average temperature between 'a' and 'b' files if difference below threshold (0.2°C)
average_temperature_if_close(df_files)

# 28. Report NaN counts in 'a' dfs for flagged calculations
report_nan_temperature_differences(df_files, calculations)

# 29. Drop extra columns, keep only necessary ones (#, date_col, Temp)
drop_extra_columns(df_files, panama_codes)

# 30. Plot temperature time series for all data (post-trim and cleaning)
plot_temperature_time_series(df_files, panama_codes)

# 31. Merge offset files 'c' and 'd' for each site, file number, returning merged dict
merged_offset_data = merge_offset_files(df_files, panama_codes)

# 32. Plot offset agreement scatter plots comparing 'c' and 'd' logger temperatures
plot_offset_agreement(df_files, panama_codes)

# 33. Plot merged temperature time series for each site from merged data
plot_merged_temperatures(merged_offset_data, panama_codes)

# 34. Print start and end times for each dataframe and matching deployment data
print_start_end_times(df_files, panama_codes, deployment_data_dict)

# 35. Generate and print trimmed filenames for 'a' files and merged files (no saving)
generate_trimmed_filenames(df_files, merged_offset_data, panama_codes, calculations)

# 36. Save 'a' files and merged files as CSVs to output folder with adjusted columns
save_offload_files(df_files, merged_offset_data, panama_codes, output_folder)

# 37. Create and save plots from exported CSV files in exported_folder_path to save_dir
create_and_save_offload_plots(exported_folder_path, save_dir, panama_codes)

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