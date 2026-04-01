#%% Imports
import os
import pandas as pd
import glob
import matplotlib.pyplot as plt
from datetime import datetime
import xarray as xr
import numpy as np
#%% Get file paths
 
# Define folder path where your CSV files are located
folder_path = r'C:\Users\900094088\Documents\Oceanography\csv_to_netcdf\working_dir'

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
#%%

df_files = {}
time_ranges = {}
start_end_times = {}  # Dictionary to hold start and end times

# Iterate through each CSV file
for csv_file in csv_files:
    # Extract site code from file name
    file_name = os.path.basename(csv_file).split('.')[0]  # Remove the file extension
    parts = file_name.split('_')
    site_code = parts[1]  # Extract the site code
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    
    # Extract start and end times
    df['Date Time, UTC-04:00'] = pd.to_datetime(df['Date Time, UTC-04:00'])
    start_time = df['Date Time, UTC-04:00'].min()
    end_time = df['Date Time, UTC-04:00'].max()
    
    # Format start and end times
    formatted_start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
    formatted_end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
    
    # Check if the site code already exists in the dictionary
    if site_code in df_files:
        # Append the new DataFrame to the existing one
        df_files[site_code] = pd.concat([df_files[site_code], df], ignore_index=True)
        
        # Update start and end times
        current_start_time = start_end_times[site_code]['start']
        current_end_time = start_end_times[site_code]['end']
        new_start_time = min(current_start_time, start_time)
        new_end_time = max(current_end_time, end_time)
        start_end_times[site_code]['start'] = new_start_time
        start_end_times[site_code]['end'] = new_end_time
        
        # Update time_ranges dictionary
        time_ranges[site_code] = f"{new_start_time.strftime('%Y-%m-%d %H:%M:%S')} - {new_end_time.strftime('%Y-%m-%d %H:%M:%S')}"
    else:
        # Create a new entry in the dictionaries
        df_files[site_code] = df
        start_end_times[site_code] = {'start': start_time, 'end': end_time}
        time_ranges[site_code] = f"{formatted_start_time} - {formatted_end_time}"

#%% Rename columns in the DataFrames
# Iterate through all dataframes in df_files dictionary
for site_code, df in df_files.items():
    # Rename columns in the DataFrame
    df.rename(columns={
        '#': 'Number',    
        'Date Time, UTC-04:00': 'Time',
        'Temp, °C': 'Temperature'
    }, inplace=True)
    
    # Convert 'Time' column to datetime
    df['Time'] = pd.to_datetime(df['Time'])
#%%    
#### FOR EACH SITE CODE I NEED TO ASSIGN AND CREATE NC FILES WITH THE PARAMITERS BELOW. ###
#TCSWAT
#%% Define metadata attributes for each site code
metadata = {}
for site_code in site_codes:
    if site_code in time_ranges:
        history = f"File processed from {time_ranges[site_code]}"
        
        global_attributes = {
            #'title': 'Oceanographic Data', # included in ERDDAP GenerateDatasetXML
            #'institution': 'Your Institution Name', # included in ERDDAP GenerateDatasetXML
            'source': 'Benthic temperature record from South Water, St. Thomas, USVI',
            'history': history,
            'references': 'www.uvi.edu',
            'coordinates': '18.28068, -64.94592',
            'geospatial_lat_max': '18.28068',
            'geospatial_lat_min': '18.28068',
            'geospatial_lat_units': 'degrees_north',
            'geospatial_lon_max': '-64.94592',
            'geospatial_lon_min': '-64.94592',
            'geospatial_lon_units': 'degrees_east',
            'depth': '20 m',
            'location': 'South Water',
            'site_description': 'Midshelf hardbottom site with diverse fish community',
            'project': 'The United States Virgin Islands Territorial Coral Reef Monitoring Program',
            'funding': 'Department of Planning and Natural Resources, NOAA Coral Reef Conservation Program',
            'contact': 'Tyler B. Smith',
            'contact_email': 'tsmith@uvi.edu',
            'contact_phone': '+1 340 693 1394',
            'device_name': 'HOBO U22-001 Water Temp',
            'comment': 'Data collected during oceanographic expeditions.'
        }

        variable_attributes = {
            'Number': {
                'long_name': 'Sequential number of the datum',
                'units': '1'
            },
            'Temperature': {
                'long_name': 'Water Temperature',
                'units': 'Celsius'
            },
            'Time': {
                'long_name': 'Time of Measurement',
                'units': 'seconds since 1970-01-01T00:00:00Z'
            }
        }

        metadata[site_code] = {
            'global_attributes': global_attributes,
            'variable_attributes': variable_attributes
        }
#%% Modify the metadata for each site code that is in the working directory
csv_files # Check the files in the working directory for which site codes are present and edit below accordingly
#%% Modify for TCCORB
if 'TCCORB' in metadata:
    metadata['TCCORB']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCCORB']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCCORB']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCCORB']['global_attributes']['site_description'] = 'New site description for TCCORB'
    metadata['TCCORB']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCCORB']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCCORB']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCCORB']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCCORB']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCFSHB
if 'TCFSHB' in metadata:
    metadata['TCFSHB']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCFSHB']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCFSHB']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCFSHB']['global_attributes']['site_description'] = 'New site description for TCFSHB'
    metadata['TCFSHB']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCFSHB']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCFSHB']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCFSHB']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCFSHB']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCMERI
if 'TCMERI' in metadata:
    metadata['TCMERI']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCMERI']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCMERI']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCMERI']['global_attributes']['site_description'] = 'New site description for TCMERI'
    metadata['TCMERI']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCMERI']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCMERI']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCMERI']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCMERI']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCFSHB
if 'TCBKPT' in metadata:
    metadata['TCBKPT']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCBKPT']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCBKPT']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCBKPT']['global_attributes']['site_description'] = 'New site description for TCFSHB'
    metadata['TCBKPT']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCBKPT']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCBKPT']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCBKPT']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCBKPT']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCBOTB
if 'TCBOTB' in metadata:
    metadata['TCBOTB']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCBOTB']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCBOTB']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCBOTB']['global_attributes']['site_description'] = 'New site description for TCBOTB'
    metadata['TCBOTB']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCBOTB']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCBOTB']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCBOTB']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCBOTB']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCBRWB
if 'TCBRWB' in metadata:
    metadata['TCBRWB']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCBRWB']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCBRWB']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCBRWB']['global_attributes']['site_description'] = 'New site description for TCBRWB'
    metadata['TCBRWB']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCBRWB']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCBRWB']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCBRWB']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCBRWB']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCBKIT
if 'TCBKIT' in metadata:
    metadata['TCBKIT']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCBKIT']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCBKIT']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCBKIT']['global_attributes']['site_description'] = 'New site description for TCBKIT'
    metadata['TCBKIT']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCBKIT']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCBKIT']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCBKIT']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCBKIT']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCCORK
if 'TCCORK' in metadata:
    metadata['TCCORK']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCCORK']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCCORK']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCCORK']['global_attributes']['site_description'] = 'New site description for TCCORK'
    metadata['TCCORK']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCCORK']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCCORK']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCCORK']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCCORK']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCCLGE
if 'TCCLGE' in metadata:
    metadata['TCCLGE']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCCLGE']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCCLGE']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCCLGE']['global_attributes']['site_description'] = 'New site description for TCCLGE'
    metadata['TCCLGE']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCCLGE']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCCLGE']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCCLGE']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCCLGE']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCFLTC
if 'TCFLTC' in metadata:
    metadata['TCFLTC']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCFLTC']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCFLTC']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCFLTC']['global_attributes']['site_description'] = 'New site description for TCFLTC'
    metadata['TCFLTC']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCFLTC']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCFLTC']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCFLTC']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCFLTC']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCGB63
if 'TCGB63' in metadata:
    metadata['TCGB63']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCGB63']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCGB63']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCGB63']['global_attributes']['site_description'] = 'New site description for TCGB63'
    metadata['TCGB63']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCGB63']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCGB63']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCGB63']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCGB63']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCGMKT
if 'TCGMKT' in metadata:
    metadata['TCGMKT']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCGMKT']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCGMKT']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCGMKT']['global_attributes']['site_description'] = 'New site description for TCGMKT'
    metadata['TCGMKT']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCGMKT']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCGMKT']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCGMKT']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCGMKT']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCHB40
if 'TCHB40' in metadata:
    metadata['TCHB40']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCHB40']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCHB40']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCHB40']['global_attributes']['site_description'] = 'New site description for TCHB40'
    metadata['TCHB40']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCHB40']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCHB40']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCHB40']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCHB40']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCHB30
if 'TCHB30' in metadata:
    metadata['TCHB30']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCHB30']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCHB30']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCHB30']['global_attributes']['site_description'] = 'New site description for TCHB30'
    metadata['TCHB30']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCHB30']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCHB30']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCHB30']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCHB30']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCHB20
if 'TCHB20' in metadata:
    metadata['TCHB20']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCHB20']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCHB20']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCHB20']['global_attributes']['site_description'] = 'New site description for TCHB20'
    metadata['TCHB20']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCHB20']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCHB20']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCHB20']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCHB20']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCMAGB
if 'TCMAGB' in metadata:
    metadata['TCMAGB']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCMAGB']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCMAGB']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCMAGB']['global_attributes']['site_description'] = 'New site description for TCMAGB'
    metadata['TCMAGB']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCMAGB']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCMAGB']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCMAGB']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCMAGB']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSAVA
if 'TCSAVA' in metadata:
    metadata['TCSAVA']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCSAVA']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCSAVA']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSAVA']['global_attributes']['site_description'] = 'New site description for TCSAVA'
    metadata['TCSAVA']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSAVA']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSAVA']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSAVA']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSAVA']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSHCS
if 'TCSHCS' in metadata:
    metadata['TCSHCS']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCSHCS']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCSHCS']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSHCS']['global_attributes']['site_description'] = 'New site description for TCSHCS'
    metadata['TCSHCS']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSHCS']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSHCS']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSHCS']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSHCS']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSCAP
if 'TCSCAP' in metadata:
    metadata['TCSCAP']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCSCAP']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCSCAP']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSCAP']['global_attributes']['site_description'] = 'New site description for TCSCAP'
    metadata['TCSCAP']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSCAP']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSCAP']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSCAP']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSCAP']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSC35
if 'TCSC35' in metadata:
    metadata['TCSC35']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCSC35']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCSC35']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSC35']['global_attributes']['site_description'] = 'New site description for TCSC35'
    metadata['TCSC35']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSC35']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSC35']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSC35']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSC35']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSWAT
if 'TCSWAT' in metadata:
    metadata['TCSWAT']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCSWAT']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCSWAT']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSWAT']['global_attributes']['site_description'] = 'New site description for TCSWAT'
    metadata['TCSWAT']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSWAT']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSWAT']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSWAT']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSWAT']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCLSTJ
if 'TCLSTJ' in metadata:
    metadata['TCLSTJ']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCLSTJ']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCLSTJ']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCLSTJ']['global_attributes']['site_description'] = 'New site description for TCLSTJ'
    metadata['TCLSTJ']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCLSTJ']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCLSTJ']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCLSTJ']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCLSTJ']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCBKIX
if 'TCBKIX' in metadata:
    metadata['TCBKIX']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCBKIX']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCBKIX']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCBKIX']['global_attributes']['site_description'] = 'New site description for TCBKIX'
    metadata['TCBKIX']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCBKIX']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCBKIX']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCBKIX']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCBKIX']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCBX33
if 'TCBX33' in metadata:
    metadata['TCBX33']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCBX33']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCBX33']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCBX33']['global_attributes']['site_description'] = 'New site description for TCBX33'
    metadata['TCBX33']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCBX33']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCBX33']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCBX33']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCBX33']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCCB08
if 'TCCB08' in metadata:
    metadata['TCCB08']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCCB08']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCCB08']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCCB08']['global_attributes']['site_description'] = 'New site description for TCCB08'
    metadata['TCCB08']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCCB08']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCCB08']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCCB08']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCCB08']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCCB40
if 'TCCB40' in metadata:
    metadata['TCCB40']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCCB40']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCCB40']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCCB40']['global_attributes']['site_description'] = 'New site description for TCCB40'
    metadata['TCCB40']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCCB40']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCCB40']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCCB40']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCCB40']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCCB99
if 'TCCB99' in metadata:
    metadata['TCCB99']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCCB99']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCCB99']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCCB99']['global_attributes']['site_description'] = 'New site description for TCCB99'
    metadata['TCCB99']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCCB99']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCCB99']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCCB99']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCCB99']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCCB67
if 'TCCB67' in metadata:
    metadata['TCCB67']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCCB67']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCCB67']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCCB67']['global_attributes']['site_description'] = 'New site description for TCCB67'
    metadata['TCCB67']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCCB67']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCCB67']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCCB67']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCCB67']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCCSTL
if 'TCCSTL' in metadata:
    metadata['TCCSTL']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCCSTL']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCCSTL']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCCSTL']['global_attributes']['site_description'] = 'New site description for TCCSTL'
    metadata['TCCSTL']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCCSTL']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCCSTL']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCCSTL']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCCSTL']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCEAGR
if 'TCEAGR' in metadata:
    metadata['TCEAGR']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCEAGR']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCEAGR']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCEAGR']['global_attributes']['site_description'] = 'New site description for TCEAGR'
    metadata['TCEAGR']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCEAGR']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCEAGR']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCEAGR']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCEAGR']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCGRPD
if 'TCGRPD' in metadata:
    metadata['TCGRPD']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCGRPD']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCGRPD']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCGRPD']['global_attributes']['site_description'] = 'New site description for TCGRPD'
    metadata['TCGRPD']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCGRPD']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCGRPD']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCGRPD']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCGRPD']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCJCKB
if 'TCJCKB' in metadata:
    metadata['TCJCKB']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCJCKB']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCJCKB']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCJCKB']['global_attributes']['site_description'] = 'New site description for TCJCKB'
    metadata['TCJCKB']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCJCKB']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCJCKB']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCJCKB']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCJCKB']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCKNGC
if 'TCKNGC' in metadata:
    metadata['TCKNGC']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCKNGC']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCKNGC']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCKNGC']['global_attributes']['site_description'] = 'New site description for TCKNGC'
    metadata['TCKNGC']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCKNGC']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCKNGC']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCKNGC']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCKNGC']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCLBEM
if 'TCLBEM' in metadata:
    metadata['TCLBEM']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCLBEM']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCLBEM']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCLBEM']['global_attributes']['site_description'] = 'New site description for TCLBEM'
    metadata['TCLBEM']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCLBEM']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCLBEM']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCLBEM']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCLBEM']['global_attributes']['geospatial_lon_min'] = 'Type min lon'

#%% Modify for TCLB99
if 'TCLB99' in metadata:
    metadata['TCLB99']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCLB99']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCLB99']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCLB99']['global_attributes']['site_description'] = 'New site description for TCLB99'
    metadata['TCLB99']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCLB99']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCLB99']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCLB99']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCLB99']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCLB67
if 'TCLB67' in metadata:
    metadata['TCLB67']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCLB67']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCLB67']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCLB67']['global_attributes']['site_description'] = 'New site description for TCLB67'
    metadata['TCLB67']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCLB67']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCLB67']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCLB67']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCLB67']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCLBRH
if 'TCLBRH' in metadata:
    metadata['TCLBRH']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCLBRH']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCLBRH']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCLBRH']['global_attributes']['site_description'] = 'New site description for TCLBRH'
    metadata['TCLBRH']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCLBRH']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCLBRH']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCLBRH']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCLBRH']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCMT24
if 'TCMT24' in metadata:
    metadata['TCMT24']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCMT24']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCMT24']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCMT24']['global_attributes']['site_description'] = 'New site description for TCMT24'
    metadata['TCMT24']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCMT24']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCMT24']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCMT24']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCMT24']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCMT40
if 'TCMT40' in metadata:
    metadata['TCMT40']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCMT40']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCMT40']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCMT40']['global_attributes']['site_description'] = 'New site description for TCMT40'
    metadata['TCMT40']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCMT40']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCMT40']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCMT40']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCMT40']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSR30
if 'TCSR30' in metadata:
    metadata['TCSR30']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCSR30']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCSR30']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSR30']['global_attributes']['site_description'] = 'New site description for TCSR30'
    metadata['TCSR30']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSR30']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSR30']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSR30']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSR30']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSR99
if 'TCSR99' in metadata:
    metadata['TCSR99']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCSR99']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCSR99']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSR99']['global_attributes']['site_description'] = 'New site description for TCSR99'
    metadata['TCSR99']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSR99']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSR99']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSR99']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSR99']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSR41
if 'TCSR41' in metadata:
    metadata['TCSR41']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCSR41']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCSR41']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSR41']['global_attributes']['site_description'] = 'New site description for TCSR41'
    metadata['TCSR41']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSR41']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSR41']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSR41']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSR41']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSR67
if 'TCSR67' in metadata:
    metadata['TCSR67']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCSR67']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCSR67']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSR67']['global_attributes']['site_description'] = 'New site description for TCSR67'
    metadata['TCSR67']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSR67']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSR67']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSR67']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSR67']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSR10
if 'TCSR10' in metadata:
    metadata['TCSR10']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCSR10']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCSR10']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSR10']['global_attributes']['site_description'] = 'New site description for TCSR10'
    metadata['TCSR10']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSR10']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSR10']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSR10']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSR10']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSPTH
if 'TCSPTH' in metadata:
    metadata['TCSPTH']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCSPTH']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCSPTH']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSPTH']['global_attributes']['site_description'] = 'New site description for TCSPTH'
    metadata['TCSPTH']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSPTH']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSPTH']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSPTH']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSPTH']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCLE67
if 'TCLE67' in metadata:
    metadata['TCLE67']['global_attributes']['source'] = 'Type Source Here'
    metadata['TCLE67']['global_attributes']['location'] = 'Type Modified Location'
    metadata['TCLE67']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCLE67']['global_attributes']['site_description'] = 'New site description for TCSPTH'
    metadata['TCLE67']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCLE67']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCLE67']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCLE67']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCLE67']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Convert CSV data to NetCDF with metadata incorporated
output_folder = r'C:\Users\900094088\Documents\Oceanography\csv_to_netcdf\netcdf_output'

for csv_file in csv_files:
    # Extract site code and base file name
    file_name = os.path.basename(csv_file).split('.')[0]
    parts = file_name.split('_')
    site_code = parts[1]

    df = df_files[site_code]
    ds = xr.Dataset.from_dataframe(df.set_index('Time'))

    ds['Time'] = xr.DataArray(df['Time'].astype(np.int64) // 10**9, dims='Time', attrs=metadata[site_code]['variable_attributes']['Time'])

    # Add global attributes
    for attr_name, attr_value in metadata[site_code]['global_attributes'].items():
        ds.attrs[attr_name] = attr_value

    # Add variable attributes
    for var_name, var_attrs in metadata[site_code]['variable_attributes'].items():
        for attr_name, attr_value in var_attrs.items():
            ds[var_name].attrs[attr_name] = attr_value

    # Save to NetCDF file with the same base file name as the CSV
    output_file = os.path.join(output_folder, f"{file_name}.nc")
    ds.to_netcdf(output_file)

print("Conversion to NetCDF completed.")
# %%
