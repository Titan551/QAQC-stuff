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
folder_path = r'/Users/gilliancoleman/Smith_Lab/Gillian'

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
    metadata['TCCORB']['global_attributes']['source'] = 'Benthic temperature record from Coral Bay, St. John, USVI'
    metadata['TCCORB']['global_attributes']['location'] = 'Coral Bay'
    metadata['TCCORB']['global_attributes']['depth'] = '9 m'
    metadata['TCCORB']['global_attributes']['site_description'] = 'Patch reef in Coral Bay outside Coral Harbor'
    metadata['TCCORB']['global_attributes']['coordinates'] = '18.33797, -64.70402'
    metadata['TCCORB']['global_attributes']['geospatial_lat_max'] = '18.33797'
    metadata['TCCORB']['global_attributes']['geospatial_lat_min'] = '18.33797'
    metadata['TCCORB']['global_attributes']['geospatial_lon_max'] = '-64.70402'
    metadata['TCCORB']['global_attributes']['geospatial_lon_min'] = '-64.70402'
#%% Modify for TCFSHB
if 'TCFSHB' in metadata:
    metadata['TCFSHB']['global_attributes']['source'] = ' Benthic temperature record from Fish Bay, St. John, USVI'
    metadata['TCFSHB']['global_attributes']['location'] = 'Fish Bay'
    metadata['TCFSHB']['global_attributes']['depth'] = '6 m'
    metadata['TCFSHB']['global_attributes']['site_description'] = 'Just outside the Virgin Islands National Park on the western shore along a gradient of land based sources of pollution'
    metadata['TCFSHB']['global_attributes']['coordinates'] = '18.31417, -64.76408'
    metadata['TCFSHB']['global_attributes']['geospatial_lat_max'] = '18.31417'
    metadata['TCFSHB']['global_attributes']['geospatial_lat_min'] = '18.31417'
    metadata['TCFSHB']['global_attributes']['geospatial_lon_max'] = '-64.76408'
    metadata['TCFSHB']['global_attributes']['geospatial_lon_min'] = '-64.76408'
#%% Modify for TCMERI
if 'TCMERI' in metadata:
    metadata['TCMERI']['global_attributes']['source'] = 'Benthic temperature record from Meri Shoal, St. John, USVI'
    metadata['TCMERI']['global_attributes']['location'] = 'Meri Shoal'
    metadata['TCMERI']['global_attributes']['depth'] = '30 m'
    metadata['TCMERI']['global_attributes']['site_description'] = 'Mesophotic reef off St. John south of Pilsbury Sound and near the CariCOOS Buoy VI1'
    metadata['TCMERI']['global_attributes']['coordinates'] = '18.24447,-64.75862'
    metadata['TCMERI']['global_attributes']['geospatial_lat_max'] = '18.24447'
    metadata['TCMERI']['global_attributes']['geospatial_lat_min'] = '18.24447'
    metadata['TCMERI']['global_attributes']['geospatial_lon_max'] = '-64.75862'
    metadata['TCMERI']['global_attributes']['geospatial_lon_min'] = '-64.75862'
#%% Modify for TCFSHB
if 'TCBKPT' in metadata:
    metadata['TCBKPT']['global_attributes']['source'] = 'Benthic temperature record from Black Point, St. Thomas, USVI'
    metadata['TCBKPT']['global_attributes']['location'] = 'Black Point'
    metadata['TCBKPT']['global_attributes']['depth'] = '9 m'
    metadata['TCBKPT']['global_attributes']['site_description'] = 'High cover mixed orbicellid fringing reef near UVI'
    metadata['TCBKPT']['global_attributes']['coordinates'] = '18.34450, -64.98595'
    metadata['TCBKPT']['global_attributes']['geospatial_lat_max'] = '18.34450'
    metadata['TCBKPT']['global_attributes']['geospatial_lat_min'] = '18.34450'
    metadata['TCBKPT']['global_attributes']['geospatial_lon_max'] = '-64.98595'
    metadata['TCBKPT']['global_attributes']['geospatial_lon_min'] = '-64.98595'
#%% Modify for TCBOTB
if 'TCBOTB' in metadata:
    metadata['TCBOTB']['global_attributes']['source'] = 'Benthic temperature record from Botany Bay, St. Thomas, USVI'
    metadata['TCBOTB']['global_attributes']['location'] = 'Botany Bay'
    metadata['TCBOTB']['global_attributes']['depth'] = '8 m'
    metadata['TCBOTB']['global_attributes']['site_description'] = 'High cover reef near development (Botany Bay) on northside of St. Thomas'
    metadata['TCBOTB']['global_attributes']['coordinates'] = '18.35738, -65.03442'
    metadata['TCBOTB']['global_attributes']['geospatial_lat_max'] = '18.35738'
    metadata['TCBOTB']['global_attributes']['geospatial_lat_min'] = '18.35738'
    metadata['TCBOTB']['global_attributes']['geospatial_lon_max'] = '-65.03442'
    metadata['TCBOTB']['global_attributes']['geospatial_lon_min'] = '-65.03442'
#%% Modify for TCBRWB
if 'TCBRWB' in metadata:
    metadata['TCBRWB']['global_attributes']['source'] = 'Benthic temperature record from Brewers Bay, St. Thomas, USVI'
    metadata['TCBRWB']['global_attributes']['location'] = 'Brewers Bay'
    metadata['TCBRWB']['global_attributes']['depth'] = '6 m'
    metadata['TCBRWB']['global_attributes']['site_description'] = 'High cover Orbicella annularis reef near UVI'
    metadata['TCBRWB']['global_attributes']['coordinates'] = '18.34403, -64.98435'
    metadata['TCBRWB']['global_attributes']['geospatial_lat_max'] = '18.34403'
    metadata['TCBRWB']['global_attributes']['geospatial_lat_min'] = '18.34403'
    metadata['TCBRWB']['global_attributes']['geospatial_lon_max'] = '-64.98435'
    metadata['TCBRWB']['global_attributes']['geospatial_lon_min'] = '-64.98435'
#%% Modify for TCBKIT
if 'TCBKIT' in metadata:
    metadata['TCBKIT']['global_attributes']['source'] = 'Benthic temperature record from Buck Island, St. Thomas, USVI'
    metadata['TCBKIT']['global_attributes']['location'] = 'Buck Island'
    metadata['TCBKIT']['global_attributes']['depth'] = '14 m'
    metadata['TCBKIT']['global_attributes']['site_description'] = 'Fringing reef near north of uninhabited Buck Island, St. Thomas'
    metadata['TCBKIT']['global_attributes']['coordinates'] = '18.27883, -64.89833'
    metadata['TCBKIT']['global_attributes']['geospatial_lat_max'] = '18.27883'
    metadata['TCBKIT']['global_attributes']['geospatial_lat_min'] = '18.27883'
    metadata['TCBKIT']['global_attributes']['geospatial_lon_max'] = '-64.89833'
    metadata['TCBKIT']['global_attributes']['geospatial_lon_min'] = '-64.89833'
#%% Modify for TCCORK
if 'TCCORK' in metadata:
    metadata['TCCORK']['global_attributes']['source'] = 'Benthic temperature record from Coculus Rock, St. Thomas, USVI'
    metadata['TCCORK']['global_attributes']['location'] = 'Coculus Rock'
    metadata['TCCORK']['global_attributes']['depth'] = '7 m'
    metadata['TCCORK']['global_attributes']['site_description'] = 'Fringing reef on basalt near mouth of Benner Bay and Mangrove Lagoon'
    metadata['TCCORK']['global_attributes']['coordinates'] = '18.31257, -64.86058'
    metadata['TCCORK']['global_attributes']['geospatial_lat_max'] = '18.31257'
    metadata['TCCORK']['global_attributes']['geospatial_lat_min'] = '18.31257'
    metadata['TCCORK']['global_attributes']['geospatial_lon_max'] = '-64.86058'
    metadata['TCCORK']['global_attributes']['geospatial_lon_min'] = '-64.86058'
#%% Modify for TCCLGE
if 'TCCLGE' in metadata:
    metadata['TCCLGE']['global_attributes']['source'] = 'Benthic temperature record from College Shoal East, St. Thomas, USVI'
    metadata['TCCLGE']['global_attributes']['location'] = 'College Shoal East'
    metadata['TCCLGE']['global_attributes']['depth'] = '30 m'
    metadata['TCCLGE']['global_attributes']['site_description'] = 'Mesophotic reef with high orbicellid cover inside the Hind Bank Marine Conservation District'
    metadata['TCCLGE']['global_attributes']['coordinates'] = '18.18568, -65.07677'
    metadata['TCCLGE']['global_attributes']['geospatial_lat_max'] = '18.18568'
    metadata['TCCLGE']['global_attributes']['geospatial_lat_min'] = '18.18568'
    metadata['TCCLGE']['global_attributes']['geospatial_lon_max'] = '-65.07677'
    metadata['TCCLGE']['global_attributes']['geospatial_lon_min'] = '-65.07677'
#%% Modify for TCFLTC
if 'TCFLTC' in metadata:
    metadata['TCFLTC']['global_attributes']['source'] = 'Benthic temperature record from Flat Cay, St. Thomas, USVI'
    metadata['TCFLTC']['global_attributes']['location'] = 'Flat Cay'
    metadata['TCFLTC']['global_attributes']['depth'] = '12 m'
    metadata['TCFLTC']['global_attributes']['site_description'] = 'Fringing reef near uninhabited cay southwest of St. Thomas'
    metadata['TCFLTC']['global_attributes']['coordinates'] = '18.31822, -64.99104'
    metadata['TCFLTC']['global_attributes']['geospatial_lat_max'] = '18.31822'
    metadata['TCFLTC']['global_attributes']['geospatial_lat_min'] = '18.31822'
    metadata['TCFLTC']['global_attributes']['geospatial_lon_max'] = '-64.99104'
    metadata['TCFLTC']['global_attributes']['geospatial_lon_min'] = '-64.99104'
#%% Modify for TCGB63
if 'TCGB63' in metadata:
    metadata['TCGB63']['global_attributes']['source'] = 'Benthic temperature record from Ginsburg Fringe, St. Thomas, USVI'
    metadata['TCGB63']['global_attributes']['location'] = 'Ginsburg Fringe'
    metadata['TCGB63']['global_attributes']['depth'] = '63 m'
    metadata['TCGB63']['global_attributes']['site_description'] = 'Lower mesophotic coral reef on well-developed agaricid fringe'
    metadata['TCGB63']['global_attributes']['coordinates'] = '18.1877,-64.95998'
    metadata['TCGB63']['global_attributes']['geospatial_lat_max'] = '18.1877'
    metadata['TCGB63']['global_attributes']['geospatial_lat_min'] = '18.1877'
    metadata['TCGB63']['global_attributes']['geospatial_lon_max'] = '-64.95998'
    metadata['TCGB63']['global_attributes']['geospatial_lon_min'] = '-64.95998'
#%% Modify for TCGMKT
if 'TCGMKT' in metadata:
    metadata['TCGMKT']['global_attributes']['source'] = 'Benthic temperature record from Grammanik Tiger, St. Thomas, USVI'
    metadata['TCGMKT']['global_attributes']['location'] = 'Grammanik Tiger'
    metadata['TCGMKT']['global_attributes']['depth'] = '38 m'
    metadata['TCGMKT']['global_attributes']['site_description'] = 'Mesophotic coral reef at multi-species fish spawning aggregation in the Grammanik Bank fisheries seasonal closed area'
    metadata['TCGMKT']['global_attributes']['coordinates'] = '18.18885, -64.95659'
    metadata['TCGMKT']['global_attributes']['geospatial_lat_max'] = '18.18885'
    metadata['TCGMKT']['global_attributes']['geospatial_lat_min'] = '18.18885'
    metadata['TCGMKT']['global_attributes']['geospatial_lon_max'] = '-64.95659'
    metadata['TCGMKT']['global_attributes']['geospatial_lon_min'] = '-64.95659'
#%% Modify for TCHB40
if 'TCHB40' in metadata:
    metadata['TCHB40']['global_attributes']['source'] = 'Benthic temperature record from Hind Bank, St. Thomas, USVI'
    metadata['TCHB40']['global_attributes']['location'] = 'Hind Bank'
    metadata['TCHB40']['global_attributes']['depth'] = '39 m'
    metadata['TCHB40']['global_attributes']['site_description'] = 'Mesophotic coral reef at red hind (Epinephelus guttatus) fish spawning aggregation in the Hind Bank Marine Conservation District'
    metadata['TCHB40']['global_attributes']['coordinates'] = '18.20217, -65.00158'
    metadata['TCHB40']['global_attributes']['geospatial_lat_max'] = '18.20217'
    metadata['TCHB40']['global_attributes']['geospatial_lat_min'] = '18.20217'
    metadata['TCHB40']['global_attributes']['geospatial_lon_max'] = '-65.00158'
    metadata['TCHB40']['global_attributes']['geospatial_lon_min'] = '-65.00158'
#%% Modify for TCHB30
if 'TCHB30' in metadata:
    metadata['TCHB30']['global_attributes']['source'] = 'Benthic temperature record from Hind Bank, St. Thomas, USVI'
    metadata['TCHB30']['global_attributes']['location'] = 'Hind Bank'
    metadata['TCHB30']['global_attributes']['depth'] = '30 m'
    metadata['TCHB30']['global_attributes']['site_description'] = 'Thermistor string deployment at a minimum 30m depth.  Line swing can cause deepening of sensor.'
    metadata['TCHB30']['global_attributes']['coordinates'] = '18.20217, -65.00158'
    metadata['TCHB30']['global_attributes']['geospatial_lat_max'] = '18.20217'
    metadata['TCHB30']['global_attributes']['geospatial_lat_min'] = '18.20217'
    metadata['TCHB30']['global_attributes']['geospatial_lon_max'] = '-65.00158'
    metadata['TCHB30']['global_attributes']['geospatial_lon_min'] = '-65.00158'
#%% Modify for TCHB20
if 'TCHB20' in metadata:
    metadata['TCHB20']['global_attributes']['source'] = 'Benthic temperature record from Hind Bank, St. Thomas, USVI'
    metadata['TCHB20']['global_attributes']['location'] = 'Hind Bank'
    metadata['TCHB20']['global_attributes']['depth'] = '20 m'
    metadata['TCHB20']['global_attributes']['site_description'] = 'Thermistor string deployment at a minimum of 20m depth. Line swing can cause deepening of sensor.'
    metadata['TCHB20']['global_attributes']['coordinates'] = '18.20217, -65.00158'
    metadata['TCHB30']['global_attributes']['geospatial_lat_max'] = '18.20217'
    metadata['TCHB30']['global_attributes']['geospatial_lat_min'] = '18.20217'
    metadata['TCHB30']['global_attributes']['geospatial_lon_max'] = '-65.00158'
    metadata['TCHB30']['global_attributes']['geospatial_lon_min'] = '-65.00158'
#%% Modify for TCMAGB
if 'TCMAGB' in metadata:
    metadata['TCMAGB']['global_attributes']['source'] = 'Benthic temperature record from Magens Bay, St. Thomas, USVI '
    metadata['TCMAGB']['global_attributes']['location'] = 'Magens Bay'
    metadata['TCMAGB']['global_attributes']['depth'] = '7 m'
    metadata['TCMAGB']['global_attributes']['site_description'] = 'Fringing coral reef on northside of St. Thomas impacted by sedimentation from developed hillsides of the watershed'
    metadata['TCMAGB']['global_attributes']['coordinates'] = '18.37425, -64.93438'
    metadata['TCMAGB']['global_attributes']['geospatial_lat_max'] = '18.37425'
    metadata['TCMAGB']['global_attributes']['geospatial_lat_min'] = '18.37425'
    metadata['TCMAGB']['global_attributes']['geospatial_lon_max'] = '-64.93438'
    metadata['TCMAGB']['global_attributes']['geospatial_lon_min'] = '-64.93438'
#%% Modify for TCSAVA
if 'TCSAVA' in metadata:
    metadata['TCSAVA']['global_attributes']['source'] = 'Benthic temperature record from Savana, St. Thomas USVI'
    metadata['TCSAVA']['global_attributes']['location'] = 'Savana'
    metadata['TCSAVA']['global_attributes']['depth'] = '9 m'
    metadata['TCSAVA']['global_attributes']['site_description'] = 'Fringing coral reef near uninhabited Savana Island'
    metadata['TCSAVA']['global_attributes']['coordinates'] = '18.34064,'
    metadata['TCSAVA']['global_attributes']['geospatial_lat_max'] = '18.34064,-65.08205'
    metadata['TCSAVA']['global_attributes']['geospatial_lat_min'] = '18.34064'
    metadata['TCSAVA']['global_attributes']['geospatial_lon_max'] = '-65.08205'
    metadata['TCSAVA']['global_attributes']['geospatial_lon_min'] = '-65.08205'
#%% Modify for TCSHCS
if 'TCSHCS' in metadata:
    metadata['TCSHCS']['global_attributes']['source'] = 'Benthic temperature record from Seahorse Cottage Shoal, St. Thomas, USVI'
    metadata['TCSHCS']['global_attributes']['location'] = 'Seahorse Cottage Shoal'
    metadata['TCSHCS']['global_attributes']['depth'] = '20 m'
    metadata['TCSHCS']['global_attributes']['site_description'] = 'Isoloated midshelf orbicellid bank reef with high coral cover'
    metadata['TCSHCS']['global_attributes']['coordinates'] = '18.29467,-64.8675'
    metadata['TCSHCS']['global_attributes']['geospatial_lat_max'] = '18.29467'
    metadata['TCSHCS']['global_attributes']['geospatial_lat_min'] = '18.29467'
    metadata['TCSHCS']['global_attributes']['geospatial_lon_max'] = '-64.8675'
    metadata['TCSHCS']['global_attributes']['geospatial_lon_min'] = '-64.8675'
#%% Modify for TCSCAP
if 'TCSCAP' in metadata:
    metadata['TCSCAP']['global_attributes']['source'] = ' Benthic temperature record from South Capella, St. Thomas, USVI'
    metadata['TCSCAP']['global_attributes']['location'] = 'South Capella'
    metadata['TCSCAP']['global_attributes']['depth'] = '20 m'
    metadata['TCSCAP']['global_attributes']['site_description'] = 'Midshelf orbicellid linear reef with high coral cover'
    metadata['TCSCAP']['global_attributes']['coordinates'] = '18.26267, -64.87237'
    metadata['TCSCAP']['global_attributes']['geospatial_lat_max'] = '18.26267'
    metadata['TCSCAP']['global_attributes']['geospatial_lat_min'] = '18.26267'
    metadata['TCSCAP']['global_attributes']['geospatial_lon_max'] = '-64.87237'
    metadata['TCSCAP']['global_attributes']['geospatial_lon_min'] = '-64.87237'
#%% Modify for TCSC35
if 'TCSC35' in metadata:
    metadata['TCSC35']['global_attributes']['source'] = 'Benthic temperature record from South Capella, St. Thomas, USVI'
    metadata['TCSC35']['global_attributes']['location'] = 'South Capella 35m'
    metadata['TCSC35']['global_attributes']['depth'] = '35 m'
    metadata['TCSC35']['global_attributes']['site_description'] = 'Temperature probe only site'
    metadata['TCSC35']['global_attributes']['coordinates'] = '18.26267,-64.87237'
    metadata['TCSC35']['global_attributes']['geospatial_lat_max'] = '18.26267'
    metadata['TCSC35']['global_attributes']['geospatial_lat_min'] = '18.26267'
    metadata['TCSC35']['global_attributes']['geospatial_lon_max'] = '-64.87237'
    metadata['TCSC35']['global_attributes']['geospatial_lon_min'] = '-64.87237'
#%% Modify for TCSWAT
if 'TCSWAT' in metadata:
    metadata['TCSWAT']['global_attributes']['source'] = 'Benthic temperature record from South Water, St. Croix USVI'
    metadata['TCSWAT']['global_attributes']['location'] = 'South Water'
    metadata['TCSWAT']['global_attributes']['depth'] = '20 m'
    metadata['TCSWAT']['global_attributes']['site_description'] = 'Midshelf hardbottom site with diverse fish community'
    metadata['TCSWAT']['global_attributes']['coordinates'] = '18.28068,-64.94592'
    metadata['TCSWAT']['global_attributes']['geospatial_lat_max'] = '18.28068'
    metadata['TCSWAT']['global_attributes']['geospatial_lat_min'] = '18.28068'
    metadata['TCSWAT']['global_attributes']['geospatial_lon_max'] = '-64.94592'
    metadata['TCSWAT']['global_attributes']['geospatial_lon_min'] = '-64.94592'
#%% Modify for TCLSTJ
if 'TCLSTJ' in metadata:
    metadata['TCLSTJ']['global_attributes']['source'] = 'Benthic temperature record from St James, St. Thomas, USVI'
    metadata['TCLSTJ']['global_attributes']['location'] = 'St James'
    metadata['TCLSTJ']['global_attributes']['depth'] = '15 m'
    metadata['TCLSTJ']['global_attributes']['site_description'] = 'Patch reef near developing cay'
    metadata['TCLSTJ']['global_attributes']['coordinates'] = '18.29459, -64.83238'
    metadata['TCLSTJ']['global_attributes']['geospatial_lat_max'] = '18.29459'
    metadata['TCLSTJ']['global_attributes']['geospatial_lat_min'] = '18.29459'
    metadata['TCLSTJ']['global_attributes']['geospatial_lon_max'] = '-64.83238'
    metadata['TCLSTJ']['global_attributes']['geospatial_lon_min'] = '-64.83238'
#%% Modify for TCBKIX
if 'TCBKIX' in metadata:
    metadata['TCBKIX']['global_attributes']['source'] = 'Benthic temperature record from Buck Island, St. Croix, USVI'
    metadata['TCBKIX']['global_attributes']['location'] = 'Buck Island'
    metadata['TCBKIX']['global_attributes']['depth'] = '15 m'
    metadata['TCBKIX']['global_attributes']['site_description'] = 'Orbicella reef southeast of the Buck Island National Park barrier reef within the Virgin Islands National Monument'
    metadata['TCBKIX']['global_attributes']['coordinates'] = '17.78500, -64.60917'
    metadata['TCBKIX']['global_attributes']['geospatial_lat_max'] = '17.78500'
    metadata['TCBKIX']['global_attributes']['geospatial_lat_min'] = '17.78500'
    metadata['TCBKIX']['global_attributes']['geospatial_lon_max'] = '-64.60917'
    metadata['TCBKIX']['global_attributes']['geospatial_lon_min'] = '-64.60917'
#%% Modify for TCBX33
if 'TCBX33' in metadata:
    metadata['TCBX33']['global_attributes']['source'] = 'Benthic temperature record from Buck Island, St. Croix, USVI'
    metadata['TCBX33']['global_attributes']['location'] = 'Buck Island'
    metadata['TCBX33']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCBX33']['global_attributes']['site_description'] = 'Mesophotic reef with high orbicellid cover north of Buck Island within the Virgin Islands National Monument'
    metadata['TCBX33']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCBX33']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCBX33']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCBX33']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCBX33']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCCB08
if 'TCCB08' in metadata:
    metadata['TCCB08']['global_attributes']['source'] = 'Benthic temperature record from Cane Bay, St. Croix, USVI'
    metadata['TCCB08']['global_attributes']['location'] = 'Cane Bay'
    metadata['TCCB08']['global_attributes']['depth'] = '10 m'
    metadata['TCCB08']['global_attributes']['site_description'] = 'Orbicella reef near a dive mooring'
    metadata['TCCB08']['global_attributes']['coordinates'] = '17.77388, -64.81350'
    metadata['TCCB08']['global_attributes']['geospatial_lat_max'] = '17.77388'
    metadata['TCCB08']['global_attributes']['geospatial_lat_min'] = '17.77388'
    metadata['TCCB08']['global_attributes']['geospatial_lon_max'] = '-64.81350'
    metadata['TCCB08']['global_attributes']['geospatial_lon_min'] = '-64.81350'
#%% Modify for TCCB40
if 'TCCB40' in metadata:
    metadata['TCCB40']['global_attributes']['source'] = 'Benthic temperature record from Cane Bay Deep, St. Croix, USVI'
    metadata['TCCB40']['global_attributes']['location'] = 'Cane Bay Deep'
    metadata['TCCB40']['global_attributes']['depth'] = '38 m'
    metadata['TCCB40']['global_attributes']['site_description'] = 'Wall reef closest offshelf from Cane Bay shallow site'
    metadata['TCCB40']['global_attributes']['coordinates'] = '17.77661, -64.81522'
    metadata['TCCB40']['global_attributes']['geospatial_lat_max'] = '17.77661'
    metadata['TCCB40']['global_attributes']['geospatial_lat_min'] = '17.77661'
    metadata['TCCB40']['global_attributes']['geospatial_lon_max'] = '-64.81522'
    metadata['TCCB40']['global_attributes']['geospatial_lon_min'] = '-64.81522'
#%% Modify for TCCB99
if 'TCCB99' in metadata:
    metadata['TCCB99']['global_attributes']['source'] = 'Benthic temperature record from Cane Bay, St. Croix, USVI'
    metadata['TCCB99']['global_attributes']['location'] = 'Cane Bay'
    metadata['TCCB99']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCCB99']['global_attributes']['site_description'] = 'New site description for TCCB99'
    metadata['TCCB99']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCCB99']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCCB99']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCCB99']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCCB99']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCCB67
if 'TCCB67' in metadata:
    metadata['TCCB67']['global_attributes']['source'] = 'Benthic temperature record from Cane Bay, St. Croix, USVI'
    metadata['TCCB67']['global_attributes']['location'] = 'Cane Bay'
    metadata['TCCB67']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCCB67']['global_attributes']['site_description'] = 'New site description for TCCB67'
    metadata['TCCB67']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCCB67']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCCB67']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCCB67']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCCB67']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCCSTL
if 'TCCSTL' in metadata:
    metadata['TCCSTL']['global_attributes']['source'] = 'Benthic temperature record from Castle, St. Croix, USVI'
    metadata['TCCSTL']['global_attributes']['location'] = 'Castle'
    metadata['TCCSTL']['global_attributes']['depth'] = '7 m'
    metadata['TCCSTL']['global_attributes']['site_description'] = 'Haphazard selection along the Tague Bay reef near the closed West Indies Laboratory'
    metadata['TCCSTL']['global_attributes']['coordinates'] = '17.76278, -64.59743'
    metadata['TCCSTL']['global_attributes']['geospatial_lat_max'] = '17.76278'
    metadata['TCCSTL']['global_attributes']['geospatial_lat_min'] = '17.76278'
    metadata['TCCSTL']['global_attributes']['geospatial_lon_max'] = '-64.59743'
    metadata['TCCSTL']['global_attributes']['geospatial_lon_min'] = '-64.59743'
#%% Modify for TCEAGR
if 'TCEAGR' in metadata:
    metadata['TCEAGR']['global_attributes']['source'] = 'Benthic temperature record from Eagle Ray, St. Croix, USVI'
    metadata['TCEAGR']['global_attributes']['location'] = 'Eagle Ray'
    metadata['TCEAGR']['global_attributes']['depth'] = '10 m'
    metadata['TCEAGR']['global_attributes']['site_description'] = 'Dive mooring near mouth of Christiansted Harbor'
    metadata['TCEAGR']['global_attributes']['coordinates'] = '17.7615, -64.6988'
    metadata['TCEAGR']['global_attributes']['geospatial_lat_max'] = '17.7615'
    metadata['TCEAGR']['global_attributes']['geospatial_lat_min'] = '17.7615'
    metadata['TCEAGR']['global_attributes']['geospatial_lon_max'] = '-64.6988'
    metadata['TCEAGR']['global_attributes']['geospatial_lon_min'] = '-64.6988'
#%% Modify for TCGRPD
if 'TCGRPD' in metadata:
    metadata['TCGRPD']['global_attributes']['source'] = 'Benthic temperature record from Great Pond, St. Croix, USVI'
    metadata['TCGRPD']['global_attributes']['location'] = 'Great Pond'
    metadata['TCGRPD']['global_attributes']['depth'] = '6 m'
    metadata['TCGRPD']['global_attributes']['site_description'] = 'Highest presence of Acropora on south shore of St. Croix in the East End Marine Park'
    metadata['TCGRPD']['global_attributes']['coordinates'] = '17.71097, -64.65221'
    metadata['TCGRPD']['global_attributes']['geospatial_lat_max'] = '17.71097'
    metadata['TCGRPD']['global_attributes']['geospatial_lat_min'] = '17.71097'
    metadata['TCGRPD']['global_attributes']['geospatial_lon_max'] = '-64.65221'
    metadata['TCGRPD']['global_attributes']['geospatial_lon_min'] = '-64.65221'
#%% Modify for TCJCKB
if 'TCJCKB' in metadata:
    metadata['TCJCKB']['global_attributes']['source'] = 'Benthic temperature record from Jacks Bay, St. Croix, USVI'
    metadata['TCJCKB']['global_attributes']['location'] = 'Jacks Bay'
    metadata['TCJCKB']['global_attributes']['depth'] = '14 m'
    metadata['TCJCKB']['global_attributes']['site_description'] = 'In East End Marine Park near the southeast tip of St. Croix'
    metadata['TCJCKB']['global_attributes']['coordinates'] = '17.74337, -64.57160'
    metadata['TCJCKB']['global_attributes']['geospatial_lat_max'] = '17.74337'
    metadata['TCJCKB']['global_attributes']['geospatial_lat_min'] = '17.74337'
    metadata['TCJCKB']['global_attributes']['geospatial_lon_max'] = '-64.57160'
    metadata['TCJCKB']['global_attributes']['geospatial_lon_min'] = '-64.57160'
#%% Modify for TCKNGC
if 'TCKNGC' in metadata:
    metadata['TCKNGC']['global_attributes']['source'] = 'Benthic temperature record from Kings Corner, St. Croix, USVI'
    metadata['TCKNGC']['global_attributes']['location'] = 'Kings Corner'
    metadata['TCKNGC']['global_attributes']['depth'] = '17 m'
    metadata['TCKNGC']['global_attributes']['site_description'] = 'Western St. Croix site south of Fredriksted'
    metadata['TCKNGC']['global_attributes']['coordinates'] = '17.69116, -64.90008'
    metadata['TCKNGC']['global_attributes']['geospatial_lat_max'] = '17.69116'
    metadata['TCKNGC']['global_attributes']['geospatial_lat_min'] = '17.69116'
    metadata['TCKNGC']['global_attributes']['geospatial_lon_max'] = '-64.90008'
    metadata['TCKNGC']['global_attributes']['geospatial_lon_min'] = '-64.90008'
#%% Modify for TCLBEM
if 'TCLBEM' in metadata:
    metadata['TCLBEM']['global_attributes']['source'] = 'Benthic temperature record from Lang Bank EEMP, St. Croix, USVI'
    metadata['TCLBEM']['global_attributes']['location'] = 'Lang Bank EEMP'
    metadata['TCLBEM']['global_attributes']['depth'] = '27 m'
    metadata['TCLBEM']['global_attributes']['site_description'] = 'Mesophotic coral reef in EEMP.  Selected haphazardly'
    metadata['TCLBEM']['global_attributes']['coordinates'] = '17.72145, -64.54706'
    metadata['TCLBEM']['global_attributes']['geospatial_lat_max'] = '17.72145'
    metadata['TCLBEM']['global_attributes']['geospatial_lat_min'] = '17.72145'
    metadata['TCLBEM']['global_attributes']['geospatial_lon_max'] = '-64.54706'
    metadata['TCLBEM']['global_attributes']['geospatial_lon_min'] = '-64.54706'

#%% Modify for TCLB99
if 'TCLB99' in metadata:
    metadata['TCLB99']['global_attributes']['source'] = 'Benthic temperature record from Lang Bank, St. Croix, USVI'
    metadata['TCLB99']['global_attributes']['location'] = 'Lang Bank'
    metadata['TCLB99']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCLB99']['global_attributes']['site_description'] = 'New site description for TCLB99'
    metadata['TCLB99']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCLB99']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCLB99']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCLB99']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCLB99']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCLB67
if 'TCLB67' in metadata:
    metadata['TCLB67']['global_attributes']['source'] = 'Benthic temperature record from Lang Bank, St. Croix, USVI'
    metadata['TCLB67']['global_attributes']['location'] = 'Lang Bank'
    metadata['TCLB67']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCLB67']['global_attributes']['site_description'] = 'New site description for TCLB67'
    metadata['TCLB67']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCLB67']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCLB67']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCLB67']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCLB67']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCLBRH
if 'TCLBRH' in metadata:
    metadata['TCLBRH']['global_attributes']['source'] = 'Benthic temperature record from Lang Bank Red Hind FSA, St. Croix, USVI '
    metadata['TCLBRH']['global_attributes']['location'] = 'Land Bank Red Hind FSA'
    metadata['TCLBRH']['global_attributes']['depth'] = '33 m'
    metadata['TCLBRH']['global_attributes']['site_description'] = 'Colocated with a Fish Spawning Aggregation of red hind (Epinephelus guttatus)'
    metadata['TCLBRH']['global_attributes']['coordinates'] = '17.82427, -64.44963'
    metadata['TCLBRH']['global_attributes']['geospatial_lat_max'] = '17.82427'
    metadata['TCLBRH']['global_attributes']['geospatial_lat_min'] = '17.82427'
    metadata['TCLBRH']['global_attributes']['geospatial_lon_max'] = '-64.44963'
    metadata['TCLBRH']['global_attributes']['geospatial_lon_min'] = '-64.44963'
#%% Modify for TCMT24
if 'TCMT24' in metadata:
    metadata['TCMT24']['global_attributes']['source'] = 'Benthic temperature record from Mutton, St. Croix, USVI'
    metadata['TCMT24']['global_attributes']['location'] = 'Mutton Snapper FSA'
    metadata['TCMT24']['global_attributes']['depth'] = '24 m'
    metadata['TCMT24']['global_attributes']['site_description'] = 'Colocation with closed area protecting spawning staging area of muton snapper (Lutjanus analis)'
    metadata['TCMT24']['global_attributes']['coordinates'] = '17.6366,-64.8624'
    metadata['TCMT24']['global_attributes']['geospatial_lat_max'] = '17.6366'
    metadata['TCMT24']['global_attributes']['geospatial_lat_min'] = '17.6366'
    metadata['TCMT24']['global_attributes']['geospatial_lon_max'] = '-64.8624'
    metadata['TCMT24']['global_attributes']['geospatial_lon_min'] = '-64.8624'
if 'TCMT40' in metadata:
    metadata['TCMT40']['global_attributes']['source'] = 'Benthic temperature record from Mutton, St. Croix, USVI'
    metadata['TCMT40']['global_attributes']['location'] = 'Mutton Snapper FSA 40m'
    metadata['TCMT40']['global_attributes']['depth'] = '40 m'
    metadata['TCMT40']['global_attributes']['site_description'] = 'Mesophotic temperature monitoring location just offshelf from Mutton Snapper Site.'
    metadata['TCMT40']['global_attributes']['coordinates'] = '17.6366,-64.8624'
    metadata['TCMT40']['global_attributes']['geospatial_lat_max'] = '17.6366'
    metadata['TCMT40']['global_attributes']['geospatial_lat_min'] = '17.6366'
    metadata['TCMT40']['global_attributes']['geospatial_lon_max'] = '-64.8624'
    metadata['TCMT40']['global_attributes']['geospatial_lon_min'] = '-64.8624'
#%% Modify for TCSR30
if 'TCSR30' in metadata:
    metadata['TCSR30']['global_attributes']['source'] = 'Benthic temperature record from Salt River, St. Croix, USVI'
    metadata['TCSR30']['global_attributes']['location'] = 'Salt River'
    metadata['TCSR30']['global_attributes']['depth'] = '30 m'
    metadata['TCSR30']['global_attributes']['site_description'] = 'Down wall from to Salt River West in deep transects'
    metadata['TCSR30']['global_attributes']['coordinates'] = '17.78523,-64.75917'
    metadata['TCSR30']['global_attributes']['geospatial_lat_max'] = '17.78523'
    metadata['TCSR30']['global_attributes']['geospatial_lat_min'] = '17.78523'
    metadata['TCSR30']['global_attributes']['geospatial_lon_max'] = '-64.75917'
    metadata['TCSR30']['global_attributes']['geospatial_lon_min'] = '-64.75917'
#%% Modify for TCSR99
if 'TCSR99' in metadata:
    metadata['TCSR99']['global_attributes']['source'] = 'Benthic temperature record from Salt River, St. Croix, USVI'
    metadata['TCSR99']['global_attributes']['location'] = 'Salt River'
    metadata['TCSR99']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSR99']['global_attributes']['site_description'] = 'New site description for TCSR99'
    metadata['TCSR99']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSR99']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSR99']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSR99']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSR99']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSR41
if 'TCSR41' in metadata:
    metadata['TCSR41']['global_attributes']['source'] = 'Benthic temperature record from Salt River, St. Croix, USVI'
    metadata['TCSR41']['global_attributes']['location'] = 'Salt River'
    metadata['TCSR41']['global_attributes']['depth'] = '41 m'
    metadata['TCSR41']['global_attributes']['site_description'] = 'Temperature probe only site'
    metadata['TCSR41']['global_attributes']['coordinates'] = '17.78523,-64.75917'
    metadata['TCSR41']['global_attributes']['geospatial_lat_max'] = '17.78523'
    metadata['TCSR41']['global_attributes']['geospatial_lat_min'] = '17.78523'
    metadata['TCSR41']['global_attributes']['geospatial_lon_max'] = '-64.75917'
    metadata['TCSR41']['global_attributes']['geospatial_lon_min'] = '-64.75917'
#%% Modify for TCSR67
if 'TCSR67' in metadata:
    metadata['TCSR67']['global_attributes']['source'] = 'Benthic temperature record from Salt River, St. Croix, USVI'
    metadata['TCSR67']['global_attributes']['location'] = 'Salt River'
    metadata['TCSR67']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSR67']['global_attributes']['site_description'] = 'New site description for TCSR67'
    metadata['TCSR67']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSR67']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSR67']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSR67']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSR67']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSR10
if 'TCSR10' in metadata:
    metadata['TCSR10']['global_attributes']['source'] = 'Benthic temperature record from Salt River, St. Croix, USVI'
    metadata['TCSR10']['global_attributes']['location'] = 'Salt River'
    metadata['TCSR10']['global_attributes']['depth'] = 'Type Depth'
    metadata['TCSR10']['global_attributes']['site_description'] = 'New site description for TCSR10'
    metadata['TCSR10']['global_attributes']['coordinates'] = 'Type Site Coordinates'
    metadata['TCSR10']['global_attributes']['geospatial_lat_max'] = 'Type max lat'
    metadata['TCSR10']['global_attributes']['geospatial_lat_min'] = 'Type min lat'
    metadata['TCSR10']['global_attributes']['geospatial_lon_max'] = 'Type max lon'
    metadata['TCSR10']['global_attributes']['geospatial_lon_min'] = 'Type min lon'
#%% Modify for TCSPTH
if 'TCSPTH' in metadata:
    metadata['TCSPTH']['global_attributes']['source'] = 'Benthic temperature record from Sprat Hole, St. Croix, USVI'
    metadata['TCSPTH']['global_attributes']['location'] = 'Sprat Hole'
    metadata['TCSPTH']['global_attributes']['depth'] = '8 m'
    metadata['TCSPTH']['global_attributes']['site_description'] = 'High density Orbicellia annularis reef near a dive mooring on west St. Croix north of Fredriksted'
    metadata['TCSPTH']['global_attributes']['coordinates'] = '17.734,-64.8954'
    metadata['TCSPTH']['global_attributes']['geospatial_lat_max'] = '17.734'
    metadata['TCSPTH']['global_attributes']['geospatial_lat_min'] = '17.734'
    metadata['TCSPTH']['global_attributes']['geospatial_lon_max'] = '-64.8954'
    metadata['TCSPTH']['global_attributes']['geospatial_lon_min'] = '-64.8954'
#%% Convert CSV data to NetCDF with metadata incorporated
output_folder = r'/Users/gilliancoleman/Smith_Lab/nc_output'

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
