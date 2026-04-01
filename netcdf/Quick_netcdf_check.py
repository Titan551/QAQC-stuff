# QUICK .NC CHECK

#%%
import os
import xarray as xr
import pandas as pd

# Directory containing NetCDF files
nc_dir = r'D:\UVI Dropbox\SMITH LAB TEAM FOLDER\TCRMP\TCRMP_temperature\TCRMP_temperature_home_July2025\Cole_test\annual\06_NETCDF'

# Loop through all .nc files in the directory
for file_name in os.listdir(nc_dir):
    if file_name.endswith('.nc'):
        file_path = os.path.join(nc_dir, file_name)

        # Open the NetCDF file
        nc_data = xr.open_dataset(file_path)
        
        # Convert to DataFrame
        df = nc_data.to_dataframe().reset_index()
        
        # Print file name and the DataFrame (or save to CSV)
        print(f"Data from file: {file_name}")
        print(df.head())  # Display first few rows of the DataFrame
        
        # Optionally save to CSV
        #output_csv = os.path.join(nc_dir, file_name.replace('.nc', '.csv'))
        #df.to_csv(output_csv, index=False)
       #print(f"Saved {output_csv}")
        
        # Close the xarray dataset
        nc_data.close()
        
# %%
import os
import xarray as xr
import json

# Directory containing NetCDF files
nc_dir = r'C:\UVI\QAQC stuff\netcdf\netcdfs'

# Output JSON file to save the attributes
output_json = os.path.join(nc_dir, 'netcdf_metadata.json')

# Dictionary to store metadata for all files
all_metadata = {}

# Loop through all .nc files in the directory
for file_name in os.listdir(nc_dir):
    if file_name.endswith('.nc'):
        file_path = os.path.join(nc_dir, file_name)

        # Open the NetCDF file
        nc_data = xr.open_dataset(file_path)

        # Collect metadata for the current file
        file_metadata = {
            "global_attributes": nc_data.attrs,  # Global attributes
            "variables": {}  # Variable-specific metadata
        }

        # Loop through variables and extract metadata
        for var in nc_data.data_vars:
            file_metadata["variables"][var] = {
                "attributes": nc_data[var].attrs,  # Variable attributes
                "shape": nc_data[var].shape,  # Variable dimensions
                "dimensions": list(nc_data[var].dims)  # Dimensions
            }

        # Add metadata for the current file to the main dictionary
        all_metadata[file_name] = file_metadata

        # Close the dataset
        nc_data.close()

# Export all metadata to a JSON file
with open(output_json, 'w') as json_file:
    json.dump(all_metadata, json_file, indent=4)

print(f"Metadata exported to: {output_json}")
# %%
