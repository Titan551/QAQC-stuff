# NCPLOT.PY

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

# Set paths from config
from config import CONFIG, get_path_for

ready_folder = get_path_for("05_READY")
save_dir = get_path_for("02_plot/ready")
deployment_log_path = os.path.join(CONFIG['BASE_DIRECTORY'],"Temperature_UVI_deployment_log.csv")

#%% IMPORT FUNCTIONS
from QAQC_HELPER_FUNCTIONS import (
    import_ready,
    get_usvi_site_codes,
    get_panama_site_codes,
    load_structured_dataframes,
    plot_temperature_time_series,
    merged_dict_add,
    plot_merged_temperatures,
    filter_deployment_log,
    check_unmatched_filenames,
    validate_time_columns,
    convert_deployment_log_datetime,
    create_deployment_data_dict,
    format_deployment_datetimes,
    print_start_end_times,
    generate_trimmed_filenames,
    create_and_save_offload_plots
    
)

# 1. Get ready files
csv_files = import_ready(ready_folder)

# 2. Get site codes lists
usvi_codes = get_usvi_site_codes()
panama_codes = get_panama_site_codes()

# 4. Load CSVs into nested dict structure
df_files = load_structured_dataframes(csv_files, usvi_codes, panama_codes)

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

# 30. Plot temperature time series for all data (post-trim and cleaning)
plot_temperature_time_series(df_files, panama_codes)

# 32. Add the files that have "merged" in the name to merged dict
merged_offset_data = merged_dict_add(df_files)

# 33. Plot merged temperature time series for each site from merged data
plot_merged_temperatures(merged_offset_data, panama_codes)

# 34. Print start and end times for each dataframe and matching deployment data
print_start_end_times(df_files, panama_codes, deployment_data_dict)

# 35. Generate and print trimmed filenames for 'a' files and merged files (no saving)
generate_trimmed_filenames(df_files, merged_offset_data, panama_codes)

# 37. Create and save plots from exported CSV files in exported_folder_path to save_dir
create_and_save_offload_plots(ready_folder, save_dir, panama_codes)