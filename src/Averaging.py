# Averaging.py

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

# Set paths from config
from config import CONFIG, get_path_for

folder_path = get_path_for("01_HOBO_OUT")
review_folder = get_path_for("04_TOREVIEW")
trimmed_csv = get_path_for("03_TRIMMED_CSVS")
deployment_log_path = os.path.join(CONFIG['BASE_DIRECTORY'],"Temperature_UVI_deployment_log.csv")
output_folder = ("05_READY")

#%% IMPORT FUNCTIONS
from QAQC_HELPER_FUNCTIONS import (
    import_trimmed
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
    offload_drifting_files
    plot_merged_temperatures,
    print_start_end_times,
    generate_trimmed_filenames,
    save_offload_files,
    create_and_save_offload_plots,
    trim_dataframe
)

#%%
# 1. Get CSV files in folder_path
csv_files = import_trimmed(trimmed_csv)

# 2. Get site codes lists
usvi_codes = get_usvi_site_codes()
panama_codes = get_panama_site_codes()

# 4. Load CSVs into nested dict structure
df_files = load_structured_dataframes(csv_files, usvi_codes, panama_codes)

# 8. Report missing 'a' identifiers in df_files
report_missing_a_identifiers(df_files)

# 9. Reassign offset identifiers 'a' and 'b' to 'c' and 'd' if needed
reassign_offset_identifiers(df_files, panama_codes)

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

# 28. Report NaN counts in 'a' dfs for flagged calculations
report_nan_temperature_differences(df_files, calculations)

# 31. Merge offset files 'c' and 'd' for each site, file number, returning merged dict
merged_offset_data = merge_offset_files(df_files, panama_codes)

# 32. Plot offset agreement scatter plots comparing 'c' and 'd' logger temperatures
offset_stats, drifting = plot_offset_agreement(df_files, panama_codes)

# offloading the merged files that are drifting for review
offload_drifting_files(drifting, review_folder)

# 26. Save flagged files as CSV to your calculations_folder output
save_flagged_files(calc_df_files, review_folder)

# 27. Average temperature between 'a' and 'b' files if difference below threshold (0.2°C)
average_temperature_if_close(df_files)

# 29. Drop extra columns, keep only necessary ones (#, date_col, Temp)
drop_extra_columns(df_files, panama_codes)

# 36. Save 'a' files and merged files as CSVs to output folder with adjusted columns
save_offload_files(df_files, merged_offset_data, panama_codes, output_folder)

