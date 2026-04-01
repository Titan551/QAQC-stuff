# TRIM/PLOT.PY

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

# Set paths from config
from config import CONFIG, get_path_for

folder_path = get_path_for("01_HOBO_OUT")
pretrimmed = get_path_for("02_PLOTS/pretrimmed")
posttrimmed = get_path_for("02_PLOTS/posttrimmed")
trimmed_csv = get_path_for("03_TRIMMED_CSVS")


#IMPORT FUNCTIONS
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
    plot_post_trimmed,
    export_trimmed_csvs
)


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

# 16. Plot pre-trimmed data for QC and save
plot_pre_trimmed(df_files, panama_codes, pretrimmed_path)

# 17. Parse deployment datetime strings back into datetime objects (after formatting)
parse_deployment_datetime_strings(deployment_data_dict)

# 18. Trim df_files by deployment date/time ranges
trim_dataframes_by_date(df_files, deployment_data_dict, panama_codes)

# 19. Final trim edges of each dataframe (defaults: drop first 4 and last 5 rows)
final_trim_dataframe_edges(df_files)

# 20. Check if pairs ('a' & 'b', 'c' & 'd', etc.) have same data lengths
check_data_lengths(df_files)

# 21. Plot post-trimmed data for QC and save
plot_post_trimmed(df_files, panama_codes, posttrimmed_path)

# 22. export trimmed files
export_trimmed_csvs(df_files, trimmed_csv)