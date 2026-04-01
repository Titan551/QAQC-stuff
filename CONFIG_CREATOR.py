#%% IMPORTS
import os
import yaml

#%% PATHS
base_data_folder = r"D:\UVI Dropbox\SMITH LAB TEAM FOLDER\TCRMP\TCRMP_temperature\TCRMP_temperature_home_July2025\2025\PBL"
deployment_log_path = r"D:\UVI Dropbox\SMITH LAB TEAM FOLDER\TCRMP\TCRMP_temperature\TCRMP_temperature_home_July2025\Temperature_UVI_deployment_log.csv"
config_output_folder = r"D:\UVI Dropbox\SMITH LAB TEAM FOLDER\TCRMP\TCRMP_temperature\TCRMP_temperature_home_July2025\src"

# GENERATE CONFIG FILE NAME
# Split base_data_folder and extract last two parts
parts = os.path.normpath(base_data_folder).split(os.sep)
folder_suffix = f"{parts[-2]}_{parts[-1]}"
config_name = f"config_{folder_suffix}.yml"

# BUILD CONFIG DICT
config = {}

# Static paths
config["deployment_log_path"] = deployment_log_path
config["os_path"] = base_data_folder

# Add all subfolder paths
for item in os.listdir(base_data_folder):
    full_path = os.path.join(base_data_folder, item)
    if os.path.isdir(full_path):
        key = item.lower().replace(" ", "_") + "_path"
        config[key] = full_path.replace("\\", "/")

# SAVE CONFIG TO YAML
os.makedirs(config_output_folder, exist_ok=True)
config_path = os.path.join(config_output_folder, config_name)

with open(config_path, "w") as f:
    yaml.dump(config, f, sort_keys=False)

print(f"✅ Config saved to: {config_path}")

# %%
