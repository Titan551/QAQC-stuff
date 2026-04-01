#%%
import os

# Base folder
base_dir = r"D:\UVI Dropbox\SMITH LAB TEAM FOLDER\TCRMP\TCRMP_temperature\TCRMP_temperature_database_csv"

# Loop through all subfolders
for folder in os.listdir(base_dir):
    old_path = os.path.join(base_dir, folder)
    
    # Only rename if it's a folder and starts with "Temp_"
    if os.path.isdir(old_path) and folder.startswith("Temp_"):
        # Remove the "Temp_" prefix and replace spaces with underscores
        new_name = folder.replace("Temp_", "").replace(" ", "_")
        new_path = os.path.join(base_dir, new_name)
        
        # Rename
        os.rename(old_path, new_path)
        print(f"Renamed: {folder} → {new_name}")

print("✅ All done.")

# %%
