#%%
 # CASEY BS
#Average 'a' and 'b' files and calculate variance, then combine all
combined_df_list = []

# Iterate through each site code
for site_code, file_numbers in df_files.items():
    for file_number, identifiers in file_numbers.items():
        if 'a' in identifiers and 'b' in identifiers:
            df_a = identifiers['a']['DataFrame'].copy()
            df_b = identifiers['b']['DataFrame'].copy()

            original_dt_col = "Date Time, GMT-04:00"
            new_dt_col = "Date"

            # Check required columns exist
            if original_dt_col not in df_a.columns or original_dt_col not in df_b.columns:
                print(f"Missing datetime column for {site_code}, File {file_number}")
                continue
            if 'Temp, °C' not in df_a.columns or 'Temp, °C' not in df_b.columns:
                print(f"Missing temperature column for {site_code}, File {file_number}")
                continue

            # Convert and rename datetime column
            df_a[new_dt_col] = pd.to_datetime(df_a[original_dt_col], errors='coerce')
            df_b[new_dt_col] = pd.to_datetime(df_b[original_dt_col], errors='coerce')

            # Compute average and variance
            avg_temp = (df_a['Temp, °C'].astype(float) + df_b['Temp, °C'].astype(float)) / 2
            var_temp = ((df_a['Temp, °C'].astype(float) - avg_temp)**2 + 
                        (df_b['Temp, °C'].astype(float) - avg_temp)**2) / 2

            df_avg = pd.DataFrame({
                new_dt_col: df_a[new_dt_col],
                'Temp': avg_temp,
                'Temp_Variance': var_temp,
                'Site_Code': site_code,
                'File_Number': file_number
            })

            combined_df_list.append(df_avg)

        else:
            print(f"Only one file for Site: {site_code}, File Number: {file_number}, skipping averaging.")

# Combine all averaged data
if combined_df_list:
    master_df = pd.concat(combined_df_list, ignore_index=True)
    print(f"Combined DataFrame shape: {master_df.shape}")
else:
    print("No averaged data to combine.")

#%%
# Ensure 'Date' is datetime
master_df['Date'] = pd.to_datetime(master_df['Date'])

# Sort by date
master_df.sort_values('Date', inplace=True)

# Optional: Resample to hourly or daily average to smooth the plot (uncomment if needed)
# master_df = master_df.set_index('Date').resample('D').mean().reset_index()

# Plot
plt.figure(figsize=(14, 6))

# Plot temperature
plt.plot(master_df['Date'], master_df['Temp'], label='Average Temperature', color='blue', alpha=0.7)

plt.xlabel('Date')
plt.ylabel('Temperature (°C)')
#plt.xlim(pd.Timestamp('2019-01-01'), pd.Timestamp('2020-12-31'))
plt.title('Averaged Temperature for Black Point')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
