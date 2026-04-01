#
import argparse
import os
import glob
import pandas as pd
import xarray as xr
import yaml

#python Net_CDF.py --input ./csvs --output ./netcdfs --metadata ./metadata

def load_site_metadata(metadata_folder, site_code):
    filepath = os.path.join(metadata_folder, f"{site_code}.yml")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Metadata file not found for site: {site_code}")
    with open(filepath, 'r') as f:
        full_yaml = yaml.safe_load(f)
    if site_code not in full_yaml:
        raise KeyError(f"Site code {site_code} not found in YAML file")
    return full_yaml[site_code]


def extract_site_code(filename):
    parts = os.path.basename(filename).split('_')
    return parts[1] if len(parts) > 1 else os.path.splitext(filename)[0]

def make_netcdf(df, site_code, global_attrs, var_attrs, output_path):
    # Convert time to datetime and set index
    df['Time'] = pd.to_datetime(df['Time'])
    df.set_index('Time', inplace=True)

    # Create Dataset
    ds = xr.Dataset.from_dataframe(df)

    # Assign time coordinate as seconds since epoch
    ds = ds.assign_coords(Time=("Time", df.index.astype("datetime64[s]").astype(int)))
    if 'Time' in var_attrs:
        ds['Time'].attrs.update(var_attrs['Time'])

    # Coordinates from metadata
    lat = float(global_attrs['geospatial_lat_max'])
    lon = float(global_attrs['geospatial_lon_max'])
    depth_val = float(global_attrs['depth'].split()[0])  # e.g., '9 m' → 9

    ds['latitude'] = xr.DataArray([lat], dims="latitude", attrs={
        'standard_name': 'latitude',
        'units': 'degrees_north',
        'axis': 'Y'
    })

    ds['longitude'] = xr.DataArray([lon], dims="longitude", attrs={
        'standard_name': 'longitude',
        'units': 'degrees_east',
        'axis': 'X'
    })

    ds['depth'] = xr.DataArray([depth_val], dims="depth", attrs=var_attrs.get('depth', {
        'standard_name': 'depth',
        'units': 'm',
        'positive': 'down',
        'axis': 'Z'
    }))

    # Add attributes
    # Fill time coverage and creation date
    start_time = df.index.min().isoformat()
    end_time = df.index.max().isoformat()
    date_created = pd.Timestamp.utcnow().isoformat()

    # Inject dynamic time attributes
    global_attrs = global_attrs.copy()  # Don't modify original
    global_attrs['time_coverage_start'] = start_time
    global_attrs['time_coverage_end'] = end_time
    global_attrs['date_created'] = date_created

    ds.attrs.update(global_attrs)

    for var, attrs in var_attrs.items():
        if var in ds.variables:
            ds[var].attrs.update(attrs)

    # Save NetCDF
    ds.to_netcdf(output_path)
    print(f"Saved: {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Convert site CSVs to CF-compliant NetCDF using YAML metadata.")
    parser.add_argument('--input', required=True, help='Folder containing input CSV files')
    parser.add_argument('--output', required=True, help='Folder to save NetCDF files')
    parser.add_argument('--metadata', required=True, help='Folder containing site YAML metadata files')
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    csv_files = glob.glob(os.path.join(args.input, '*.csv'))

    for csv_file in csv_files:
        site_code = extract_site_code(csv_file)
        print(f"Processing site: {site_code}")

        df = pd.read_csv(csv_file)

        # Rename columns to match expected variable names
        df.rename(columns={
            '#': 'Number',
            'Date Time, UTC-04:00': 'Time',
            'Temp, °C': 'Temperature'
        }, inplace=True)

        site_metadata = load_site_metadata(args.metadata, site_code)
        global_attrs = site_metadata.get('global_attributes', {})
        var_attrs = site_metadata.get('variable_attributes', {})

        base_name = os.path.splitext(os.path.basename(csv_file))[0]
        output_path = os.path.join(args.output, f"{base_name}.nc")

        make_netcdf(df, site_code, global_attrs, var_attrs, output_path)

if __name__ == '__main__':
    main()
