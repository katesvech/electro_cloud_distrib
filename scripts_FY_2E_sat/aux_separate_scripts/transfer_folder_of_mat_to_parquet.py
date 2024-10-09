import h5py
import os
import numpy as np
import pandas as pd

# Path setup
mat_files_way = 'E:/2024_Lightning_Analysis/FY-2E_data/'
# folder_name = '2018_part_0_small'
folder_name = '2018_part_2_of_6'
mat_files_folder = mat_files_way + folder_name + '/'

mat_files = [f for f in os.listdir(mat_files_folder) if f.endswith('.mat')]

if mat_files:
    first_mat_file = mat_files[0]
    print("First .mat file:", first_mat_file)
else:
    print("No .mat files found in the specified folder.")

file_name = mat_files_folder + first_mat_file

# Preparing to capture skipped files and errors
skipped_files = 0
skipped_files_list = []

# Get dataset names from the example file
with h5py.File(file_name, 'r') as mat_file:
    TSC = mat_file['TSC']

# Writing dataset names (variables) within 'TSC'
def get_dataset_names(name, obj):
    if isinstance(obj, h5py.Dataset) and name.startswith('TSC/'):
        dataset_name = name.split('/')[-1]  # Extract dataset name
        dataset_names.append(dataset_name)

dataset_names = []
with h5py.File(file_name, 'r') as mat_file:
    mat_file.visititems(get_dataset_names)

print("Names of the datasets (variables) within the 'TSC' group:")
print(dataset_names)

# Setup output folder and file
output_folder = 'E:/2024_Lightning_Analysis/FY-2E_data/data_extracted'
os.makedirs(output_folder, exist_ok=True)

indices = [0, 3, 5, 8, 11, 12, 13, 14]  # Selected cloud data variables
lightning_index = 18  # Variable for lightning discharge data

# List all .mat files in the folder
mat_files = [f for f in os.listdir(mat_files_folder) if f.endswith('.mat')]

if not mat_files:
    raise ValueError("No .mat files found in the specified folder.")

variable_names = [dataset_names[i] for i in indices]
lightning_var_name = dataset_names[lightning_index]
first_file_prefix = mat_files[0][:4]
output_parquet = os.path.join(output_folder, f'{folder_name}_ALL_data_with_lightning.parquet')

data_rows = []
cloud_number = 1

# Iterate over .mat files and handle exceptions
for mat_file_name in mat_files:
    mat_file_path = os.path.join(mat_files_folder, mat_file_name)

    try:
        with h5py.File(mat_file_path, 'r') as mat_file:
            TSC = mat_file['TSC']
            file_name_without_ext = mat_file_name.replace('.mat', '')

            # Loop through all rows (clouds) in the dataset
            num_rows = TSC[variable_names[0]].shape[0]
            for i in range(num_rows):
                cloud_data = [file_name_without_ext, cloud_number]
                for j, var_name in enumerate(variable_names):
                    dataset = TSC[var_name]
                    if indices[j] == 3:  # For the variable with index 3
                        data = dataset[i, :]
                        if isinstance(data[0], h5py.h5r.Reference):
                            actual_data = mat_file[data[0]][()]
                        else:
                            actual_data = data
                        actual_data = np.array(actual_data).flatten()
                       
                        if actual_data.size == 2:
                            # Add both values as separate columns
                            cloud_data.extend([actual_data[0], actual_data[1]])
                            # cloud_data.append(actual_data[0])
                            # print(cloud_data, '\n')
                            # cloud_data.append(actual_data[1])
                            # print(cloud_data, '\n', '\n')
                        else:
                            raise ValueError(f"Expected an array of size 2, got {actual_data.size}.")
                    else:
                        data = dataset[i, :]
                        if isinstance(data[0], h5py.h5r.Reference):
                            actual_data = mat_file[data[0]][()]
                        else:
                            actual_data = data[0]
                        actual_data = np.array(actual_data).flatten()
                        cloud_data.append(actual_data[0])

                # Extract lightning discharge data
                lightning_references = TSC[lightning_var_name][i, :]
                if isinstance(lightning_references[0], h5py.h5r.Reference):
                    lightning_data = mat_file[lightning_references[0]][()]
                    lightning_data = np.array(lightning_data)
                    if lightning_data.shape[0] != 14:
                        raise ValueError(f"Expected 14 parameters for each lightning, got {lightning_data.shape[0]}.")
                    num_discharges = lightning_data.shape[1]
                    for d in range(num_discharges):
                        discharge_info = lightning_data[:, d]
                        row_data = cloud_data + discharge_info.tolist()
                        data_rows.append(row_data)

                cloud_number += 1

    except (OSError, ValueError) as e:
        print(f"Skipping file {mat_file_name} due to error: {e}")
        skipped_files += 1
        skipped_files_list.append(mat_file_name)

# Create DataFrame from collected rows
column_names = ['File_Name', 'Cloud_Number']

# Add variable names for selected indices, handling Center_lonlat (index 3) separately
for j, var_name in enumerate(variable_names):
    if indices[j] == 3:
        # For Center_lonlat, add two columns: Center_lonlat_1 and Center_lonlat_2
        column_names.append(f"{var_name}_1")
        column_names.append(f"{var_name}_2")
    else:
        # For all other variables, just add their names directly
        column_names.append(var_name)

# Append additional lightning-related columns
column_names += [
    'Year', 'Month', 'Day', 'Hour', 'Minute', 'Second',
    'Latitude', 'Longitude', 'Error of location', 'Number of discharges',
    'Number of day', 'Lightning is in cloud', 'Lightning is in ellipse', 'TBB near lightning'
]

# Check the final column names
print(column_names)


# Ensure cloud_data correctly matches the number of columns (26 total columns)
df = pd.DataFrame(data_rows, columns=column_names)

# Write DataFrame to Parquet file
df.to_parquet(output_parquet, engine='pyarrow')

# Print summary and log skipped files
print(f"\nData from {len(mat_files) - skipped_files} .mat files written to {output_parquet}")
print(f"{skipped_files} files were skipped due to errors.")

if skipped_files > 0:
    print("\nThe following files were skipped due to errors:")
    for skipped_file in skipped_files_list:
        print(skipped_file)
