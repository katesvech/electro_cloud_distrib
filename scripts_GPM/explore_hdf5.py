# -*- coding: utf-8 -*-
"""
Created on Fri Dec 20 19:02:17 2024

@author: kate
"""
import h5py

input_folder = 'F:/2024_Lightning_Analysis/GPM_data/100_100_km_all_778_initial_CMB/'
input_file = input_folder + '2B-CS-43E40N44E40N.GPM.DPRGMI.CORRA2022.20180513-S225555-E225608.023900.V07A' +'.HDF5'

def explore_hdf5_file(file_path):

    def print_structure(name, obj):
        """Callback to print the structure of the file."""
        if isinstance(obj, h5py.Group):
            print(f"Group: {name}")
        elif isinstance(obj, h5py.Dataset):
            print(f"Dataset: {name}, Shape: {obj.shape}, Dtype: {obj.dtype}")

    try:
        with h5py.File(file_path, 'r') as hdf_file:
            print(f"Exploring file: {file_path}\n")
            hdf_file.visititems(print_structure)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    explore_hdf5_file(input_file)
