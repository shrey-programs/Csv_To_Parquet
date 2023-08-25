import os
import pandas as pd

# Define paths
csv_folder_path = 'csv'
parquet_folder_path = 'parquet'

# Check if parquet folder exists, if not create it
if not os.path.exists(parquet_folder_path):
    os.mkdir(parquet_folder_path)

# Get all the files in the csv folder
all_files = os.listdir(csv_folder_path)

# Filter only the csv files
csv_files = [f for f in all_files if f.endswith('.csv')]

# Convert each CSV file to Parquet
for csv_file in csv_files:
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(os.path.join(csv_folder_path, csv_file))
    
    # Convert the filename from .csv to .parquet
    parquet_file = csv_file.replace('.csv', '.parquet')
    
    # Save the DataFrame as a Parquet file
    df.to_parquet(os.path.join(parquet_folder_path, parquet_file), engine='pyarrow')

print(f"Converted {len(csv_files)} CSV files to Parquet format.")
