# Databricks notebook source
# Retrieve raw files
def get_raw_files():
    # List of file paths
    raw_file_paths = dbutils.fs.ls("/mnt/staging/raw")
    
    # Extract the file names
    raw_file_names = [file_info.name for file_info in raw_file_paths]
    return raw_file_names

# Retrieve processed files
def get_processed_files():
    # List of file paths
    processed_file_paths = dbutils.fs.ls("/mnt/staging/processed")
    
    # Extract the file names
    processed_file_names = [file_info.name for file_info in processed_file_paths]
    return processed_file_names

# COMMAND ----------

# Archive logic
def archive_files(archive_directory, source_directory, file_names):
    import os
    import sys
    from datetime import datetime

    # Check if there are five files
    if len(file_names) == 5:
        # Move (rename) the second and third files to the archive
        for i in range(0, 3):
            file_to_move = file_names[i]
            source_path = os.path.join(source_directory, file_to_move)
            destination_path = os.path.join(archive_directory, f'archive_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}-'+file_to_move)
            dbutils.fs.mv(source_path, destination_path)

            print(file_to_move+' has been moved to archive files')
    else:
        print("There are not enough files for archiving.")


# COMMAND ----------

# Archive processed files

# Define the archive directory
archive_directory = '/mnt/staging/processed_archive/'
processed_directory = '/mnt/staging/processed/'

processed_file_names = get_processed_files()

archive_files(archive_directory, processed_directory, processed_file_names)


# COMMAND ----------

# Archive raw files

# Define the archive directory
archive_directory = '/mnt/staging/raw_archive/'
raw_directory = '/mnt/staging/raw/'

raw_file_names = get_raw_files()

archive_files(archive_directory, raw_directory, raw_file_names)
