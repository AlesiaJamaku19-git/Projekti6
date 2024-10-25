import pandas as pd
import os

# Get the directory path where the script is executing
script_path = os.path.dirname(os.path.abspath(__file__))

# Move up one directory and specify the relative path to the target file
target_file_path = os.path.join(os.path.dirname(script_path), "Bronze", "swiggy-restaurants-dataset.json")

print("Path of the file is :", target_file_path)

# Read the CSV file into a DataFrame
data = pd.read_json(target_file_path)

# Delete the colom 
data = data.drop('Wardha', axis=1)
data = data.drop('Washim', axis=1)
data = data.drop('Wayanad', axis=1)

# Drop rows where any column contains a null value
data = data.dropna()


target_file_path = os.path.join(os.path.dirname(script_path), "Silver", "swiggy-restaurants-dataset.json")

# Save the transform data to Solver Folder
data.to_json(target_file_path, index=False)

print("swiggy-restaurants-dataset.json successfully transfered to Silver/swiggy-restaurants-dataset.json.", target_file_path)
