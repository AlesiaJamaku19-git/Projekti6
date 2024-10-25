import pandas as pd
import os

# Get the directory path where the script is executing
script_path = os.path.dirname(os.path.abspath(__file__))

# Move up one directory and specify the relative path to the target file
target_file_path = os.path.join(os.path.dirname(script_path), "Bronze", "amazon-fine-food-reviews.csv")

print("Path of the file is :", target_file_path)

# Read the CSV file into a DataFrame
data = pd.read_csv(target_file_path)

# Delete the colom Text
data = data.drop('Text', axis=1)

# Drop rows where any column contains a null value
data = data.dropna()

target_file_path = os.path.join(os.path.dirname(script_path), "Silver", "amazon-fine-food-reviews.csv")

# Save the transform data to Solver Folder
data.to_csv(target_file_path, index=False)

print("amazon-fine-food-reviews.csv successfully transfered to Silver/amazon-fine-food-reviews.csv.", target_file_path)
