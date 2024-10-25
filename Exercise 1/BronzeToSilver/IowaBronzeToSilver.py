import pandas as pd
import os

# Get the directory path where the script is executing
script_path = os.path.dirname(os.path.abspath(__file__))

# Move up one directory and specify the relative path to the target file
target_file_path = os.path.join(os.path.dirname(script_path), "Bronze", "Iowa_Liquor_Sales.csv")

print("Path of the file is :", target_file_path)

# Read the CSV file into a DataFrame
data = pd.read_csv(target_file_path)


# Delete the colom 
data = data.drop('Pack', axis=1)

# Drop rows where any column contains a null value
data = data.dropna()


target_file_path = os.path.join(os.path.dirname(script_path), "Silver", "Iowa_Liquor_Sales.csv")

# Save the transform data to Solver Folder
data.to_csv(target_file_path, index=False)

print("Iowa_Liquor_Sales.csv successfully transfered to Silver/Iowa_Liquor_Sales.csv.", target_file_path)

