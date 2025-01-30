import os
import json

# Directory containing the JSON files
directory = "samples/nl2sql/columns"

# Attribute to add
new_attribute = {
    "datasource": "adventureworks"
}

# Iterate over all files in the directory
for filename in os.listdir(directory):
    if filename.endswith(".json"):
        file_path = os.path.join(directory, filename)
        
        # Read the existing content of the JSON file
        with open(file_path, "r") as file:
            data = json.load(file)
        
        # Add the new attribute
        data.update(new_attribute)
        
        # Write the updated content back to the file
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)

print("All JSON files have been updated.")

