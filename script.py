import pandas as pd
#  to run this script i am using a virtual enviornment and i reccomend same for other for pandas 
# initially you need to create using this command - python3 -m venv myenv
# you can run it using the follwoing command - source myenv/bin/activate
# then normal pip3 script.py
# once it is done a normal 'deactivate' will do the job


# Path to your input TSV file
input_file_path = 'title.basics.tsv'  # Update with the path to your input TSV file

# Path for the output TSV file
output_file_path = 'filtered_title.basics.tsv'  # Update with the path to your output TSV file

try:
    # Read the TSV file
    df = pd.read_csv(input_file_path, sep='\t', dtype=str)
    print("DataFrame Loaded Successfully. Columns:", df.columns)  # Print the columns to verify

    # Apply filtering
    # 1. Filter out rows where 'primaryTitle' is longer than 255 characters
    # 2. Filter out rows where 'isAdult' is not '0' or '1'
    filtered_df = df[
        (df['primaryTitle'].apply(lambda x: len(str(x)) <= 255)) &
        (df['isAdult'].isin(['0', '1']))  # Checks if 'isAdult' contains only '0' or '1'
    ]
    print(f"Filtered DataFrame with {len(filtered_df)} records out of {len(df)} total records.")

    # Save the filtered data back to a new TSV file
    if len(filtered_df) > 1000000:
        filtered_df = filtered_df.iloc[:1000000]    
    filtered_df.to_csv(output_file_path, sep='\t', index=False)

    print("Filtered data has been saved successfully!")

except Exception as e:
    print("An error occurred:", e)





import csv
import json

# Read the TSV file and write to a JSON file with 'tconst' as the key
with open('filtered_title.basics.tsv', 'r') as file:
    tsv_reader = csv.DictReader(file, delimiter='\t')
    # Create a dictionary with 'tconst' as the key
    data = {}
    for row in tsv_reader:
        # Extract 'tconst' and use it as the key for the rest of the data
        key = row.pop('tconst')  # Remove 'tconst' and get its value
        data[key] = row  # The rest of the data becomes the value for this key

    with open('filtered_titled_basics.json', 'w') as jsonfile:
        json.dump(data, jsonfile, indent=4)  