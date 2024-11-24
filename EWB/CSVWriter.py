import csv
import pandas as pd
import os
import re

def create_csv(file_name, *args):
    with open(file_name, mode='a', newline='') as file:
        writer = csv.writer(file)
        # Write each row as separate entries
        writer.writerow(args)



data = {'connections': ["[('from: PowerTransformer{32318718|COOK-MARKET}', 'to: Fuse{48864|684090}'), ('from: PowerTransformer{32318718|COOK-MARKET}', 'to: Disconnector{734076|9016408}'), ('from: PowerTransformer{32318718|COOK-MARKET}', 'to: Junction{32318766}')]"]}
df = pd.DataFrame(data)


def reformat_connections(connections):

    cleaned_str = re.sub(r"^\[\(|\)\]$", "", connections)
    cleaned_str = cleaned_str.replace("'from:", "from:")
    cleaned_str = cleaned_str.replace("', '", ",")
    cleaned_str = cleaned_str.replace("')", "")
    cleaned_str = cleaned_str.replace("'to:", "to:")
    cleaned_str = cleaned_str.replace(", (from:", ";from:")
    cleaned_str = cleaned_str.replace("}'", "}")
    print(f"cleaned_str: {cleaned_str}")
    
    pairs = cleaned_str.split(";")
    print(pairs)
    pairs = "\n".join(pairs)
    print(pairs)


# Apply the function to the 'connections' column
# df['connections'] = df['connections'].apply(reformat_connections)
