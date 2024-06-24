import glob as g
import pandas as pd
import json
from collections import defaultdict
from datetime import datetime

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if len(v) > 0 and isinstance(v[0], dict):
                for idx, item in enumerate(v):
                    items.extend(flatten_dict(item, f"{new_key}{sep}{idx}", sep=sep).items())
            else:
                items.append((new_key, ','.join(map(str, v))))  # Convert list of simple values to a comma-separated string
        elif isinstance(v, str) and ";" in v:
            # Handle string values containing semicolons
            items.append((new_key, v.replace(';', ' ')))  # Replace semicolons with commas
        else:
            items.append((new_key, v))
    return dict(items)

# Function to extract and flatten data from a JSON file
def extract_from_json(file):
    records = []
    try:
        with open(file, 'r') as f:
            for line in f:
                record = json.loads(line)
                flattened_record = flatten_dict(record)
                records.append(flattened_record)
        print(f"Fichier JSON '{file}' extrait avec succès")
    except Exception as e:
        print(f"Erreur lors de l'extraction du fichier JSON '{file}': {e}")
    return pd.DataFrame(records)

# Function to extract data from CSV and JSON files
def extract():
    extracted_data = pd.DataFrame()
    
    # for i in g.glob("*.csv"):
    #     extracted_data = pd.concat([extracted_data, extract_from_csv(i)], ignore_index=True)
    
    for i in g.glob("*.json"):
        json_data = extract_from_json(i)
        # Ensure columns are aligned before concatenation
        extracted_data = pd.concat([extracted_data, json_data], ignore_index=True, sort=False)
    
    return extracted_data

# Function to extract data from a CSV file
def extract_from_csv(file):
    try:
        df = pd.read_csv(file)
        print(f"Fichier CSV '{file}' extrait avec succès")
        return df
    except Exception as e:
        print(f"Erreur lors de l'extraction du fichier CSV '{file}': {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

# Function to load data into a CSV file
def load(targetfile, data_to_file):
    try:
        data_to_file.to_csv(targetfile, index=False)
        print(f"Données chargées avec succès dans le fichier '{targetfile}'")
    except Exception as e:
        print(f"Erreur lors du chargement des données dans le fichier CSV '{targetfile}': {e}")

# Execute the ETL process
current_date_time = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
extracted_df = extract()
load(targetfile=f"output_data_{current_date_time}.csv", data_to_file=extracted_df)
