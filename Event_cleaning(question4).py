import pandas as pd
from pathlib import Path
from glob import glob
from concurrent.futures import ThreadPoolExecutor
import pyarrow.parquet as pq
from threading import Lock

# Base path to your extracted data
base_path = Path(r'C:\Users\fatemeh\OneDrive\Desktop\Tennis Schema\extracted')
date_folders = sorted(glob(str(base_path / '2024*')))

# Columns to extract from event files
event_columns = [
    'match_id', 'start_datetime', 'current_period_start_timestamp'
]

# Thread-safe container
event_rows = []
lock = Lock()

def safe_read_event(file_path):
    try:
        schema = pq.read_table(file_path).schema
        available_cols = [col for col in event_columns if col in schema.names]
        df = pd.read_parquet(file_path, columns=available_cols)
        for col in event_columns:
            if col not in df.columns:
                df[col] = "NG"  # Fill missing columns
        df.fillna("NG", inplace=True)  # Fill missing values
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def process_event_folder(folder_path):
    match_dir = Path(folder_path) / 'data/raw/raw_match_parquet'
    event_files = glob(str(match_dir / 'event_*.parquet'))

    local_event = []
    for file in event_files:
        df = safe_read_event(file)
        if df is not None:
            local_event.append(df)

    with lock:
        event_rows.extend(local_event)

# Run in parallel
with ThreadPoolExecutor(max_workers=18) as executor:
    executor.map(process_event_folder, date_folders)

# Combine all dataframes
event_df = pd.concat(event_rows, ignore_index=True)

# Save to CSV
event_df.to_csv('event_question4.csv',index=False)

print("✅ event.csv created successfully with missing values filled as 'NG'")