import pandas as pd
from pathlib import Path
from glob import glob
from concurrent.futures import ThreadPoolExecutor
import pyarrow.parquet as pq
from threading import Lock

# Step 1: Collect unique full names
all_names = set()
lock = Lock()
base_path = Path(r'C:\Users\fatemeh\OneDrive\Desktop\Tennis Schema\extracted')
date_folders = sorted(glob(str(base_path / '2024*')))

def collect_names(folder_path):
    match_dir = Path(folder_path) / 'data/raw/raw_match_parquet'
    match_files = glob(str(match_dir / 'home_team_*.parquet')) + \
                  glob(str(match_dir / 'away_team_*.parquet'))

    local_names = set()
    for f in match_files:
        try:
            schema = pq.read_table(f).schema
            if 'full_name' in schema.names:
                df = pd.read_parquet(f, columns=['full_name']).dropna()
                local_names.update(df['full_name'].tolist())
        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue

    with lock:
        all_names.update(local_names)

with ThreadPoolExecutor(max_workers=18) as executor:
    executor.map(collect_names, date_folders)

print(f"\nUnique player count from all folders: {len(all_names)}")

# Step 2: Map full_name to height
name_to_height = {}
def collect_heights(folder_path):
    match_dir = Path(folder_path) / 'data/raw/raw_match_parquet'
    match_files = glob(str(match_dir / 'home_team_*.parquet')) + \
                  glob(str(match_dir / 'away_team_*.parquet'))

    local_map = {}
    for f in match_files:
        try:
            schema = pq.read_table(f).schema
            if 'full_name' in schema.names and 'height' in schema.names:
                df = pd.read_parquet(f, columns=['full_name', 'height']).dropna()
                for _, row in df.iterrows():
                    name = row['full_name']
                    if name in all_names and name not in name_to_height:
                        local_map[name] = row['height']
        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue

    with lock:
        name_to_height.update(local_map)

with ThreadPoolExecutor(max_workers=18) as executor:
    executor.map(collect_heights, date_folders)

# Step 3: Save to CSV and Excel
df_result = pd.DataFrame(list(name_to_height.items()), columns=['full_name', 'height'])
df_result.to_csv('unique_players_with_height.csv', index=False)
df_result.to_excel('unique_players_with_height.xlsx', index=False)

print(f"\nSaved {len(df_result)} players with height to CSV and Excel.")