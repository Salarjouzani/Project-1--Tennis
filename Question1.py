import pandas as pd
from pathlib import Path
from glob import glob
from concurrent.futures import ThreadPoolExecutor
import pyarrow.parquet as pq
from threading import Lock

all_names = set()
lock = Lock()
base_path = Path(r'C:\Users\fatemeh\OneDrive\Desktop\Tennis Schema\extracted')
date_folders = sorted(glob(str(base_path / '2024*')))
def process_folder(folder_path):
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
    executor.map(process_folder, date_folders)
print(f"\nUnique player count from all folders: {len(all_names)}")