import pandas as pd
from pathlib import Path
from glob import glob
from concurrent.futures import ThreadPoolExecutor
import pyarrow.parquet as pq
from threading import Lock

# Base path
base_path = Path(r'C:\Users\fatemeh\OneDrive\Desktop\Tennis Schema\extracted')
date_folders = sorted(glob(str(base_path / '2024*')))

# Load original (uncleaned) player data
home_df = pd.read_csv(r'C:\Users\fatemeh\OneDrive\Desktop\home_team.csv', dtype=str)
away_df = pd.read_csv(r'C:\Users\fatemeh\OneDrive\Desktop\away_team.csv', dtype=str)

# Thread-safe odds container
odds_rows = []
lock = Lock()

def process_odds_folder(folder_path):
    odds_dir = Path(folder_path) / 'data/raw/raw_odds_parquet'
    odds_files = glob(str(odds_dir / '*.parquet'))

    local_odds = []
    for file in odds_files:
        try:
            df = pd.read_parquet(file, engine='pyarrow')
            df = df[(df['market_name'] == 'full_time') & (df['winnig'] == 1)]
            df = df[['match_id', 'choice_name']]
            local_odds.append(df)
        except Exception as e:
            print(f"Error reading odds file {file}: {e}")

    with lock:
        odds_rows.extend(local_odds)

# Run in parallel
with ThreadPoolExecutor(max_workers=18) as executor:
    executor.map(process_odds_folder, date_folders)

# Combine odds data
odds_df = pd.concat(odds_rows, ignore_index=True)

# Ensure match_id is string
odds_df['match_id'] = odds_df['match_id'].astype(str)
home_df['match_id'] = home_df['match_id'].astype(str)
away_df['match_id'] = away_df['match_id'].astype(str)

# Merge winners with player names
home_wins = odds_df[odds_df['choice_name'] == '1'].merge(home_df[['match_id', 'full_name']], on='match_id')
away_wins = odds_df[odds_df['choice_name'] == '2'].merge(away_df[['match_id', 'full_name']], on='match_id')

# Combine and filter out "NG"
all_winners = pd.concat([home_wins, away_wins], ignore_index=True)
all_winners = all_winners[all_winners['full_name'] != "NG"]

# Count unique match wins per player
unique_wins = all_winners.drop_duplicates(subset=['match_id', 'full_name'])
win_counts = unique_wins['full_name'].value_counts().reset_index()
win_counts.columns = ['player_name', 'win_count']

# Show top winner
top_winner = win_counts.iloc[0]
print(f"🏆 Player with the most unique match wins: {top_winner['player_name']} ({top_winner['win_count']} wins)")

