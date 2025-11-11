import pandas as pd
import os
data_dir = r'C:\Users\fatemeh\OneDrive\Desktop\tennis_project'
use_cleaned = True
home_file = 'home_team_cleaned.csv' if use_cleaned else 'home_team.csv'
away_file = 'away_team_cleaned.csv' if use_cleaned else 'away_team.csv'
events_df = pd.read_csv(os.path.join(data_dir, 'second_question_event_cleaning.csv'), dtype=str)
home_df = pd.read_csv(os.path.join(data_dir, home_file), dtype=str)
away_df = pd.read_csv(os.path.join(data_dir, away_file), dtype=str)
events_df['winner_code'] = events_df['winner_code'].str.strip()
home_wins = events_df[events_df['winner_code'] == '1']
away_wins = events_df[events_df['winner_code'] == '2']
home_winners = home_wins.merge(home_df[['match_id', 'full_name', 'country']], on='match_id')
away_winners = away_wins.merge(away_df[['match_id', 'full_name', 'country']], on='match_id')
all_winners = pd.concat([home_winners, away_winners], ignore_index=True)
all_winners = all_winners[all_winners['full_name'] != "NG"]
unique_wins = all_winners.drop_duplicates(subset=['match_id'])
win_counts = unique_wins.groupby(['full_name', 'country']).size().reset_index(name='win_count')
win_counts.to_csv(os.path.join(data_dir, 'question6.csv'), index=False)
print(win_counts.sort_values(by='win_count', ascending=False).head())