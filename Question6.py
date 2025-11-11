import pandas as pd
df = pd.read_csv('question6.csv')
country_wins = df.groupby('country')['win_count'].sum().reset_index()
country_wins = country_wins.sort_values(by='win_count', ascending=False)
country_wins.to_csv('question6_country_ranking.csv', index=False)
print("Countries Ranked by Total Match Wins:")
print(country_wins.head())
print("\n\nthe most successful is: \n",country_wins.head(1))