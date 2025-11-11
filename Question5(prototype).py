import pandas as pd
import matplotlib.pyplot as plt
df = pd.read_csv('question4_time.csv')
period_cols = [f'period_{i}' for i in range(1, 6)]
df[period_cols] = df[period_cols].fillna(0).astype(int)
df['set_count'] = df[period_cols].gt(0).sum(axis=1)
df = df[df['set_count'] > 0]
df[['match_id', 'set_count']].to_csv('question5_set_count.csv', index=False)
set_distribution = df['set_count'].value_counts().sort_index()
labels = [f'{i} set{"s" if i != 1 else ""}' for i in set_distribution.index]
sizes = set_distribution.values
plt.figure(figsize=(7, 7))
plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=plt.cm.Paired.colors)
plt.axis('equal')  
plt.show()