import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to database
conn = sqlite3.connect('cricket_analytics.db')

# Load data from each table
test = pd.read_sql("SELECT * FROM test_matches", conn)
odi = pd.read_sql("SELECT * FROM odi_matches", conn)
t20 = pd.read_sql("SELECT * FROM t20_matches", conn)
conn.close()

# Combine all matches
all_matches = pd.concat([test, odi, t20])

# 1. Matches by Format (Pie Chart)
format_counts = all_matches['match_type'].value_counts()
plt.figure(figsize=(8, 8))
plt.pie(format_counts, labels=format_counts.index, autopct='%1.1f%%')
plt.title('Matches by Format')
plt.savefig('1_matches_by_format.png')
plt.close()

# 2. Top Teams by Wins (Bar Plot)
top_teams = all_matches['winner'].value_counts().head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x=top_teams.values, y=top_teams.index)
plt.title('Top 10 Teams by Wins')
plt.xlabel('Number of Wins')
plt.savefig('2_top_teams.png')
plt.close()

# 3. Toss Decisions (Bar Plot)
toss_decisions = all_matches['toss_decision'].value_counts()
plt.figure(figsize=(6, 6))
sns.barplot(x=toss_decisions.index, y=toss_decisions.values)
plt.title('Toss Decisions')
plt.ylabel('Count')
plt.savefig('3_toss_decisions.png')
plt.close()

# 4. Toss Win vs Match Win (Bar Plot)
toss_win = all_matches[all_matches['toss_winner'] == all_matches['winner']]
percentage = len(toss_win) / len(all_matches) * 100
plt.figure(figsize=(6, 6))
plt.bar(['Toss Winner Won'], [percentage])
plt.title('Percentage of Matches Where Toss Winner Won')
plt.ylabel('Percentage')
plt.savefig('4_toss_win_match.png')
plt.close()

# 5. Matches by Year (Line Plot)
all_matches['year'] = pd.to_datetime(all_matches['date']).dt.year
matches_by_year = all_matches['year'].value_counts().sort_index()
plt.figure(figsize=(12, 6))
matches_by_year.plot()
plt.title('Matches Played Each Year')
plt.ylabel('Number of Matches')
plt.savefig('5_matches_by_year.png')
plt.close()

# 6. Top Venues (Bar Plot)
top_venues = all_matches['venue'].value_counts().head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x=top_venues.values, y=top_venues.index)
plt.title('Top 10 Venues')
plt.xlabel('Number of Matches')
plt.savefig('6_top_venues.png')
plt.close()

# 7. Matches by Month (Bar Plot)
all_matches['month'] = pd.to_datetime(all_matches['date']).dt.month_name()
month_counts = all_matches['month'].value_counts()
plt.figure(figsize=(10, 6))
sns.barplot(x=month_counts.index, y=month_counts.values)
plt.title('Matches by Month')
plt.ylabel('Count')
plt.savefig('7_matches_by_month.png')
plt.close()

# 8. Team Performance by Format (Grouped Bar)
team_wins = all_matches.groupby(['winner', 'match_type']).size().unstack().fillna(0)
top_teams = team_wins.sum(axis=1).sort_values(ascending=False).head(5).index
team_wins.loc[top_teams].plot(kind='bar', figsize=(10, 6))
plt.title('Wins by Format for Top Teams')
plt.ylabel('Number of Wins')
plt.savefig('8_team_format.png')
plt.close()

# 9. City Analysis (Bar Plot)
top_cities = all_matches['city'].value_counts().head(10)
plt.figure(figsize=(10, 6))
sns.barplot(x=top_cities.values, y=top_cities.index)
plt.title('Top 10 Cities Hosting Matches')
plt.xlabel('Number of Matches')
plt.savefig('9_top_cities.png')
plt.close()

# 10. Toss Decision by Format (Stacked Bar)
toss_by_format = all_matches.groupby(['match_type', 'toss_decision']).size().unstack()
toss_by_format.plot(kind='bar', stacked=True, figsize=(10, 6))
plt.title('Toss Decisions by Format')
plt.ylabel('Count')
plt.savefig('10_toss_by_format.png')
plt.close()

print("All 10 visualizations created successfully!")