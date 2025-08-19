import sqlite3
import pandas as pd
from tabulate import tabulate
from datetime import datetime

def create_connection():
    """Create a database connection to the SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect('cricket_analytics.db')
        print("Successfully connected to the database")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def run_queries(conn):
    """Execute and display 20 analytical queries using only available columns"""
    queries = {
        # Basic Counts
        1: "Total matches by format",
        2: "Matches per year across all formats",
        
        # Team Performance
        3: "Teams with most matches played",
        4: "Top 5 winning teams overall",
        5: "Win percentage by team (min 20 matches)",
        6: "Team performance by match format",
        7: "Head-to-head records between top teams",
        
        # Match Characteristics
        8: "Toss decision frequency by format",
        9: "Toss win vs match win correlation",
        10: "Most successful teams when winning toss",
        
        # Venue Analysis
        11: "Top 10 most used venues",
        12: "Venues with highest home advantage",
        13: "Cities hosting most matches",
        
        # Temporal Analysis
        14: "Matches per month (seasonality)",
        15: "Team performance by decade",
        
        # Match Type Specific
        16: "Test match results over time",
        17: "T20 match winners analysis",
        18: "ODI match winners analysis",
        
        # Advanced Analytics
        19: "Teams with best win rate when losing toss",
        20: "Most consistent venues (hosting multiple formats)"
    }

    sql_statements = {
        1: """
        SELECT match_type, COUNT(*) as matches
        FROM (
            SELECT match_type FROM test_matches
            UNION ALL SELECT match_type FROM odi_matches
            UNION ALL SELECT match_type FROM t20_matches
        )
        GROUP BY match_type
        ORDER BY matches DESC
        """,
        
        2: """
        SELECT SUBSTR(date, 1, 4) as year, COUNT(*) as matches
        FROM (
            SELECT date FROM test_matches
            UNION ALL SELECT date FROM odi_matches
            UNION ALL SELECT date FROM t20_matches
        )
        WHERE date IS NOT NULL
        GROUP BY year
        ORDER BY year
        """,
        
        3: """
        SELECT team, COUNT(*) as matches_played
        FROM (
            SELECT team1 as team FROM test_matches UNION ALL
            SELECT team2 as team FROM test_matches UNION ALL
            SELECT team1 as team FROM odi_matches UNION ALL
            SELECT team2 as team FROM odi_matches UNION ALL
            SELECT team1 as team FROM t20_matches UNION ALL
            SELECT team2 as team FROM t20_matches
        )
        GROUP BY team
        ORDER BY matches_played DESC
        LIMIT 10
        """,
        
        4: """
        SELECT winner, COUNT(*) as wins
        FROM (
            SELECT winner FROM test_matches WHERE winner IS NOT NULL
            UNION ALL SELECT winner FROM odi_matches WHERE winner IS NOT NULL
            UNION ALL SELECT winner FROM t20_matches WHERE winner IS NOT NULL
        )
        GROUP BY winner
        ORDER BY wins DESC
        LIMIT 5
        """,
        
        5: """
        WITH team_matches AS (
            SELECT team, COUNT(*) as total_matches,
                   SUM(CASE WHEN winner = team THEN 1 ELSE 0 END) as wins
            FROM (
                SELECT team1 as team, winner FROM test_matches UNION ALL
                SELECT team2 as team, winner FROM test_matches UNION ALL
                SELECT team1 as team, winner FROM odi_matches UNION ALL
                SELECT team2 as team, winner FROM odi_matches UNION ALL
                SELECT team1 as team, winner FROM t20_matches UNION ALL
                SELECT team2 as team, winner FROM t20_matches
            )
            GROUP BY team
            HAVING total_matches >= 20
        )
        SELECT team, 
               total_matches,
               wins,
               ROUND((wins * 100.0 / total_matches), 2) as win_percentage
        FROM team_matches
        ORDER BY win_percentage DESC
        LIMIT 10
        """,
        
        6: """
        SELECT 
            team,
            SUM(CASE WHEN format = 'test' THEN wins ELSE 0 END) as test_wins,
            SUM(CASE WHEN format = 'odi' THEN wins ELSE 0 END) as odi_wins,
            SUM(CASE WHEN format = 't20' THEN wins ELSE 0 END) as t20_wins
        FROM (
            SELECT 'test' as format, winner as team, COUNT(*) as wins 
            FROM test_matches WHERE winner IS NOT NULL GROUP BY winner
            UNION ALL
            SELECT 'odi', winner, COUNT(*) FROM odi_matches WHERE winner IS NOT NULL GROUP BY winner
            UNION ALL
            SELECT 't20', winner, COUNT(*) FROM t20_matches WHERE winner IS NOT NULL GROUP BY winner
        )
        GROUP BY team
        ORDER BY (test_wins + odi_wins + t20_wins) DESC
        LIMIT 10
        """,
        
        7: """
        WITH top_teams AS (
            SELECT winner FROM (
                SELECT winner, COUNT(*) as wins FROM (
                    SELECT winner FROM test_matches WHERE winner IS NOT NULL
                    UNION ALL SELECT winner FROM odi_matches WHERE winner IS NOT NULL
                    UNION ALL SELECT winner FROM t20_matches WHERE winner IS NOT NULL
                )
                GROUP BY winner
                ORDER BY wins DESC
                LIMIT 5
            )
        )
        SELECT 
            t1.team1,
            t1.team2,
            COUNT(*) as total_matches,
            SUM(CASE WHEN t1.winner = t1.team1 THEN 1 ELSE 0 END) as team1_wins,
            SUM(CASE WHEN t1.winner = t1.team2 THEN 1 ELSE 0 END) as team2_wins,
            SUM(CASE WHEN t1.winner IS NULL THEN 1 ELSE 0 END) as draws_or_ties
        FROM (
            SELECT team1, team2, winner FROM test_matches
            UNION ALL SELECT team1, team2, winner FROM odi_matches
            UNION ALL SELECT team1, team2, winner FROM t20_matches
        ) t1
        WHERE (t1.team1 IN (SELECT winner FROM top_teams) AND t1.team2 IN (SELECT winner FROM top_teams))
        GROUP BY t1.team1, t1.team2
        HAVING total_matches >= 5
        ORDER BY total_matches DESC
        """,
        
        8: """
        SELECT match_type, toss_decision, COUNT(*) as count
        FROM (
            SELECT match_type, toss_decision FROM test_matches
            UNION ALL SELECT match_type, toss_decision FROM odi_matches
            UNION ALL SELECT match_type, toss_decision FROM t20_matches
        )
        WHERE toss_decision IS NOT NULL
        GROUP BY match_type, toss_decision
        ORDER BY match_type, count DESC
        """,
        
        9: """
        SELECT 
            match_type,
            COUNT(*) as total_matches,
            SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) as toss_and_win,
            ROUND(SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as percentage
        FROM (
            SELECT match_type, toss_winner, winner FROM test_matches
            UNION ALL SELECT match_type, toss_winner, winner FROM odi_matches
            UNION ALL SELECT match_type, toss_winner, winner FROM t20_matches
        )
        WHERE toss_winner IS NOT NULL AND winner IS NOT NULL
        GROUP BY match_type
        ORDER BY percentage DESC
        """,
        
        10: """
        SELECT 
            winner,
            COUNT(*) as wins_after_toss_win,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) 
                FROM (
                    SELECT winner FROM test_matches WHERE toss_winner = winner
                    UNION ALL SELECT winner FROM odi_matches WHERE toss_winner = winner
                    UNION ALL SELECT winner FROM t20_matches WHERE toss_winner = winner
                ) t WHERE t.winner = m.winner
            ), 2) as win_percentage_when_toss_winner
        FROM (
            SELECT winner FROM test_matches WHERE toss_winner = winner
            UNION ALL SELECT winner FROM odi_matches WHERE toss_winner = winner
            UNION ALL SELECT winner FROM t20_matches WHERE toss_winner = winner
        ) m
        GROUP BY winner
        HAVING wins_after_toss_win >= 10
        ORDER BY win_percentage_when_toss_winner DESC
        LIMIT 10
        """,
        
        11: """
        SELECT venue, COUNT(*) as matches_hosted
        FROM (
            SELECT venue FROM test_matches
            UNION ALL SELECT venue FROM odi_matches
            UNION ALL SELECT venue FROM t20_matches
        )
        WHERE venue IS NOT NULL
        GROUP BY venue
        ORDER BY matches_hosted DESC
        LIMIT 10
        """,
        
        12: """
        WITH venue_teams AS (
            SELECT 
                venue,
                city,
                team1 as team,
                COUNT(*) as total_matches,
                SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END) as wins
            FROM (
                SELECT venue, city, team1, winner FROM test_matches
                UNION ALL SELECT venue, city, team1, winner FROM odi_matches
                UNION ALL SELECT venue, city, team1, winner FROM t20_matches
            )
            GROUP BY venue, city, team1
            
            UNION ALL
            
            SELECT 
                venue,
                city,
                team2 as team,
                COUNT(*) as total_matches,
                SUM(CASE WHEN winner = team2 THEN 1 ELSE 0 END) as wins
            FROM (
                SELECT venue, city, team2, winner FROM test_matches
                UNION ALL SELECT venue, city, team2, winner FROM odi_matches
                UNION ALL SELECT venue, city, team2, winner FROM t20_matches
            )
            GROUP BY venue, city, team2
        )
        SELECT 
            venue,
            city,
            team,
            total_matches,
            wins,
            ROUND((wins * 100.0 / total_matches), 2) as win_percentage
        FROM venue_teams
        WHERE total_matches >= 10
        ORDER BY win_percentage DESC
        LIMIT 10
        """,
        
        13: """
        SELECT city, COUNT(*) as matches_hosted
        FROM (
            SELECT city FROM test_matches
            UNION ALL SELECT city FROM odi_matches
            UNION ALL SELECT city FROM t20_matches
        )
        WHERE city IS NOT NULL
        GROUP BY city
        ORDER BY matches_hosted DESC
        LIMIT 10
        """,
        
        14: """
        SELECT 
            CASE 
                WHEN SUBSTR(date, 6, 2) IN ('12', '01', '02') THEN 'Winter'
                WHEN SUBSTR(date, 6, 2) IN ('03', '04', '05') THEN 'Spring'
                WHEN SUBSTR(date, 6, 2) IN ('06', '07', '08') THEN 'Summer'
                WHEN SUBSTR(date, 6, 2) IN ('09', '10', '11') THEN 'Fall'
                ELSE 'Unknown'
            END as season,
            COUNT(*) as matches
        FROM (
            SELECT date FROM test_matches
            UNION ALL SELECT date FROM odi_matches
            UNION ALL SELECT date FROM t20_matches
        )
        WHERE date IS NOT NULL
        GROUP BY season
        ORDER BY matches DESC
        """,
        
        15: """
        SELECT 
            team,
            SUBSTR(year, 1, 3) || '0s' as decade,
            COUNT(*) as matches,
            SUM(CASE WHEN winner = team THEN 1 ELSE 0 END) as wins,
            ROUND(SUM(CASE WHEN winner = team THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as win_percentage
        FROM (
            SELECT team1 as team, winner, SUBSTR(date, 1, 4) as year FROM test_matches
            UNION ALL SELECT team2 as team, winner, SUBSTR(date, 1, 4) as year FROM test_matches
            UNION ALL SELECT team1 as team, winner, SUBSTR(date, 1, 4) as year FROM odi_matches
            UNION ALL SELECT team2 as team, winner, SUBSTR(date, 1, 4) as year FROM odi_matches
            UNION ALL SELECT team1 as team, winner, SUBSTR(date, 1, 4) as year FROM t20_matches
            UNION ALL SELECT team2 as team, winner, SUBSTR(date, 1, 4) as year FROM t20_matches
        )
        WHERE year IS NOT NULL AND year >= '1970'
        GROUP BY team, decade
        HAVING matches >= 20
        ORDER BY decade, win_percentage DESC
        """,
        
        16: """
        SELECT 
            SUBSTR(date, 1, 4) as year,
            COUNT(*) as test_matches,
            SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END) as team1_wins,
            SUM(CASE WHEN winner = team2 THEN 1 ELSE 0 END) as team2_wins,
            SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) as draws
        FROM test_matches
        WHERE date IS NOT NULL
        GROUP BY year
        HAVING test_matches >= 5
        ORDER BY year
        """,
        
        17: """
        SELECT 
            winner,
            COUNT(*) as t20_wins,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) FROM t20_matches WHERE winner IS NOT NULL
            ), 2) as percentage_of_total_wins
        FROM t20_matches
        WHERE winner IS NOT NULL
        GROUP BY winner
        HAVING COUNT(*) >= 10
        ORDER BY t20_wins DESC
        LIMIT 10
        """,
        
        18: """
        SELECT 
            winner,
            COUNT(*) as odi_wins,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) FROM odi_matches WHERE winner IS NOT NULL
            ), 2) as percentage_of_total_wins
        FROM odi_matches
        WHERE winner IS NOT NULL
        GROUP BY winner
        HAVING COUNT(*) >= 20
        ORDER BY odi_wins DESC
        LIMIT 10
        """,
        
        19: """
        SELECT 
            winner,
            COUNT(*) as wins_without_toss,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) 
                FROM (
                    SELECT winner FROM test_matches WHERE toss_winner != winner
                    UNION ALL SELECT winner FROM odi_matches WHERE toss_winner != winner
                    UNION ALL SELECT winner FROM t20_matches WHERE toss_winner != winner
                ) t WHERE t.winner = m.winner
            ), 2) as win_percentage_when_losing_toss
        FROM (
            SELECT winner FROM test_matches WHERE toss_winner != winner
            UNION ALL SELECT winner FROM odi_matches WHERE toss_winner != winner
            UNION ALL SELECT winner FROM t20_matches WHERE toss_winner != winner
        ) m
        GROUP BY winner
        HAVING wins_without_toss >= 10
        ORDER BY win_percentage_when_losing_toss DESC
        LIMIT 10
        """,
        
        20: """
        SELECT venue, 
               COUNT(DISTINCT match_type) as formats_hosted,
               GROUP_CONCAT(DISTINCT match_type) as format_list
        FROM (
            SELECT venue, 'test' as match_type FROM test_matches
            UNION SELECT venue, 'odi' FROM odi_matches
            UNION SELECT venue, 't20' FROM t20_matches
        )
        WHERE venue IS NOT NULL
        GROUP BY venue
        HAVING COUNT(DISTINCT match_type) > 1
        ORDER BY formats_hosted DESC, venue
        """
    }

    # Execute and display queries
    for num, title in queries.items():
        print(f"\n=== Query {num}: {title} ===")
        try:
            df = pd.read_sql_query(sql_statements[num], conn)
            print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
        except Exception as e:
            print(f"Error executing query {num}: {e}")

def main():
    conn = create_connection()
    if conn:
        run_queries(conn)
        conn.close()
        print("\nAll 20 queries executed successfully")

if __name__ == "__main__":
    main()