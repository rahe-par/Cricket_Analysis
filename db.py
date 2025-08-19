import pandas as pd
from sqlalchemy import create_engine, inspect

def create_match_dataframes():
    """Load CSV and create separate DataFrames for each match type"""
    # Load the processed data
    df = pd.read_csv("processed_matches.csv")
    
    # Clean match_type values (handle case variations)
    df['match_type'] = df['match_type'].str.lower().str.strip()
    
    # Create separate DataFrames
    test_matches = df[df['match_type'] == 'test'].copy()
    odi_matches = df[df['match_type'] == 'odi'].copy()
    t20_matches = df[df['match_type'] == 't20'].copy()
    
    # Add match_id as primary key to each DataFrame
    for df_match in [test_matches, odi_matches, t20_matches]:
        df_match.insert(0, 'match_id', range(1, len(df_match) + 1))
    
    return test_matches, odi_matches, t20_matches

def create_database(test_matches, odi_matches, t20_matches):
    """Create SQL database with separate tables for each match type"""
    # Create SQLAlchemy engine (SQLite)
    engine = create_engine('sqlite:///cricket_analytics.db')
    
    # Write DataFrames to database
    test_matches.to_sql('test_matches', engine, if_exists='replace', index=False)
    odi_matches.to_sql('odi_matches', engine, if_exists='replace', index=False)
    t20_matches.to_sql('t20_matches', engine, if_exists='replace', index=False)
    
    return engine

def verify_database(engine):
    """Verify the database structure and contents"""
    inspector = inspect(engine)
    
    # Print table information
    print("Tables in database:", inspector.get_table_names())
    
    # Print record counts
    for table in ['test_matches', 'odi_matches', 't20_matches']:
        count = pd.read_sql_query(f"SELECT COUNT(*) FROM {table}", engine).iloc[0,0]
        print(f"{table}: {count} records")
    
    # Print schema of one table
    print("\nSchema of test_matches:")
    for column in inspector.get_columns('test_matches'):
        print(f"{column['name']:15} {column['type']}")



def main():
    # Step 1: Create DataFrames
    test_matches, odi_matches, t20_matches = create_match_dataframes()
    
    print("DataFrame sizes:")
    print(f"Test matches: {len(test_matches)} records")
    print(f"ODI matches: {len(odi_matches)} records")
    print(f"T20 matches: {len(t20_matches)} records")
    
    # Step 2: Create database
    engine = create_database(test_matches, odi_matches, t20_matches)
    
    # Step 3: Verify database
    verify_database(engine)
    
    # Close connection
    engine.dispose()
    print("\nDatabase created successfully at cricket_analytics.db")

if __name__ == "__main__":
    main()