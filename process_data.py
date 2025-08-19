import os
import json
import pandas as pd

# Directory where the JSON files are stored
DATA_DIR = "cricsheet_data"

# Output CSV file
OUTPUT_FILE = "processed_matches.csv"

def process_match_file(file_path):
    """Process a single match JSON file and return a dictionary of match data."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            match_data = json.load(f)

        # Extract basic match info
        info = match_data.get("info", {})
        match_type = info.get("match_type", "")
        teams = info.get("teams", [])
        date = info.get("dates", [""])[0]
        venue = info.get("venue", "")
        city = info.get("city", "")
        toss_winner = info.get("toss", {}).get("winner", "")
        toss_decision = info.get("toss", {}).get("decision", "")
        winner = info.get("outcome", {}).get("winner", "")

        # Create a record
        return {
            "file_name": os.path.basename(file_path),
            "match_type": match_type,
            "team1": teams[0] if len(teams) > 0 else "",
            "team2": teams[1] if len(teams) > 1 else "",
            "date": date,
            "venue": venue,
            "city": city,
            "toss_winner": toss_winner,
            "toss_decision": toss_decision,
            "winner": winner
        }
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def process_all_matches():
    """Process all JSON files in the DATA_DIR and save to a CSV."""
    all_matches = []
    for root, _, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith(".json"):  # Only process JSON files
                file_path = os.path.join(root, file)
                match_record = process_match_file(file_path)
                if match_record:
                    all_matches.append(match_record)

    # Convert to DataFrame and save
    df = pd.DataFrame(all_matches)
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"Processed {len(all_matches)} matches and saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    process_all_matches()
