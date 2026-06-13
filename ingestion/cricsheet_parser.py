import json
import os
from pathlib import Path

def load_match(filepath: str) -> dict:
    """Load one IPL match JSON from Cricsheet"""
    with open(filepath, 'r') as f:
        return json.load(f)

def extract_deliveries(match_data: dict):
    """
    Generator — yields one ball event at a time
    Each event = one delivery in the match
    """
    meta = match_data.get('info', {})
    match_id = meta.get('dates', ['unknown'])[0]
    teams = meta.get('teams', [])
    
    for innings in match_data.get('innings', []):
        batting_team = innings.get('team', '')
        
        for over_data in innings.get('overs', []):
            over_num = over_data.get('over', 0)
            
            for ball_idx, delivery in enumerate(over_data.get('deliveries', [])):
                yield {
                    'match_id': str(match_id),
                    'teams': teams,
                    'batting_team': batting_team,
                    'over': over_num,
                    'ball': ball_idx + 1,
                    'batter': delivery.get('batter', ''),
                    'bowler': delivery.get('bowler', ''),
                    'runs_off_bat': delivery.get('runs', {}).get('batter', 0),
                    'extras': delivery.get('runs', {}).get('extras', 0),
                    'total_runs': delivery.get('runs', {}).get('total', 0),
                    'wicket': delivery.get('wickets', [{}])[0].get('kind', None)
                                if delivery.get('wickets') else None
                }

def get_all_match_files(data_dir: str) -> list:
    """Return list of all JSON match files"""
    return list(Path(data_dir).glob('*.json'))


# Test karo
if __name__ == "__main__":
    files = get_all_match_files('../data')
    if not files:
        print("No match files found in data/ folder")
    else:
        match = load_match(str(files[0]))
        print(f"Match: {match['info']['teams']}")
        print(f"Date: {match['info']['dates']}")
        
        deliveries = list(extract_deliveries(match))
        print(f"Total deliveries: {len(deliveries)}")
        print(f"First ball: {deliveries[0]}")