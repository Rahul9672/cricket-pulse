import json
import pandas as pd
from pathlib import Path
from collections import defaultdict
import sys
import os

# Parser import karo
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'ingestion'))
from cricsheet_parser import load_match, extract_deliveries, get_all_match_files

# ─────────────────────────────────────────
# STEP 1 — Sab matches ka data load karo
# ─────────────────────────────────────────

def load_season_data(data_dir: str, max_matches: int = 20) -> pd.DataFrame:
    """
    Sab IPL match files padho
    Ek flat DataFrame banao — har row = ek ball event
    max_matches = kitne matches load karne hain (testing ke liye)
    """
    all_deliveries = []
    files = get_all_match_files(data_dir)[:max_matches]
    
    print(f"Loading {len(files)} matches...")
    
    for i, filepath in enumerate(files):
        try:
            match = load_match(str(filepath))
            
            # Match metadata
            meta = match.get('info', {})
            teams = meta.get('teams', ['?', '?'])
            date  = meta.get('dates', ['unknown'])[0]
            venue = meta.get('venue', 'unknown')
            
            for delivery in extract_deliveries(match):
                delivery['date']  = date
                delivery['venue'] = venue
                all_deliveries.append(delivery)
                
        except Exception as e:
            print(f"  Skipping {filepath.name}: {e}")
    
    df = pd.DataFrame(all_deliveries)
    print(f"Loaded {len(df):,} deliveries from {len(files)} matches")
    return df

# ─────────────────────────────────────────
# STEP 2 — Data clean karo
# ─────────────────────────────────────────

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Data quality checks — DE mein yeh zaroori hai
    Bad data → bad analytics
    """
    original_len = len(df)
    
    # 1. Null values check
    print(f"\n Null check:")
    print(df[['batter','bowler','runs_off_bat']].isnull().sum())
    
    # 2. Invalid runs filter (runs 0-6 hone chahiye, extras alag)
    df = df[df['runs_off_bat'].between(0, 6)]
    
    # 3. Empty batter names remove karo
    df = df[df['batter'].str.strip() != '']
    
    # 4. Data types fix karo
    df['runs_off_bat'] = df['runs_off_bat'].astype(int)
    df['over']         = df['over'].astype(int)
    df['date']         = pd.to_datetime(df['date'])
    
    cleaned_len = len(df)
    print(f"\n Cleaned: {original_len - cleaned_len} bad rows removed")
    print(f"   Final dataset: {cleaned_len:,} deliveries")
    
    return df

# ─────────────────────────────────────────
# STEP 3 — Analytics functions
# ─────────────────────────────────────────

def top_batters(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Top batters by total runs
    Pandas groupby = Spark groupBy ka chota bhai
    """
    batting = (
        df.groupby('batter')
        .agg(
            runs        = ('runs_off_bat', 'sum'),
            balls_faced = ('runs_off_bat', 'count'),
            fours       = ('runs_off_bat', lambda x: (x == 4).sum()),
            sixes       = ('runs_off_bat', lambda x: (x == 6).sum()),
        )
        .reset_index()
    )
    
    # Strike rate = (runs/balls) * 100
    batting['strike_rate'] = (
        (batting['runs'] / batting['balls_faced']) * 100
    ).round(2)
    
    return batting.sort_values('runs', ascending=False).head(top_n)

def top_bowlers(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Top bowlers by wickets + economy rate
    """
    # Wickets count karo
    wickets_df = (
        df[df['wicket'].notna()]           # sirf wicket balls
        .groupby('bowler')
        .size()
        .reset_index(name='wickets')
    )
    
    # Economy rate = runs given per over
    economy_df = (
        df.groupby('bowler')
        .agg(
            runs_given  = ('total_runs', 'sum'),
            balls_bowled = ('total_runs', 'count')
        )
        .reset_index()
    )
    economy_df['economy'] = (
        (economy_df['runs_given'] / economy_df['balls_bowled']) * 6
    ).round(2)
    
    # Merge karo
    bowling = economy_df.merge(wickets_df, on='bowler', how='left')
    bowling['wickets'] = bowling['wickets'].fillna(0).astype(int)
    
    # Minimum 10 overs bowled ho
    bowling = bowling[bowling['balls_bowled'] >= 60]
    
    return bowling.sort_values('wickets', ascending=False).head(top_n)

def powerplay_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Powerplay (overs 1-6) vs death overs (17-20) comparison
    Sliding window concept — same jo LeetCode mein seekha
    """
    df['phase'] = pd.cut(
        df['over'],
        bins=[-1, 5, 14, 19],
        labels=['Powerplay (1-6)', 'Middle (7-15)', 'Death (16-20)']
    )
    
    phase_stats = (
        df.groupby(['batting_team', 'phase'], observed=True)
        .agg(
            runs         = ('runs_off_bat', 'sum'),
            balls        = ('runs_off_bat', 'count'),
            wickets_lost = ('wicket', lambda x: x.notna().sum())
        )
        .reset_index()
    )
    
    phase_stats['run_rate'] = (
        (phase_stats['runs'] / phase_stats['balls']) * 6
    ).round(2)
    
    return phase_stats

def venue_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Kaunse venue pe batting easy hai?
    Average runs per match per venue
    """
    return (
        df.groupby('venue')
        .agg(
            total_runs = ('runs_off_bat', 'sum'),
            matches    = ('date', 'nunique'),  # unique match dates
        )
        .assign(avg_runs_per_match = lambda x: (
            x['total_runs'] / x['matches']
        ).round(1))
        .sort_values('avg_runs_per_match', ascending=False)
        .reset_index()
    )

# ─────────────────────────────────────────
# STEP 4 — Run everything
# ─────────────────────────────────────────

def run_full_analysis(data_dir: str = 'data'):
    print("=" * 60)
    print(" CricketPulse — IPL Season Analysis")
    print("=" * 60)
    
    # Load
    df = load_season_data(data_dir, max_matches=20)
    
    # Clean
    df = clean_data(df)
    
    # Analytics
    print("\n TOP 10 BATTERS:")
    print("-" * 60)
    batters = top_batters(df)
    print(batters[['batter','runs','balls_faced','strike_rate','fours','sixes']]
          .to_string(index=False))
    
    print("\n TOP 10 BOWLERS:")
    print("-" * 60)
    bowlers = top_bowlers(df)
    print(bowlers[['bowler','wickets','economy','balls_bowled']]
          .to_string(index=False))
    
    print("\n POWERPLAY vs DEATH OVERS:")
    print("-" * 60)
    phases = powerplay_analysis(df)
    print(phases.to_string(index=False))
    
    print("\n VENUE ANALYSIS:")
    print("-" * 60)
    venues = venue_analysis(df)
    print(venues.head(5).to_string(index=False))
    
    # Save to CSV — batch pipeline ka output
    batters.to_csv('batch/top_batters.csv', index=False)
    bowlers.to_csv('batch/top_bowlers.csv', index=False)
    print("\n Stats saved to batch/top_batters.csv and top_bowlers.csv")
    
    return df

if __name__ == "__main__":
    df = run_full_analysis('data')