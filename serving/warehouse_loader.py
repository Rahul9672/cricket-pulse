import psycopg2
import sys, os
sys.path.insert(0, 'ingestion')

from cricsheet_parser import load_match, extract_deliveries, get_all_match_files
from collections import defaultdict

# ─────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="cricketpulse",
        user="admin",
        password="admin123"
    )

# ─────────────────────────────────────────
# STEP 1 — Tables create karo
# ─────────────────────────────────────────

def create_tables():
    """
    Star schema — fact + dimension tables
    Fact table: match_deliveries (each ball = one row)
    Dim tables: dim_batters, dim_bowlers, dim_teams
    """
    conn = get_connection()
    cur  = conn.cursor()

    # cur.executescript = None  # psycopg2 uses execute

    # Dimension table — batters
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_batters (
            batter_id   SERIAL PRIMARY KEY,
            batter_name VARCHAR(100) UNIQUE NOT NULL
        )
    """)

    # Dimension table — teams
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_teams (
            team_id   SERIAL PRIMARY KEY,
            team_name VARCHAR(100) UNIQUE NOT NULL
        )
    """)

    # Aggregated stats table — top batters
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agg_batter_stats (
            batter_name  VARCHAR(100) PRIMARY KEY,
            total_runs   INTEGER DEFAULT 0,
            balls_faced  INTEGER DEFAULT 0,
            fours        INTEGER DEFAULT 0,
            sixes        INTEGER DEFAULT 0,
            strike_rate  FLOAT DEFAULT 0.0,
            updated_at   TIMESTAMP DEFAULT NOW()
        )
    """)

    # Aggregated stats — bowlers
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agg_bowler_stats (
            bowler_name  VARCHAR(100) PRIMARY KEY,
            wickets      INTEGER DEFAULT 0,
            runs_given   INTEGER DEFAULT 0,
            balls_bowled INTEGER DEFAULT 0,
            economy_rate FLOAT DEFAULT 0.0,
            updated_at   TIMESTAMP DEFAULT NOW()
        )
    """)

    # Phase analysis — powerplay vs death
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agg_phase_stats (
            id           SERIAL PRIMARY KEY,
            team_name    VARCHAR(100),
            phase        VARCHAR(20),
            runs         INTEGER DEFAULT 0,
            balls        INTEGER DEFAULT 0,
            wickets_lost INTEGER DEFAULT 0,
            run_rate     FLOAT DEFAULT 0.0,
            updated_at   TIMESTAMP DEFAULT NOW(),
            UNIQUE(team_name, phase)
        )
    """)

    # Match summary
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agg_match_summary (
            match_date   VARCHAR(20) PRIMARY KEY,
            venue        VARCHAR(200),
            total_runs   INTEGER DEFAULT 0,
            total_wickets INTEGER DEFAULT 0,
            total_sixes  INTEGER DEFAULT 0,
            total_fours  INTEGER DEFAULT 0,
            updated_at   TIMESTAMP DEFAULT NOW()
        )
    """)

    conn.commit()
    cur.close()
    conn.close()
    print(" All tables created")

# ─────────────────────────────────────────
# STEP 2 — Data compute + load karo
# ─────────────────────────────────────────

def compute_and_load(data_dir: str = 'data', max_matches: int = 20):
    """
    Match data padho → aggregate karo → PostgreSQL mein load karo
    """
    files = get_all_match_files(data_dir)[:max_matches]
    print(f"\nProcessing {len(files)} matches...")

    # In-memory aggregation (Pandas ki tarah, pure Python)
    batter_runs   = defaultdict(int)
    batter_balls  = defaultdict(int)
    batter_fours  = defaultdict(int)
    batter_sixes  = defaultdict(int)

    bowler_wickets = defaultdict(int)
    bowler_runs    = defaultdict(int)
    bowler_balls   = defaultdict(int)

    phase_runs     = defaultdict(int)
    phase_balls    = defaultdict(int)
    phase_wickets  = defaultdict(int)

    match_data     = defaultdict(lambda: defaultdict(int))
    match_venue    = {}

    total_balls = 0

    for filepath in files:
        try:
            match    = load_match(str(filepath))
            meta     = match.get('info', {})
            date     = meta.get('dates', ['unknown'])[0]
            venue    = meta.get('venue', 'unknown')
            match_venue[date] = venue

            for d in extract_deliveries(match):
                batter  = d.get('batter', '')
                bowler  = d.get('bowler', '')
                runs    = int(d.get('runs_off_bat', 0))
                t_runs  = int(d.get('total_runs', 0))
                wicket  = d.get('wicket')
                over    = int(d.get('over', 0))
                team    = d.get('batting_team', '')

                # Phase determine karo
                if over <= 5:
                    phase = 'Powerplay'
                elif over <= 14:
                    phase = 'Middle'
                else:
                    phase = 'Death'

                phase_key = f"{team}|{phase}"

                # Batter stats
                batter_runs[batter]  += runs
                batter_balls[batter] += 1
                if runs == 4: batter_fours[batter] += 1
                if runs == 6: batter_sixes[batter] += 1

                # Bowler stats
                bowler_runs[bowler]  += t_runs
                bowler_balls[bowler] += 1
                if wicket: bowler_wickets[bowler] += 1

                # Phase stats
                phase_runs[phase_key]    += runs
                phase_balls[phase_key]   += 1
                if wicket: phase_wickets[phase_key] += 1

                # Match summary
                match_data[date]['runs']    += runs
                match_data[date]['wickets'] += 1 if wicket else 0
                match_data[date]['sixes']   += 1 if runs == 6 else 0
                match_data[date]['fours']   += 1 if runs == 4 else 0

                total_balls += 1

        except Exception as e:
            print(f"  Skip {filepath}: {e}")

    print(f"Processed {total_balls:,} deliveries")

    # PostgreSQL mein load karo
    conn = get_connection()
    cur  = conn.cursor()

    # 1. Batter stats
    cur.execute("TRUNCATE TABLE agg_batter_stats")
    for batter, runs in batter_runs.items():
        if not batter: continue
        balls = batter_balls[batter]
        sr    = round((runs / balls * 100), 2) if balls > 0 else 0
        cur.execute("""
            INSERT INTO agg_batter_stats
                (batter_name, total_runs, balls_faced, fours, sixes, strike_rate)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (batter_name) DO UPDATE SET
                total_runs  = EXCLUDED.total_runs,
                balls_faced = EXCLUDED.balls_faced,
                strike_rate = EXCLUDED.strike_rate,
                updated_at  = NOW()
        """, (batter, runs, balls,
              batter_fours[batter], batter_sixes[batter], sr))

    # 2. Bowler stats
    cur.execute("TRUNCATE TABLE agg_bowler_stats")
    for bowler, wickets in bowler_wickets.items():
        if not bowler: continue
        balls   = bowler_balls[bowler]
        runs_g  = bowler_runs[bowler]
        economy = round((runs_g / balls * 6), 2) if balls > 0 else 0
        cur.execute("""
            INSERT INTO agg_bowler_stats
                (bowler_name, wickets, runs_given, balls_bowled, economy_rate)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (bowler_name) DO UPDATE SET
                wickets      = EXCLUDED.wickets,
                economy_rate = EXCLUDED.economy_rate,
                updated_at   = NOW()
        """, (bowler, wickets, runs_g, balls, economy))

    # 3. Phase stats
    cur.execute("TRUNCATE TABLE agg_phase_stats")
    for key, runs in phase_runs.items():
        team, phase = key.split('|')
        balls   = phase_balls[key]
        wickets = phase_wickets.get(key, 0)
        rr      = round((runs / balls * 6), 2) if balls > 0 else 0
        cur.execute("""
            INSERT INTO agg_phase_stats
                (team_name, phase, runs, balls, wickets_lost, run_rate)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_name, phase) DO UPDATE SET
                runs         = EXCLUDED.runs,
                run_rate     = EXCLUDED.run_rate,
                updated_at   = NOW()
        """, (team, phase, runs, balls, wickets, rr))

    # 4. Match summary
    cur.execute("TRUNCATE TABLE agg_match_summary")
    for date, stats in match_data.items():
        venue = match_venue.get(date, 'unknown')
        cur.execute("""
            INSERT INTO agg_match_summary
                (match_date, venue, total_runs, total_wickets,
                 total_sixes, total_fours)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (match_date) DO UPDATE SET
                total_runs = EXCLUDED.total_runs,
                updated_at = NOW()
        """, (date, venue, stats['runs'], stats['wickets'],
              stats['sixes'], stats['fours']))

    conn.commit()
    cur.close()
    conn.close()
    print(" All stats loaded to PostgreSQL!")

# ─────────────────────────────────────────
# STEP 3 — Verify karo
# ─────────────────────────────────────────

def verify_data():
    conn = get_connection()
    cur  = conn.cursor()

    # Top 5 batters
    cur.execute("""
        SELECT batter_name, total_runs, balls_faced, strike_rate
        FROM agg_batter_stats
        ORDER BY total_runs DESC
        LIMIT 5
    """)
    print("\n Top 5 Batters in PostgreSQL:")
    print("-" * 50)
    for row in cur.fetchall():
        print(f"  {row[0]:<25} {row[1]:>4} runs  SR: {row[3]}")

    # Top 5 bowlers
    cur.execute("""
        SELECT bowler_name, wickets, economy_rate
        FROM agg_bowler_stats
        ORDER BY wickets DESC
        LIMIT 5
    """)
    print("\n Top 5 Bowlers in PostgreSQL:")
    print("-" * 50)
    for row in cur.fetchall():
        print(f"  {row[0]:<25} {row[1]:>2} wickets  Eco: {row[2]}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    print("="*60)
    print(" CricketPulse — Warehouse Loader")
    print("="*60)
    create_tables()
    compute_and_load('data', max_matches=20)
    verify_data()