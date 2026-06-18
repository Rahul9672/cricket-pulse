import sys, os
sys.path.insert(0, 'ingestion')

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType,
    DateType, BooleanType, FloatType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
import pyarrow as pa
from datetime import datetime, date

from cricsheet_parser import load_match, extract_deliveries, get_all_match_files
from iceberg_setup import get_catalog, setup_namespace

# ─────────────────────────────────────────
# STEP 1 — Schema define karo
# ─────────────────────────────────────────

def get_deliveries_schema():
    """
    Iceberg schema — Spark schema jaisa but Iceberg types
    Field ID zaroori hai — schema evolution ke liye
    """
    return Schema(
        NestedField(1,  "match_id",     StringType(),  required=False),
        NestedField(2,  "match_date",   StringType(),  required=False),
        NestedField(3,  "venue",        StringType(),  required=False),
        NestedField(4,  "batting_team", StringType(),  required=False),
        NestedField(5,  "batter",       StringType(),  required=False),
        NestedField(6,  "bowler",       StringType(),  required=False),
        NestedField(7,  "over_num",     IntegerType(), required=False),
        NestedField(8,  "ball_num",     IntegerType(), required=False),
        NestedField(9,  "runs_off_bat", IntegerType(), required=False),
        NestedField(10, "total_runs",   IntegerType(), required=False),
        NestedField(11, "extras",       IntegerType(), required=False),
        NestedField(12, "wicket_kind",  StringType(),  required=False),
        NestedField(13, "is_boundary",  BooleanType(), required=False),
        NestedField(14, "is_six",       BooleanType(), required=False),
    )

# ─────────────────────────────────────────
# STEP 2 — Table create karo
# ─────────────────────────────────────────

def create_deliveries_table(catalog):
    """
    Iceberg table banao with partitioning
    """
    table_name = "cricket.ipl_deliveries"
    
    # Already exist karti hai?
    try:
        table = catalog.load_table(table_name)
        print(f"Table '{table_name}' already exists")
        return table
    except Exception:
        pass
    
    schema = get_deliveries_schema()
    
    # Partition spec — data kaise organize karo on disk
    # batting_team pe partition → ek team ke queries fast
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=4,           # batting_team field ka ID
            field_id=1000,
            transform=IdentityTransform(),  # exact value pe partition
            name="batting_team"
        )
    )
    
    table = catalog.create_table(
        identifier=table_name,
        schema=schema,
        partition_spec=partition_spec,
    )
    print(f" Table '{table_name}' created")
    return table

# ─────────────────────────────────────────
# STEP 3 — Data load + write karo
# ─────────────────────────────────────────

def load_and_write(table, data_dir: str, max_matches: int = 10):
    """
    Match data padho → PyArrow table banao → Iceberg mein write karo
    """
    files = get_all_match_files(data_dir)[:max_matches]
    print(f"\nLoading {len(files)} matches...")
    
    all_rows = []
    for filepath in files:
        try:
            match = load_match(str(filepath))
            meta  = match.get('info', {})
            date  = meta.get('dates', ['unknown'])[0]
            venue = meta.get('venue', 'unknown')
            
            for d in extract_deliveries(match):
                all_rows.append({
                    'match_id':     str(date),
                    'match_date':   str(date),
                    'venue':        str(venue),
                    'batting_team': str(d.get('batting_team', '')),
                    'batter':       str(d.get('batter', '')),
                    'bowler':       str(d.get('bowler', '')),
                    'over_num':     int(d.get('over', 0)),
                    'ball_num':     int(d.get('ball', 0)),
                    'runs_off_bat': int(d.get('runs_off_bat', 0)),
                    'total_runs':   int(d.get('total_runs', 0)),
                    'extras':       int(d.get('extras', 0)),
                    'wicket_kind':  str(d.get('wicket', '')) if d.get('wicket') else None,
                    'is_boundary':  bool(d.get('runs_off_bat', 0) >= 4),
                    'is_six':       bool(d.get('runs_off_bat', 0) == 6),
                })
        except Exception as e:
            print(f"  Skip {filepath.name}: {e}")
    
    print(f"Loaded {len(all_rows):,} deliveries")
    
    # PyArrow Table banao — Iceberg PyArrow use karta hai internally
    arrow_table = pa.table({
        'match_id':     pa.array([r['match_id']     for r in all_rows], type=pa.string()),
        'match_date':   pa.array([r['match_date']   for r in all_rows], type=pa.string()),
        'venue':        pa.array([r['venue']         for r in all_rows], type=pa.string()),
        'batting_team': pa.array([r['batting_team'] for r in all_rows], type=pa.string()),
        'batter':       pa.array([r['batter']       for r in all_rows], type=pa.string()),
        'bowler':       pa.array([r['bowler']       for r in all_rows], type=pa.string()),
        'over_num':     pa.array([r['over_num']     for r in all_rows], type=pa.int32()),
        'ball_num':     pa.array([r['ball_num']     for r in all_rows], type=pa.int32()),
        'runs_off_bat': pa.array([r['runs_off_bat'] for r in all_rows], type=pa.int32()),
        'total_runs':   pa.array([r['total_runs']   for r in all_rows], type=pa.int32()),
        'extras':       pa.array([r['extras']       for r in all_rows], type=pa.int32()),
        'wicket_kind':  pa.array([r['wicket_kind']  for r in all_rows], type=pa.string()),
        'is_boundary':  pa.array([r['is_boundary']  for r in all_rows], type=pa.bool_()),
        'is_six':       pa.array([r['is_six']       for r in all_rows], type=pa.bool_()),
    })
    
    # WRITE to Iceberg — yeh ek snapshot create karta hai
    table.append(arrow_table)
    print(f" Written to Iceberg table — Snapshot created!")
    
    return len(all_rows)

# ─────────────────────────────────────────
# STEP 4 — Queries chalao
# ─────────────────────────────────────────

def run_queries(table):
    """
    Iceberg table se data query karo
    """
    print("\n" + "="*60)
    print(" QUERYING ICEBERG TABLE")
    print("="*60)
    
    # Scan karo — PyArrow Table milega
    df = table.scan().to_arrow().to_pydict()
    
    total_rows = len(df['batter'])
    print(f"\n Total deliveries in Iceberg: {total_rows:,}")
    
    # Boundaries count karo
    boundaries = sum(1 for b in df['is_boundary'] if b)
    sixes      = sum(1 for s in df['is_six'] if s)
    wickets    = sum(1 for w in df['wicket_kind'] if w)
    
    print(f"\n Quick Stats:")
    print(f"   Total boundaries : {boundaries:,}")
    print(f"   Total sixes      : {sixes:,}")
    print(f"   Total wickets    : {wickets:,}")
    
    # Teams list
    teams = set(df['batting_team'])
    print(f"\n Teams in data: {len(teams)}")
    for t in sorted(teams):
        print(f"   - {t}")
    
    # Snapshot history — TIME TRAVEL
    print("\n SNAPSHOT HISTORY (Time Travel):")
    print("-"*60)
    for snap in table.history():
        ts = datetime.fromtimestamp(snap.timestamp_ms / 1000)
        print(f"   Snapshot {snap.snapshot_id} → {ts.strftime('%Y-%m-%d %H:%M:%S')}")
    
    return total_rows

# ─────────────────────────────────────────
# STEP 5 — Schema Evolution demonstrate karo
# ─────────────────────────────────────────

def demonstrate_schema_evolution(table):
    """
    Naya column add karo — existing data break nahi hoga
    Yeh Iceberg ka killer feature hai
    """
    print("\n" + "="*60)
    print(" SCHEMA EVOLUTION DEMO")
    print("="*60)
    
    with table.update_schema() as update:
        update.add_column(
            path="phase",
            field_type=StringType(),
            doc="Match phase: Powerplay/Middle/Death"
        )
    
    print(" Added column 'phase' to schema")
    print("   Existing data: NULL for 'phase' (no rewrite!)")
    print("   New data: can include phase values")
    
    # Updated schema dekho
    print(f"\n Updated schema columns:")
    for field in table.schema().fields:
        print(f"   Field {field.field_id}: {field.name} ({field.field_type})")

if __name__ == "__main__":
    # Setup
    catalog = get_catalog()
    setup_namespace(catalog)
    
    # Table create
    table = create_deliveries_table(catalog)
    
    # Data write
    rows = load_and_write(table, 'data', max_matches=10)
    
    # Query
    run_queries(table)
    
    # Schema evolution
    demonstrate_schema_evolution(table)
    
    print("\n Day 6 Complete!")
    print("   Iceberg table: cricket.ipl_deliveries")
    print("   Warehouse: iceberg/warehouse/")
    print("   Snapshots: check history above")