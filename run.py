import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent / "Python"

INGESTORS = [
    ROOT / "NBA" / "ingest_nba_odds.py",
    ROOT / "NFL" / "ingest_nfl_odds.py",
    ROOT / "NHL" / "ingest_nhl_odds.py",
    ROOT / "MLB" / "ingest_mlb_odds.py",
    ROOT / "NCAAF" / "ingest_ncaaf_odds.py",
]

for script in INGESTORS:
    subprocess.Popen(
        [sys.executable, str(script)],
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )
    print(f"Started {script.parent.name} ingestor")

print("\nAll ingestors running. Starting dashboard...\n")
subprocess.run(["streamlit", "run", str(ROOT / "dashboard.py")])
