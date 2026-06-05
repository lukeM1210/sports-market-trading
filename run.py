import subprocess
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).parent / "Python"

INGESTORS = [
    ROOT / "NBA" / "ingest_nba_odds.py",
    ROOT / "NFL" / "ingest_nfl_odds.py",
    ROOT / "NFL" / "ingest_nfl_futures.py",
    ROOT / "NHL" / "ingest_nhl_odds.py",
    ROOT / "MLB" / "ingest_mlb_odds.py",
    ROOT / "NCAAF" / "ingest_ncaaf_odds.py",
]


def run_ingestor(script: Path) -> None:
    name = script.parent.name
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    for line in proc.stdout:
        print(f"[{name}] {line}", end="")


for script in INGESTORS:
    t = threading.Thread(target=run_ingestor, args=(script,), daemon=True)
    t.start()
    print(f"Started {script.parent.name} ingestor")

print("\nAll ingestors running. Starting dashboard...\n")
subprocess.run(["streamlit", "run", str(ROOT / "Dashboard.py")])
