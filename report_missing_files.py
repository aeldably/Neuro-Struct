# report_missing_files.py
# Run from project root: python report_missing_files.py
# Reads dyadlist.csv to get all unique subjects, then checks outputs/BrainImaging
# for every expected (sub, ses-01 to ses-06, run-01) combination.
# Logs saved to logs/missing_files/missing_files_{timestamp}.log

import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

#  Config 

DYAD_FILE    = Path("dyadlist.csv")
OUTPUT_DIR   = Path("outputs/BrainImaging")
LOG_DIR      = Path("logs/missing_files")
SESSIONS     = [f"{i:02d}" for i in range(1, 7)]   # ses-01 … ses-06
RUN          = "01"

#  Logging setup 

def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file  = LOG_DIR / f"missing_files_{timestamp}.log"

    logger = logging.getLogger("MissingFilesReport")
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"📝 Log saved to: {log_file}")
    return logger


#  Main 

def main():
    logger = setup_logging()

    # Header
    logger.info(f"\n{'=' * 50}")
    logger.info(f"  MISSING SESSION FILES REPORT")
    logger.info(f"  Dyad list : {DYAD_FILE}")
    logger.info(f"  Output    : {OUTPUT_DIR}")
    logger.info(f"  Run at    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'=' * 50}\n")

    # Guards
    if not DYAD_FILE.exists():
        logger.error(f"Dyad list not found: {DYAD_FILE}")
        sys.exit(1)

    if not OUTPUT_DIR.exists():
        logger.error(f"Output directory not found: {OUTPUT_DIR}")
        sys.exit(1)

    # Extract all unique subjects from dyad list
    df   = pd.read_csv(DYAD_FILE, dtype=str)
    subs = sorted(
        {str(pid).strip().zfill(3) for pid in pd.concat([df["pID1"], df["pID2"]])}
    )

    logger.info(f"📋 Subjects loaded from dyad list : {len(subs)}")
    logger.info(f"📋 Sessions expected per subject  : {len(SESSIONS)} (ses-01 to ses-06)")
    logger.info(f"📋 Total combinations expected    : {len(subs) * len(SESSIONS)}\n")

    # Check each (sub, ses, run-01) against outputs
    missing = []

    for sub in subs:
        for ses in SESSIONS:
            pattern = f"**/sub-{sub}_ses-{ses}_*_run-{RUN}_channels.tsv"
            matches = list(OUTPUT_DIR.glob(pattern))
            if not matches:
                missing.append((sub, ses))
                logger.warning(f"⚠️  MISSING : sub-{sub}  |  ses-{ses}  |  run-{RUN}")
            else:
                logger.info(f"✅ FOUND   : sub-{sub}  |  ses-{ses}  |  run-{RUN}")

    # Summary
    total = len(subs) * len(SESSIONS)
    found = total - len(missing)

    logger.info(f"\n{'=' * 50}")
    logger.info(f"  SUMMARY")
    logger.info(f"{'=' * 50}")
    logger.info(f"  Subjects                 : {len(subs)}")
    logger.info(f"  Combinations checked     : {total}")
    logger.info(f"  Found                    : {found}")

    if missing:
        logger.warning(f"  Missing                  : {len(missing)}")
    else:
        logger.info(f"  Missing                  : 0")

    # Missing sessions list
    logger.info(f"\n{'=' * 50}")
    logger.info(f"  MISSING SESSIONS")
    logger.info(f"{'=' * 50}\n")

    if not missing:
        logger.info(f"  ✅ All session files accounted for.\n")
    else:
        for (sub, ses) in missing:
            logger.warning(f"  sub-{sub}  |  ses-{ses}  |  run-{RUN}")
        logger.warning(f"\n  Total: {len(missing)} missing session file(s)")

    logger.info(f"\n{'=' * 50}\n")


if __name__ == "__main__":
    main()