# validate_participants.py
# Validates participants.tsv and participants.json against BIDS specification.
#
# Usage:
#   python validate_participants.py
#   python validate_participants.py --root outputs/BrainImaging

import sys
import json
import logging
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_ROOT = Path("outputs/BrainImaging")
LOG_DIR      = Path("logs/validation")

# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file  = LOG_DIR / f"participants_validation_{timestamp}.log"

    logger = logging.getLogger("ParticipantsValidator")
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


# ── Validators ────────────────────────────────────────────────────────────────

def validate_tsv(tsv_path: Path, logger: logging.Logger) -> tuple[bool, list[str], pd.DataFrame]:
    """
    Validates participants.tsv against BIDS spec.
    Returns (passed, columns, dataframe).
    """
    logger.info(f"\n{'=' * 50}")
    logger.info(f"  VALIDATING participants.tsv")
    logger.info(f"{'=' * 50}\n")

    errors   = []
    warnings = []

    # Load
    try:
        df = pd.read_csv(tsv_path, sep="\t", dtype=str)
    except Exception as e:
        logger.error(f"❌ Could not read participants.tsv: {e}")
        return False, [], pd.DataFrame()

    logger.info(f"📋 Loaded {len(df)} rows, {len(df.columns)} columns")
    logger.info(f"   Columns: {list(df.columns)}\n")

    # Rule 1 — participant_id must exist
    if "participant_id" not in df.columns:
        errors.append("'participant_id' column is missing")
    else:
        # Rule 2 — participant_id must be the first column
        if df.columns[0] != "participant_id":
            errors.append(f"'participant_id' must be the first column (found: '{df.columns[0]}')")

        # Rule 3 — all IDs must follow sub-<label> format
        invalid_ids = df[~df["participant_id"].str.match(r"^sub-\S+$", na=False)]["participant_id"].tolist()
        if invalid_ids:
            errors.append(f"Invalid participant_id format (must be sub-<label>): {invalid_ids}")

        # Rule 4 — no duplicate IDs
        duplicates = df[df["participant_id"].duplicated()]["participant_id"].tolist()
        if duplicates:
            errors.append(f"Duplicate participant_id values: {duplicates}")

        # Rule 5 — no empty participant_id values
        empty = df["participant_id"].isna().sum()
        if empty:
            errors.append(f"{empty} empty participant_id value(s) found")

    # Rule 6 — no fully empty rows
    empty_rows = df[df.isna().all(axis=1)]
    if not empty_rows.empty:
        warnings.append(f"{len(empty_rows)} fully empty row(s) found")

    # Report
    for w in warnings:
        logger.warning(f"⚠️  {w}")

    if errors:
        for e in errors:
            logger.error(f"❌ {e}")
        logger.error(f"\n  participants.tsv — INVALID ({len(errors)} error(s))")
        return False, list(df.columns), df
    else:
        logger.info(f"  ✅ participants.tsv — VALID")
        return True, list(df.columns), df


def validate_json(json_path: Path, tsv_columns: list, logger: logging.Logger) -> bool:
    """
    Validates participants.json against BIDS spec.
    Returns True if valid.
    """
    logger.info(f"\n{'=' * 50}")
    logger.info(f"  VALIDATING participants.json")
    logger.info(f"{'=' * 50}\n")

    errors   = []
    warnings = []

    # Load
    try:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Could not read participants.json: {e}")
        return False

    logger.info(f"📋 Loaded {len(data)} key(s): {list(data.keys())}\n")

    # Rule 1 — every key must have a Description field
    for key, value in data.items():
        if not isinstance(value, dict):
            errors.append(f"Key '{key}' must be a JSON object, got {type(value).__name__}")
            continue
        if "Description" not in value:
            errors.append(f"Key '{key}' is missing required 'Description' field")

    # Rule 2 — every TSV column (except participant_id) must have a JSON entry
    tsv_non_id_cols = [c for c in tsv_columns if c != "participant_id"]
    missing_in_json = [c for c in tsv_non_id_cols if c not in data]
    if missing_in_json:
        errors.append(f"TSV columns missing from JSON: {missing_in_json}")

    # Rule 3 — every JSON key must correspond to a TSV column
    extra_in_json = [k for k in data.keys() if k not in tsv_columns]
    if extra_in_json:
        warnings.append(f"JSON keys not found in TSV columns: {extra_in_json}")

    # Report
    for w in warnings:
        logger.warning(f"⚠️  {w}")

    if errors:
        for e in errors:
            logger.error(f"❌ {e}")
        logger.error(f"\n  participants.json — INVALID ({len(errors)} error(s))")
        return False
    else:
        logger.info(f"  ✅ participants.json — VALID")
        return True


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Validate BIDS participants files.")
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="BIDS root directory")
    args   = parser.parse_args()

    root     = Path(args.root)
    tsv_path = root / "participants.tsv"
    json_path = root / "participants.json"

    logger = setup_logging()

    # Header
    logger.info(f"\n{'=' * 50}")
    logger.info(f"  BIDS PARTICIPANTS VALIDATION")
    logger.info(f"  Root   : {root}")
    logger.info(f"  Run at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'=' * 50}")

    # Guards
    if not tsv_path.exists():
        logger.error(f"❌ participants.tsv not found at: {tsv_path}")
        sys.exit(1)

    if not json_path.exists():
        logger.error(f"❌ participants.json not found at: {json_path}")
        sys.exit(1)

    # Validate
    tsv_ok,  tsv_columns, _ = validate_tsv(tsv_path,  logger)
    json_ok                  = validate_json(json_path, tsv_columns, logger)

    # Final summary
    logger.info(f"\n{'=' * 50}")
    logger.info(f"  FINAL SUMMARY")
    logger.info(f"{'=' * 50}")

    if tsv_ok:
        logger.info(f"  ✅ participants.tsv  — VALID")
    else:
        logger.error(f"  ❌ participants.tsv  — INVALID")

    if json_ok:
        logger.info(f"  ✅ participants.json — VALID")
    else:
        logger.error(f"  ❌ participants.json — INVALID")

    overall = tsv_ok and json_ok
    logger.info(f"\n  {'ALL CHECKS PASSED ✅' if overall else 'VALIDATION FAILED ❌'}")
    logger.info(f"{'=' * 50}\n")

    sys.exit(0 if overall else 1)


if __name__ == "__main__":
    main()