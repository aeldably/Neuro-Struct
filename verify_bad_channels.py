# verify_bad_channels.py
# Run from the project root: python verify_bad_channels.py

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from src.converters.bad_channel_converter import BadChannelConverter


# Configuration
PATCHED_DIR   = Path("outputs/BrainImaging")
EXCLUSION_CSV = Path("excludedChannels_visualInspection.csv")
LOG_DIR       = Path("logs/verification")


SESSION_MAP   = BadChannelConverter.SESSION_MAP
RECORDING_MAP = BadChannelConverter.RECORDING_MAP


# Logging Setup
def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file  = LOG_DIR / f"bad_channel_verification_{timestamp}.log"

    logger = logging.getLogger("ChannelVerifier")
    logger.setLevel(logging.DEBUG)

    # File Handler — full detail
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # Console Handler — same output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"📝 Log saved to: {log_file}")
    return logger


# Main
def main():
    logger = setup_logging()

    logger.info(f"\n{'=' * 60}")
    logger.info(f"  BAD CHANNEL VERIFICATION REPORT")
    logger.info(f"  Source : {EXCLUSION_CSV}")
    logger.info(f"  Output : {PATCHED_DIR}")
    logger.info(f"{'=' * 60}\n")

    # Guards
    if not EXCLUSION_CSV.exists():
        logger.error(f"Exclusion CSV not found: {EXCLUSION_CSV}")
        return

    if not PATCHED_DIR.exists():
        logger.error(f"Patched output directory not found: {PATCHED_DIR}")
        return

    df_excl = pd.read_csv(EXCLUSION_CSV, dtype=str)
    logger.info(f"📋 Loaded {len(df_excl)} exclusion rows\n")

    all_ok        = True
    checked_count = 0
    failed_count  = 0
    missing_count = 0

    for _, row in df_excl.iterrows():
        sub     = str(row["ID"]).strip().zfill(3)
        ses_raw = str(row["session"]).strip()
        rec_raw = str(row["recording"]).strip().lower()
        channel = str(row["channel"]).strip()

        ses = SESSION_MAP.get(ses_raw)
        if not ses:
            logger.warning(f"⚠️  Unknown session '{ses_raw}' in exclusion CSV — skipping row (ID={sub})")
            missing_count += 1
            all_ok = False
            continue

        run = RECORDING_MAP.get(rec_raw)
        if not run:
            logger.warning(f"⚠️  Unknown recording '{rec_raw}' in exclusion CSV — skipping row (ID={sub}, ses={ses})")
            missing_count += 1
            all_ok = False
            continue

        # Find the specific TSV for this subject/session/run
        pattern = f"**/sub-{sub}_ses-{ses}_*_run-{run}_channels.tsv"
        matches = list(PATCHED_DIR.glob(pattern))

        if not matches:
            logger.warning(f"⚠️  NO FILE FOUND : sub-{sub} ses-{ses} run-{run} | channel={channel}")
            missing_count += 1
            all_ok = False
            continue

        for tsv_path in matches:
            df = pd.read_csv(tsv_path, sep="\t")

            matched_rows = df[df["name"].str.startswith(channel)]

            if matched_rows.empty:
                logger.warning(f"⚠️  CHANNEL NOT FOUND : sub-{sub} ses-{ses} run-{run} | {channel}")
                missing_count += 1
                all_ok = False
                continue

            for _, ch_row in matched_rows.iterrows():
                status = str(ch_row["status"]).strip().lower()
                ok     = status == "bad"
                icon   = "✅" if ok else "❌"

                if not ok:
                    all_ok = False
                    failed_count += 1
                    logger.error(
                        f"{icon} sub-{sub} ses-{ses} run-{run} "
                        f"| {ch_row['name']:<25} "
                        f"| status={ch_row['status']} (expected: bad)"
                    )
                else:
                    logger.info(
                        f"{icon} sub-{sub} ses-{ses} run-{run} "
                        f"| {ch_row['name']:<25} "
                        f"| status={ch_row['status']}"
                    )

                checked_count += 1

    # Final Summary
    logger.info(f"\n{'=' * 60}")
    logger.info(f"  SUMMARY")
    logger.info(f"  Checked : {checked_count} channel rows")
    if missing_count:
        logger.warning(f"  Missing : {missing_count} (file or channel not found)")
    else:
        logger.info(f"  Missing : {missing_count}")

    if failed_count:
        logger.error(f"  Failed  : {failed_count} (status was not 'bad')")
    else:
        logger.info(f"  Failed  : {failed_count}")
    logger.info(f"\n  {'ALL CHECKS PASSED ✅' if all_ok else 'SOME CHECKS FAILED ❌'}")
    logger.info(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()