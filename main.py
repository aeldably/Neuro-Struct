import re
import warnings
import pandas as pd
import mne
from pathlib import Path
from typing import Dict, Optional, Tuple
from mne_bids import BIDSPath, write_raw_bids

# ==========================================
# 1. CONFIGURATION
# ==========================================
SOURCE_DIR = Path("dataset")
BIDS_ROOT = Path("bids_dataset")
DYAD_FILE = Path("dyadlist.csv")
TASK_NAME = "drawing"

# ==========================================
# 2. LOGIC FUNCTIONS
# ==========================================

def load_dyad_mapping(csv_path: Path) -> Dict[str, str]:
    if not csv_path.exists():
        print(f"⚠️  Warning: Dyad mapping file '{csv_path}' not found.")
        return {}

    try:
        df = pd.read_csv(csv_path, dtype=str)
        long_df = df.melt(
            id_vars=['dyadID'],
            value_vars=['pID1', 'pID2'],
            value_name='sub_id'
        )
        mapping = long_df.set_index('sub_id')['dyadID'].to_dict()
        return mapping
    except Exception as e:
        print(f"❌ Error reading dyad list: {e}")
        return {}


def parse_filename(filename: str) -> Tuple[Optional[str], Optional[str]]:
    sub_match = re.search(r"sub-(\d+)", filename)
    ses_match = re.search(r"session-(\d+)", filename)

    if sub_match and ses_match:
        return sub_match.group(1), f"{int(ses_match.group(1)):02d}"
    return None, None


# ==========================================
# 3. MAIN EXECUTION
# ==========================================

def main():
    # 1. Setup & Config Log
    if not SOURCE_DIR.exists():
        print(f"❌ Critical Error: Source folder '{SOURCE_DIR}' does not exist.")
        return

    dyad_lookup = load_dyad_mapping(DYAD_FILE)
    snirf_files = sorted(list(SOURCE_DIR.glob("*.snirf")))
    total_files = len(snirf_files)

    print(f"\n{'='*40}")
    print(f"🚀 BIDS CONVERSION STARTING")
    print(f"{'='*40}")
    print(f"📂 Source:    {SOURCE_DIR}")
    print(f"🎯 Task:      {TASK_NAME}")
    print(f"👥 Dyad Map:  {len(dyad_lookup)} pairs loaded")
    print(f"📄 Files:     {total_files} found")
    print(f"{'-'*40}\n")

    if not snirf_files:
        print("ℹ️  No files to process. Exiting.")
        return

    # Processing Loop
    for i, file_path in enumerate(snirf_files, 1):
        filename = file_path.name

        # --- 1. Parse Name ---
        sub_id, ses_id = parse_filename(filename)
        if not sub_id:
            print(f"[{i}/{total_files}] ⏭️  Skipping {filename} (Pattern mismatch)")
            continue

        print(f"[{i}/{total_files}] Processing sub-{sub_id} (ses-{ses_id})...")

        # --- 2. Determine Dyad ---
        dyad_num = dyad_lookup.get(str(int(sub_id)))
        if dyad_num:
            acq_label = f"dyad{dyad_num}"
            print(f"       ↳  Mapped to Dyad: {dyad_num}")
        else:
            acq_label = None
            print(f"       ⚠️  No Dyad ID found for sub-{sub_id}")

        # Load & Convert
        try:
            # Suppressing unnecessary warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                # Read the raw file
                raw = mne.io.read_raw_snirf(file_path,
                                            preload=False,
                                            verbose=False)

                # Setting the metadata to none
                raw.set_meas_date(None)

                # Check for montage silently
                montage = raw.get_montage()
                if montage:
                    raw.set_montage(montage)

                # Write
                bids_path = BIDSPath(
                    subject=sub_id,
                    session=ses_id,
                    task=TASK_NAME,
                    acquisition=acq_label,
                    datatype="nirs",
                    root=BIDS_ROOT
                )

                write_raw_bids(raw, bids_path, overwrite=True, verbose=False)

            # Only print this if the block above succeeded
            print(f"       ✅  Success: {bids_path.basename}")

            # Manual check: Warn user if montage was missing
            if not montage:
                print(f"       ⚠️  Warning: No montage found (Spatial data missing)")

        except Exception as e:
            print(f"       ❌  Failed: {e}")

    print(f"\n{'-'*40}")
    print("🎉 Batch processing complete.")

if __name__ == "__main__":
    main()