import re
import json
import glob
from pathlib import Path
from typing import Tuple, Optional
from mne_bids import make_dataset_description


def parse_filename(filename: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extracts Subject, Session, and Run IDs from a filename.
    Ensures EVERY file has a run number for consistency.

    Logic:
    1. 'session-a_1' -> ses-01, run-01
    2. 'session-b_1' -> ses-01, run-02
    3. 'ses-01'      -> ses-01, run-01 (Default)
    """

    # 1. SPECIAL CASE: Handle the "session-a" / "session-b" format
    # Regex: session-([a-z]) matches the 'a' or 'b'
    #        _(\d+)          matches the session number
    split_match = re.search(r"session-([a-z])_(\d+)", filename, re.IGNORECASE)

    if split_match:
        # Extract Subject (standard regex)
        sub_match = re.search(r"sub-?(\d+)", filename, re.IGNORECASE)
        if not sub_match: return None, None, None

        sub = sub_match.group(1)

        # Extract Session (the number at the end)
        raw_ses = split_match.group(2)
        ses = f"{int(raw_ses):02d}"

        # Extract Run (convert letter to number)
        run_char = split_match.group(1).lower()
        run_num = ord(run_char) - 96  # 'a'=1, 'b'=2
        run = f"{run_num:02d}"

        return sub, ses, run

    # 2. STANDARD CASE: Standard BIDS format
    sub_match = re.search(r"sub-?(\d+)", filename, re.IGNORECASE)
    ses_match = re.search(r"(?:ses|session)-?(\d+)", filename, re.IGNORECASE)

    if sub_match and ses_match:
        sub = sub_match.group(1)
        ses = f"{int(ses_match.group(1)):02d}"

        # Check if an explicit run number already exists
        run_match = re.search(r"run-?(\d+)", filename, re.IGNORECASE)

        if run_match:
            run = f"{int(run_match.group(1)):02d}"
        else:
            # FORCE CONSISTENCY: If no run is specified, default to '01'
            run = "01"

        return sub, ses, run

    # If nothing matched
    return None, None, None


def patch_nirs_coords(bids_root: Path):
    """
    Hot-fixes the missing 'NIRSCoordinateProcessingDescription' field in sidecar files.

    MNE-BIDS doesn't write this field by default for raw data, which triggers a BIDS
    validation warning. This function scans all generated JSONs and stamps "n/a"
    to confirm that no coordinate manipulation was done.
    """
    files = glob.glob(str(bids_root / "**" / "*_coordsystem.json"), recursive=True)

    for file in files:
        with open(file, 'r') as f:
            data = json.load(f)

        # Only inject if the field is actually missing
        if "NIRSCoordinateProcessingDescription" not in data:
            data["NIRSCoordinateProcessingDescription"] = "n/a"

            with open(file, 'w') as f:
                json.dump(data, f, indent=4)


def generate_description(bids_root: Path, config: dict):
    """
    Writes the mandatory 'dataset_description.json' file to the BIDS root.

    Uses values from the study_config.json to populate fields like Study Name,
    Authors, and License. This ensures the dataset top-level metadata is correct.
    """
    make_dataset_description(
        path=bids_root,
        name=config.get("StudyName", "Untitled"),
        authors=config.get("Authors", []),
        data_license=config.get("DataLicense", "CC0"),
        source_datasets=config.get("SourceDatasets", []),
        overwrite=True,
        verbose=False
    )