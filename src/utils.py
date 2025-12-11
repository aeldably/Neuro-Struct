import re
import json
import glob
from pathlib import Path
from typing import Tuple, Optional
from mne_bids import make_dataset_description


def parse_filename(filename: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extracts Subject, Session, and Run IDs from a filename using regex.

    It enforces a strict structure: if 'sub' or 'ses' tags are missing,
    it returns None to signal that the file should be skipped.
    It also handles leading zero normalization (e.g., 'ses-1' -> '01').
    """
    # Case-insensitive matching for flexible naming (sub-01, SUB-01)
    sub_match = re.search(r"sub-?(\d+)", filename, re.IGNORECASE)
    ses_match = re.search(r"(?:ses|session)-?(\d+)", filename, re.IGNORECASE)
    run_match = re.search(r"run-?(\d+)", filename, re.IGNORECASE)

    # Mandatory fields: Subject and Session are required for BIDS
    if not sub_match: return None, None, None
    if not ses_match: return sub_match.group(1), None, None

    # Normalization: Ensure standard BIDS formatting (e.g., two digits for session)
    sub = sub_match.group(1)
    ses = f"{int(ses_match.group(1)):02d}"
    run = f"{int(run_match.group(1)):02d}" if run_match else None

    return sub, ses, run


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