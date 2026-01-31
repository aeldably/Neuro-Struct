import json
import glob
import shutil
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

from mne_bids import make_dataset_description

# Ensure the file is named filename_parser.py in src folder
from src.filename_parser import FilenameParser
from src import config as cfg

# Create one shared instance
_parser = FilenameParser()

# --- Wrappers (API) ---

def parse_filename(filename: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Used by NirsConverter.
    Wraps parse_common_components to return (sub, ses, run).
    """
    return _parser.parse_common_components(filename)

def parse_artwork_filename(filename: str) -> Optional[Dict[str, Any]]:
    """
    Used by ArtworksConverter.
    Wraps parse_artwork_filename to return a dictionary of metadata.
    """
    return _parser.parse_artwork_file(filename)

def parse_coordinates_folder(folder_name: str) -> Optional[Dict[str, Any]]:
    """
    Used by Coordinate patching logic.
    Wraps parse_coordinates_folder.
    """
    return _parser.parse_coordinates_folder(folder_name)

def parse_mocap_file(filename: str) -> Optional[Dict[str, Any]]:
    """
    Used by MoCapConverter.
    """
    return _parser.parse_mocap_file(filename)

# General Utilities

def patch_nirs_coords(bids_root: Path):
    """
    Hot-fixes the missing 'NIRSCoordinateProcessingDescription' field in sidecar files.
    """
    files = glob.glob(str(bids_root / "**" / "*_coordsystem.json"), recursive=True)

    for file in files:
        with open(file, 'r') as f:
            data = json.load(f)

        if "NIRSCoordinateProcessingDescription" not in data:
            data["NIRSCoordinateProcessingDescription"] = "n/a"

            with open(file, 'w') as f:
                json.dump(data, f, indent=4)

def generate_description(bids_root: Path, config: dict):
    """
    Writes the mandatory 'dataset_description.json' file to the BIDS root.
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

def run_inventory(study_config):
    """
    Job 1: Check if input folders exist.
    """
    print(f"\n{'=' * 50}")
    print(f"🚀 PIPELINE INVENTORY CHECK")
    print(f"{'=' * 50}")

    sources = study_config.get("Sources", {})
    for key, folder_name in sources.items():
        try:
            path = cfg.get_input_path(study_config, key)
            status = "✅ Found" if path.exists() else "❌ Missing"
            count = len(list(path.glob("*"))) if path.exists() else 0
            print(f"📂 {key:<10} ({folder_name}): {status} ({count} items)")
        except Exception:
            print(f"⚠️ {key:<10} : Config Error")
    print(f"{'-' * 50}\n")

def perform_copy(source_path, target_dir, file_name, json_data=None):
    """
    Copies a file to a target directory and optionally creates a JSON sidecar file.
    """
    # Prepare Target Directory
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / file_name

    # Copy the File
    shutil.copy2(source_path, target_path)

    # Create Sidecar (optional)
    if json_data:
        json_name = Path(file_name).stem + ".json"
        json_path = target_dir / json_name

        with open(json_path, 'w') as f:
            json.dump(json_data, f, indent=4)