import json
import glob
import shutil
from pathlib import Path
from typing import Optional, Dict, Any

from mne_bids import make_dataset_description

from src.filename_parser import FilenameParser
from src import config as cfg

# Create one shared instance
_parser = FilenameParser()

def parse_filename(filename: str):
    """
    This is the function NirsConverter calls.
    It passes the work to your new class.
    """
    return _parser.parse_all(filename)

def parse_artwork_filename(filename: str) -> Optional[Dict[str, Any]]:
    """Artwork wrapper (Used by ArtworksConverter)."""
    return _parser.parse_artwork_string(filename)


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
    Authors, and License. This ensures the fNIRSDataset top-level metadata is correct.
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
    Delegates to: src.config (or a dedicated utility if preferred,
    but commonly kept here or in utils for simplicity).
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

    The function moves the specified file from the source path to the target
    directory, creating the directory structure if necessary. If JSON data is
    provided, a sidecar JSON file is created in the same target directory with
    the same base name as the copied file.

    Arguments:
        source_path (Path): The path of the file to be copied.
        target_dir (Path): The directory where the file will be copied.
        file_name (str): The name of the file in the target directory.
        json_data (Optional[dict]): JSON data to write to a sidecar file, if
            provided.

    Raises:
        OSError: If an error occurs while creating directories, copying files,
            or writing the sidecar JSON file.
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