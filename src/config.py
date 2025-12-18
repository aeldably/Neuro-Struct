from pathlib import Path
import json


# We go up two levels (.parent.parent) because this script lives in 'src/',
# so we need to step back to the main folder to find the config files.
PROJ_ROOT = Path(__file__).resolve().parent.parent

# Hardcoded filenames that are expected to exist in the project root.
# These act as the entry points for the study metadata.
CONFIG_FILE = PROJ_ROOT / "study_config.json"
DYAD_FILE = PROJ_ROOT / "dyadlist.csv"
PARTICIPANTS_FILE = PROJ_ROOT / "questionnaireData.csv"


def load_study_config():
    """
    Loads the main JSON configuration file.

    Raises an error immediately if the file is missing to prevent
    silent failures later in the pipeline.
    """
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Config file missing at {CONFIG_FILE}")
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


def get_input_path(config: dict, source_key: str) -> Path:
    """
    Constructs the full path to an INPUT folder.
    Logic: PROJ_ROOT / InputRoot / SourceFolder
    """
    dirs = config.get("Directories", {})
    sources = config.get("Sources", {})

    input_root_name = dirs.get("InputRoot", "inputs")
    source_folder_name = sources.get(source_key)

    if not source_folder_name:
        raise ValueError(f"Source key '{source_key}' not found in config 'Sources'.")

    return PROJ_ROOT / input_root_name / source_folder_name

def get_output_path(config: dict, dest_key: str) -> Path:
    """
    Constructs the full path to an OUTPUT folder.
    Logic: PROJ_ROOT / OutputRoot / DestFolder
    """
    dirs = config.get("Directories", {})
    dests = config.get("Destination", {})

    output_root_name = dirs.get("OutputRoot", "outputs")
    dest_folder_name = dests.get(dest_key)

    if not dest_folder_name:
        raise ValueError(f"Destination key '{dest_key}' not found in config 'Destination'.")

    return PROJ_ROOT / output_root_name / dest_folder_name


def resolve_path(path_str: str) -> Path:
    """
    Resolves file paths from the config JSON.

    If the path is absolute (e.g. /Users/name/...), it uses it as is.
    If relative (e.g. "fNIRSDataset"), it anchors it to the PROJ_ROOT
    to ensure portability across different machines.
    """
    path = Path(path_str)
    if path.is_absolute(): return path
    return PROJ_ROOT / path