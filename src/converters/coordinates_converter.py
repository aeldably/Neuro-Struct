from pathlib import Path
from typing import Dict, List

from src.converters.base_converter import BaseConverter
from src import utils
from src.utils import perform_copy


class CoordinatesConverter(BaseConverter):
    """
    Class for converting and organizing coordinate data based on provided study configurations.

    This class is designed to handle processing of subject-specific coordinate folders,
    validate metadata, and map files to specific outputs while adhering to a predefined
    naming convention. It extends `BaseConverter` and provides specialized behavior for
    handling folder-based input data in contrast to file-based inputs.

    Attributes:
        input_key: The key represents the input data type ("Coord").
        output_key: The key represents the data output destination ("Coord").
        file_extensions: A list of patterns to locate subject folders (e.g., ["sub-*"]).

    Raises:
        ValueError: Raised during metadata validation if required keys for the context
        are missing or invalid.
    """

    def __init__(self, study_config: Dict):
        super().__init__(
            study_config,
            input_key="Coord",  # Maps to 'inputs/RawCoordinates' in config
            output_key="Coord",  # Maps to output root
            file_extensions=["sub-*"]  # Pattern to find the subject folders
        )

    def _gather_files(self) -> List[Path]:
        """Override the default behavior to look for FOLDERS, not files."""
        return self._gather_folders()

    def _process_single_file(self, folder_path: Path) -> bool:
        """
        Processing logic for a single subject folder (e.g., sub-101_session-1).
        Inside this folder, we look for the specific CSV/MAT files.
        """

        # Parse Metadata from Folder Name
        info = utils.parse_coordinates_folder(folder_path.name)
        if not info:
            self.log_error(folder_path.name, "Skipping: Invalid Folder Name Pattern")
            return False

        # Validation
        try:
            self._validate_metadata(info, keys=["sub", "ses", "dyad"], context="Coordinates")
        except ValueError as e:
            self.log_error(folder_path.name, str(e))
            return False

        sub_id = info.get("sub")
        ses_id = info.get("ses")
        dyad_id = info.get("dyad")

        # Create the Naming Prefix (The "Stem")
        # Result: "sub-101_ses-01_acq-dyad1001_"
        # Note: We use an underscore at the end so we can just append the suffix.
        filename_prefix = (f"sub-{sub_id}_ses-{ses_id}"
                           f"_acq-dyad{dyad_id}_")

        # Define the Mapping (Source Filename -> Custom Output Suffix)
        files_map = {
            # Source File (Dynamic based on folder name)
            # Target Suffix (Custom)
            f"{folder_path.name}.csv": "optodeCoordinates.csv",
            f"{folder_path.name}_distances.csv": "channelLengths.csv",
            f"{folder_path.name}_optode_dist.csv": "distancesToRoi.csv",

            # (Optional)
            # "opto.mat": "coordsystem.mat",
            # "opto_MNI.mat": "coordsystem_MNI.mat"
        }

        # Define Destination Directory
        # Output location: Coordinates/sub-101/ses-01
        dest_dir = (self.output_root /
                    f"sub-{sub_id}" /
                    f"ses-{ses_id}" )

        success_any = False

        # Execution Loop
        for src_name, target_suffix in files_map.items():
            source_file = folder_path / src_name

            if source_file.exists():
                # Combine Prefix + Suffix
                # e.g. sub-101_ses-01_acq-dyad1001_optodeCoordinates.csv
                new_name = filename_prefix + target_suffix

                try:
                    perform_copy(source_file, dest_dir, new_name)
                    success_any = True
                except Exception as e:
                    self.log_error(f"{folder_path.name}/{src_name}", e)
            else:
                # Silent skip for missing files
                pass

        return success_any