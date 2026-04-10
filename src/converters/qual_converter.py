from pathlib import Path

from src.converters.base_converter import BaseConverter
from src import utils, loaders, config as cfg
from src.utils import perform_copy


class QualConverter(BaseConverter):
    """
    Converts raw qualitative interview files into BIDS-style structure.
    """

    def __init__(self, study_config):
        super().__init__(
            study_config=study_config,
            input_key="qual",
            output_key="qual",
            file_extensions=["*.docx"]
        )
        self.dyad_lookup = {}

    def _pre_run_setup(self):
        """Load dyad mapping before processing."""
        self.dyad_lookup = loaders.load_dyad_mapping(cfg.DYAD_FILE)

    def _process_single_file(self, file_path: Path) -> bool:
        # Parse info
        info = utils.parse_qual_file(file_path.name)
        if not info:
            self.log_error(file_path.name, "Skipping: Invalid Filename Pattern")
            return False

        # Validation
        try:
            self._validate_metadata(info, keys=["sub", "ses", "dyad"], context="Qual")
        except ValueError as e:
            self.log_error(file_path.name, str(e))
            return False

        sub_id  = info.get("sub")
        ses_id  = info.get("ses")
        dyad_id = info.get("dyad")

        # Construct new filename
        new_file_name = (
            f"sub-{sub_id}_"
            f"ses-{ses_id}_"
            f"acq-dyad{dyad_id}_"
            f"exitinterview.docx"
        )

        # Define destination
        dest_dir = self.output_root / "exit_interviews"

        # Perform copy
        try:
            perform_copy(file_path, dest_dir, new_file_name)
            self.log_success(file_path.name, new_file_name)
            return True
        except Exception as e:
            self.log_error(file_path.name, e)
            return False