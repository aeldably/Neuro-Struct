from ast import Str
from pathlib import Path
from typing import Dict, List

from src.converters.base_converter import BaseConverter

from src import utils
from src.utils import perform_copy


class MoCapConverter(BaseConverter):
    """

    """

    def __init__(self, study_config: Dict):
        super().__init__(
            study_config,
            input_key="MoCap",
            output_key="MoCap",  # Maps to output root
            file_extensions=["*.csv"]
        )

        self.task_map = {
            "1": "drawingalone",
            "2": "codrawing1",
            "3": "codrawing2",
            "4": "collaborativetask"
        }

    def _process_single_file(self, file_path: Path) -> bool:
        """
        """
        info = utils.parse_mocap_file(filename=file_path.name)
        if not info:
            self.log_error(file_path.name, "Skipping: Invalid Folder Name Pattern")
            return False

        # Validation
        try:
            self._validate_metadata(info, keys=["dyad", "ses", "task_num"], context="MoCap")
        except ValueError as e:
            self.log_error(file_path.name, str(e))
            return False

        dyad_id = info.get("dyad")
        ses_id  = info.get("ses")
        task_num = info.get("task_num")

        # map task number to task name
        task_name = self.task_map.get(task_num)

        if not task_name:
            self.log_error(file_path.name, f"Skipping: Unknown Task Number '{task_num}'")
            return False

        # construct a new file name
        # Format: sub-dyad1001_ses-01_acq-dyad1001_task-drawingalone_mocap.csv

        suffix = "mocap.csv"
        new_file_name = (
            f"sub-dyad{dyad_id}_"  # Subject Prefix
            f"ses-{ses_id}_"  # Session
            f"acq-dyad{dyad_id}_"  # dyad-id
            f"task-{task_name}_"  # Task Name
            f"{suffix}"  # Suffix
        )

        # define destination
        dest_dir = (self.output_root /
                    f"acq-dyad{dyad_id}" /
                    f"ses-{ses_id}"
                    )

        # perform copy
        try:
            perform_copy(file_path, dest_dir, new_file_name)
            return True
        except Exception as e:
            self.log_error(file_path.name, e)
            return False