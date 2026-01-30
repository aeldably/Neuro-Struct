from src.converters.base_converter import BaseConverter
from src import utils
from src.utils import perform_copy


class ArtworksConverter(BaseConverter):
    """
    Organizes artwork images into BIDS 'source' structure (Together vs Solo).
    """

    def __init__(self, study_config):
        super().__init__(
            study_config,
            input_key="Art",
            output_key="Art",
            file_extensions=["*.tif", "*.tiff", "*.jpg", "*.png"]
        )

    def _process_single_file(self, file_path):
        # Parse Info
        info = utils.parse_artwork_filename(file_path.name)
        if not info:
            # Silent skip for unrelated files, or log debug
            return False

        task_type = info.get('task')

        try:
            # Determine Strategy
            if task_type == 'together':
                dest_folder, new_name = self._prepare_together(info, file_path.suffix)
            elif task_type == 'solo':
                dest_folder, new_name = self._prepare_solo(info, file_path.suffix)
            else:
                self.log_error(file_path.name, f"Unknown task type: {task_type}")
                return False

            # Execute Copy
            perform_copy(
                source_path=file_path,
                target_dir=dest_folder,
                file_name=new_name
            )

            self.log_success(file_path.name, new_name)
            return True

        except Exception as e:
            self.log_error(file_path.name, e)
            return False

    # LOGIC HANDLERS
    def _prepare_together(self, info, suffix):
        self._validate_metadata(info, ['dyad', 'ses', 'task_num'], "Together")

        new_name = (
            f"ses-{info['ses']}_acq-dyad{info['dyad']}_"
            f"task-codrawing{info['task_num']}_artwork{suffix.lower()}"
        )
        return self.output_root / "together", new_name

    def _prepare_solo(self, info, suffix):
        self._validate_metadata(info, ['dyad', 'ses', 'sub'], "Solo")

        new_name = (
            f"sub-{info['sub']}_ses-{info['ses']}_acq-dyad{info['dyad']}_"
            f"task-drawingalone_artwork{suffix.lower()}"
        )
        return self.output_root / "alone", new_name