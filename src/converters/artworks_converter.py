import logging
from tqdm import tqdm

# Internal imports
from src import config as cfg
from src import utils
from src.utils import perform_copy  # Assumes signature: (source_path, target_dir, file_name, json_data)

logger = logging.getLogger(__name__)


class ArtworksConverter:
    """
    Handles the organization of artwork images into the BIDS structure.
    Refactored for Single Responsibility and Robustness.
    """

    def __init__(self, study_config):
        self.config = study_config
        # STRICT MODE: Crash immediately if config keys are missing
        self.source_dir = cfg.get_input_path(study_config, "Art")
        self.dist_root = cfg.get_output_path(study_config, "Art")

    def run(self):
        """Main public entry point."""
        if not self._validate_paths():
            return

        files = self._gather_files()
        if not files:
            logger.info(f"ℹ️  [Artworks] No images found in {self.source_dir}")
            return

        print(f"\n🎨 Starting Artworks Conversion ({len(files)} files)")
        print(f"   Source: {self.source_dir}")
        print("-" * 50)

        success_count = 0
        failure_count = 0

        for file_path in tqdm(files, desc="Converting", unit="img"):
            if self._process_single_file(file_path):
                success_count += 1
            else:
                failure_count += 1

        print("-" * 50)
        final_msg = f"✅ Pipeline Complete. Success: {success_count}, Failures: {failure_count}"
        logger.info(final_msg)
        print(final_msg + "\n")

    def _process_single_file(self, file_path):
        """
        Processes a single artwork file by determining its task type, preparing necessary
        information, and performing the required file operation.

        This method parses the provided file's name to extract relevant details and routes
        the file to an appropriate handler based on its task type. The handler determines
        the destination folder and a new file name for the operation. If any error occurs
        during the process, it is logged.

        Parameters:
        file_path (Path): The path to the file being processed.

        Returns:
        bool: True if the file was successfully processed, False otherwise.

        Raises:
        Exception: If an error occurs during the file operation or preparation.
        """
        # Parse info
        info = utils.parse_artwork_filename(file_path.name)
        if not info:
            logger.info(f"⚪ Skipping: {file_path.name} (Pattern mismatch)")
            return False

        task_type = info.get('task')

        try:
            # Route to specific handler
            # Handlers return a tuple: (destination_folder_path, new_filename_string)
            if task_type == 'together':
                dest_folder, new_name = self._prepare_together(info, file_path.suffix)
            elif task_type == 'solo':
                dest_folder, new_name = self._prepare_solo(info, file_path.suffix)
            else:
                logger.warning(f"⚠️ Unknown task type '{task_type}' for {file_path.name}")
                return False

            # Execute Copy (I/O)
            perform_copy(
                source_path=file_path,
                target_dir=dest_folder,
                file_name=new_name
            )

            self._log_success(file_path.name, new_name)
            return True

        except Exception as e:
            self._log_error(file_path, e)
            return False

    # Logic Handlers

    def _prepare_together(self, info, file_suffix):
        """Validates metadata and generates paths for 'Together' task."""
        # Validate
        self._validate_metadata(info, required_keys=['dyad', 'ses', 'task_num'], context="Together Task")

        # Extract
        dyad_id = info['dyad']
        ses_id = info['ses']
        task_num = info['task_num']

        # Construct Filename
        # ses-01_acq-dyad1001_task-codrawing1_artwork.tif
        new_name = (
            f"ses-{ses_id}_acq-dyad{dyad_id}_"
            f"task-codrawing{task_num}_artwork{file_suffix.lower()}"
        )

        # Determine Folder
        dest_folder = self.dist_root / "together"

        return dest_folder, new_name

    def _prepare_solo(self, info, file_suffix):
        """Validates metadata and generates paths for 'Solo' task."""
        # Validate
        self._validate_metadata(info, required_keys=['dyad', 'ses', 'sub'], context="Solo Task")

        # Extract
        sub_id = info['sub']
        dyad_id = info['dyad']
        ses_id = info['ses']

        # Construct Filename
        # sub-101_ses-01_acq-dyad1001_task-drawingalone_artwork.tif
        new_name = (
            f"sub-{sub_id}_ses-{ses_id}_acq-dyad{dyad_id}_"
            f"task-drawingalone_artwork{file_suffix.lower()}"
        )

        # Determine Folder
        dest_folder = self.dist_root / "alone"

        return dest_folder, new_name

    #  Validation Helper

    def _validate_metadata(self, info, required_keys, context):
        """
        Generic validator. Raises ValueError if any key is missing or None.
        """
        missing = [key for key in required_keys if not info.get(key)]

        if missing:
            raise ValueError(
                f"❌ Incomplete Metadata ({context}): Required {required_keys}.\n"
                f"   Missing fields: {missing}\n"
                f"   Extracted Info: {info}"
            )

    # Setup & Logging Helpers

    def _validate_paths(self):
        if not self.source_dir.exists():
            print(f"⚠️ Source not found: {self.source_dir}")
            return False
        # Create root output at once
        self.dist_root.mkdir(parents=True, exist_ok=True)
        return True

    def _gather_files(self):
        extensions = ['*.tif', '*.tiff', '*.jpg', '*.png']
        files = []
        # Adds lowercase and uppercase files to the list
        for ext in extensions:
            files.extend(list(self.source_dir.glob(ext)))
            files.extend(list(self.source_dir.glob(ext.upper())))
        return sorted(list(set(files)))

    def _log_success(self, source_label, filename):
        # Using <30 to align columns nicely in console
        tqdm.write(f"✅ {source_label:<30} -> {filename}")
        logger.info(f"Converted: {source_label} -> {filename}")

    def _log_error(self, file_path, e):
        msg = f"❌ Error processing {file_path.name}: {e}"
        logger.error(msg, exc_info=True)
        tqdm.write(msg)