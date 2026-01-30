import logging
from abc import ABC, abstractmethod
from pathlib import Path
from tqdm import tqdm
from src import config as cfg

# Common Logger
logger = logging.getLogger(__name__)

class BaseConverter(ABC):
    """
    Abstract Base Class for implementing data conversion processes.

    This class serves as a blueprint for data conversion workflows. It provides a predefined
    structure for the entire conversion process, including path validation, file gathering,
    processing, and final reporting. Additional customization points are provided as hooks
    or through mandatory implementation of abstract methods in child classes. The class
    also facilitates logging for successful and failed operations.

    Attributes:
        config (dict): Configuration dictionary containing the study settings.
        extensions (list): File extensions to process (e.g., ['*.tif', '*.snirf']).
        input_key (str): Key to retrieve the source path from the study configuration.
        source_dir (Path or None): Source directory where the files to be converted reside.
        output_root (Path or None): Destination root directory for converted data.
    """

    def __init__(self,
                 study_config,
                 input_key: str,
                 output_key: str,
                 file_extensions: list):
        """
        Args:
            study_config: Dictionary containing study settings.
            input_key: Key to look up source path in config (e.g., "NIRS", "Art").
            output_key: Key to look up destination path in config.
            file_extensions: List of extensions to process (e.g., ['*.tif', '*.snirf']).
        """
        self.config = study_config
        self.extensions = file_extensions
        self.input_key = input_key

        # Resolve Paths Safely
        try:
            self.source_dir = cfg.get_input_path(study_config, input_key)
            self.output_root = cfg.get_output_path(study_config, output_key)
        except ValueError as e:
            logger.error(f"Config Error for {input_key}: {e}")
            self.source_dir = None
            self.output_root = None

    # ABSTRACT METHODS (Must be implemented by children)

    @abstractmethod
    def _process_single_file(self, file_path: Path) -> bool:
        """
        Process a single file.
        Returns True if successful, False if failed.
        """
        pass

    # HOOKS (Optional override)

    def _pre_run_setup(self):
        """Hook to run before the file loop starts."""
        pass

    def _post_run_teardown(self):
        """Hook to run after the file loop ends."""
        pass

    # SHARED HELPERS

    def _validate_paths(self):
        if not self.source_dir:
            return False
        if not self.source_dir.exists():
            print(f"⚠️ Source folder not found: {self.source_dir}")
            return False
        self.output_root.mkdir(parents=True, exist_ok=True)
        return True

    def _gather_files(self):
        """Finds all files matching the extensions (case-insensitive)."""
        files = []
        # Adds files matching extension to file list
        for ext in self.extensions:
            files.extend(list(self.source_dir.glob(ext)))
            files.extend(list(self.source_dir.glob(ext.upper())))
        return sorted(list(set(files)))

    def _gather_folders(self):
        """
        Gathers directories matching the patterns in self.extensions.
        Useful when metadata is on the folder level (e.g. sub-01_ses-01).
        """
        folders = []
        for pattern in self.extensions:
            # glob returns files AND folders, so we must filter with .is_dir()
            matches = self.source_dir.glob(pattern)
            folders.extend([p for p in matches if p.is_dir()])

        return sorted(list(set(folders)))

    def log_success(self, source, dest):
        tqdm.write(f"✅ {source:<30} -> {dest}")
        logger.info(f"Converted: {source} -> {dest}")

    def log_error(self, source, error):
        msg = f"❌ Error processing {source}: {error}"
        tqdm.write(msg)
        logger.error(msg, exc_info=True)

    def _validate_metadata(self, info, keys, context):
        missing = [k for k in keys if not info.get(k)]
        if missing:
            raise ValueError(f"Missing {missing} for {context}")

    def run(self):
        """
        The Template Method: Defines the strict skeleton of the conversion process.
        """
        # Validation
        if not self._validate_paths():
            return

        # Hook: Pre-Run (Load metadata, etc.)
        self._pre_run_setup()

        # Gather Files
        files = self._gather_files()
        if not files:
            logger.info(f"ℹ️  [{self.input_key}] No files found in {self.source_dir}")
            return

        print(f"\n🚀 Starting {self.__class__.__name__} ({len(files)} files)")
        print(f"   Source: {self.source_dir}")

        success_count = 0
        fail_count = 0

        # Execution Loop
        for f in tqdm(files, desc=self.input_key, unit="file"):
            if self._process_single_file(f):
                success_count += 1
            else:
                fail_count += 1

        # Hook: Post-Run (Cleanup, TSV generation)
        self._post_run_teardown()

        # Final Report
        msg = f"✅ {self.input_key} Complete. Success: {success_count}, Failures: {fail_count}\n"
        print("-" * 50)
        print(msg)
        logger.info(msg)