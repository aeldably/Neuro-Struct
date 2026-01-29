import warnings
import io
from contextlib import redirect_stdout
import pandas as pd
import mne
from mne_bids import BIDSPath, write_raw_bids
from tqdm import tqdm

# Internal imports
from src import config as cfg
from src import loaders, utils


class NirsConverter:
    """
    NirsConverter facilitates the conversion of NIRS (Near-Infrared Spectroscopy) data into the Brain Imaging Data
    Structure (BIDS) format.

    This class manages the entire pipeline for processing raw NIRS files, handling metadata integration, and
    finalizing BIDS-compliant directories. It is designed to work with study configurations, ensuring compatibility
    with project-specific requirements.

    Attributes:
        config: Dict-like object containing study-specific configurations.
        task_name: The name of the task associated with the NIRS recordings. Defaults to 'drawing'.
        source_dir: Source directory containing NIRS `.snirf` files, resolved dynamically.
        bids_root: Destination directory for the BIDS-compatible data, resolved dynamically.
        dyad_lookup: Dictionary acting as a lookup table for dyad mapping.
        demo_lookup: Dictionary containing demographic metadata loaded from auxiliary CSV files.
    """

    def __init__(self, study_config):
        self.config = study_config
        self.task_name = study_config.get("TaskName", "drawing")

        # Resolve Paths immediately upon instantiation
        try:
            self.source_dir = cfg.get_input_path(study_config, "NIRS")
            self.bids_root = cfg.get_output_path(study_config, "NIRS")
        except ValueError as e:
            print(f"❌ NIRS Config Error: {e}")
            self.source_dir = None
            self.bids_root = None

        # Placeholders for metadata
        self.dyad_lookup = {}
        self.demo_lookup = {}

    def _validate_paths(self):
        """
        Validates the existence and setup of required paths.

        This method ensures the validity of the `source_dir` attribute and its
        existence in the filesystem. If `source_dir` is invalid or does not exist,
        the method logs an appropriate message and returns `False`. Additionally, it
        makes sure that the `bids_root` directory is created if it doesn't already
        exist.

        Returns:
            bool: Returns `True` if `source_dir` is valid and exists, otherwise
            returns `False`."""
        if not self.source_dir:
            return False

        if not self.source_dir.exists():
            print(f"❌ Skipping NIRS: Source folder '{self.source_dir.name}' not found.")
            return False

        self.bids_root.mkdir(parents=True, exist_ok=True)
        return True

    def _load_metadata(self):
        """Loads auxiliary CSVs into memory."""
        self.dyad_lookup = loaders.load_dyad_mapping(cfg.DYAD_FILE)
        self.demo_lookup = loaders.load_demographics(cfg.PARTICIPANTS_FILE)

    def _convert_single_file(self, file_path):
        """
        Converts and processes a single file in SNIRF format to BIDS-compliant format with metadata injection
        and proper error handling.

        The method handles metadata extraction, error suppression, and writing of the raw data in
        BIDS-compatible format. It uses utility methods and configured lookups to handle subject, session,
        and run IDs. Metadata such as sex can be injected into the dataset, and various warnings
        are silenced during the process.

        Parameters:
        file_path : Path
            Path to the SNIRF file that needs to be converted and processed.

        Returns:
        bool
            True if the file was successfully converted and processed; False otherwise.
        """
        # Parse Filename
        sub_id, ses_id, run_id = utils.parse_filename(file_path.name)
        if not sub_id or not ses_id:
            tqdm.write(f"⚠️ Skipping {file_path.name} (Invalid ID)")
            return False

        # Prepare Metadata
        dyad_num = self.dyad_lookup.get(str(int(sub_id)))
        acq_label = f"dyad{dyad_num}" if dyad_num else None

        lookup_key = f"{int(sub_id)}_{ses_id}"
        subject_meta = self.demo_lookup.get(lookup_key, {})

        try:
            # Silence MNE and Write BIDS
            f = io.StringIO()
            with redirect_stdout(f), warnings.catch_warnings(), mne.utils.use_log_level('error'):
                warnings.simplefilter("ignore")

                # Read
                raw = mne.io.read_raw_snirf(file_path, preload=False, verbose=False)
                raw.set_meas_date(None)
                if raw.get_montage(): raw.set_montage(raw.get_montage())

                # Inject Sex (In-Memory)
                if 'sex' in subject_meta:
                    info = raw.info.get('subject_info') or {}
                    info['sex'] = subject_meta['sex']
                    raw.info['subject_info'] = info

                # Write
                bids_path = BIDSPath(
                    subject=sub_id, session=ses_id, run=run_id,
                    task=self.task_name, acquisition=acq_label,
                    datatype="nirs", root=self.bids_root
                )
                write_raw_bids(raw, bids_path, overwrite=True, verbose=False)

            tqdm.write(f"✅ sub-{sub_id} (ses-{ses_id})")
            return True

        except Exception as e:
            tqdm.write(f"❌ sub-{sub_id} failed: {e}")
            return False

    def _post_process_participants_tsv(self):
        """
        Batch updates the participants.tsv with Age data after all files are written.
        """
        tsv_path = self.bids_root / "participants.tsv"
        if not tsv_path.exists():
            return

        # Load Data
        df = pd.read_csv(tsv_path, sep='\t')

        # Fix Column Types (The Solution)
        # Ensure 'age' exists and is treated as generic text/objects, not strict floats.
        if 'age' not in df.columns:
            df['age'] = 'n/a'

        # This line prevents the FutureWarning by allowing mixed types (numbers & strings)
        df['age'] = df['age'].astype(object)

        # Update Loop
        for index, row in df.iterrows():
            # Clean the ID (e.g., 'sub-101' -> '101')
            sub_id = str(row['participant_id']).replace('sub-', '')

            # Find matching age in demo_lookup
            for key, meta in self.demo_lookup.items():
                # Check if key (e.g. "101_1") starts with subject ID
                if key.split('_')[0] == sub_id and 'age' in meta:
                    df.at[index, 'age'] = meta['age']
                    break

        # Save
        df.to_csv(tsv_path, sep='\t', index=False, na_rep='n/a')
        print("✅ Participants.tsv updated with Age data.")

    def run(self):
        """
        The main driver method. Call this to execute the pipeline.
        """
        # Validation
        if not self._validate_paths():
            return

        # Setup
        self._load_metadata()
        snirf_files = sorted(list(self.source_dir.glob("*.snirf")))

        if not snirf_files:
            print("ℹ️  No NIRS files to process.")
            return

        print(f"🚀 Starting NIRS Conversion: {len(snirf_files)} files -> {self.bids_root.name}\n")

        # Execution Loop
        for file_path in tqdm(snirf_files, desc="Converting NIRS", unit="file"):
            self._convert_single_file(file_path)

        # Finalization
        print(f"\n{'-' * 40}")
        utils.generate_description(self.bids_root, self.config)
        utils.patch_nirs_coords(self.bids_root)
        self._post_process_participants_tsv()
        print("✅ NIRS Pipeline Complete.\n")