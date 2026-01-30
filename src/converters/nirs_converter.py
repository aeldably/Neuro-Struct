import io
import warnings
import pandas as pd
from contextlib import redirect_stdout
import mne
from mne_bids import BIDSPath, write_raw_bids

# Internal Imports
from src.converters.base_converter import BaseConverter
from src import config as cfg
from src import loaders, utils


class NirsConverter(BaseConverter):
    """
    Converts raw SNIRF data to BIDS format.
    """

    def __init__(self, study_config):
        super().__init__(
            study_config,
            input_key="NIRS",
            output_key="NIRS",
            file_extensions=["*.snirf"]
        )
        self.task_name = study_config.get("TaskName", "drawing")
        self.dyad_lookup = {}
        self.demo_lookup = {}

    # HOOKS

    def _pre_run_setup(self):
        """Load Metadata before processing."""
        self.dyad_lookup = loaders.load_dyad_mapping(cfg.DYAD_FILE)
        self.demo_lookup = loaders.load_demographics(cfg.PARTICIPANTS_FILE)

    def _post_run_teardown(self):
        """Finalize BIDS dataset structure."""
        utils.generate_description(self.output_root, self.config)
        utils.patch_nirs_coords(self.output_root)
        self._batch_update_participants_tsv()

    # CORE LOGIC

    def _process_single_file(self, file_path):
        # Parse Filename
        sub_id, ses_id, run_id = utils.parse_filename(file_path.name)
        if not sub_id or not ses_id:
            self.log_error(file_path.name, "Invalid Filename Pattern")
            return False

        # Prepare Metadata
        dyad_num = self.dyad_lookup.get(str(int(sub_id)))
        acq_label = f"dyad{dyad_num}" if dyad_num else None

        # Look up demographics (key: "101_01")
        lookup_key = f"{int(sub_id)}_{ses_id}"
        subject_meta = self.demo_lookup.get(lookup_key, {})

        try:
            # MNE Processing (Silenced)
            f = io.StringIO()
            with redirect_stdout(f), warnings.catch_warnings(), mne.utils.use_log_level('error'):
                warnings.simplefilter("ignore")

                # Read Raw
                raw = mne.io.read_raw_snirf(file_path, preload=False, verbose=False)
                raw.set_meas_date(None)
                if raw.get_montage():
                    raw.set_montage(raw.get_montage())

                # Inject Sex
                if 'sex' in subject_meta:
                    info = raw.info.get('subject_info') or {}
                    info['sex'] = subject_meta['sex']
                    raw.info['subject_info'] = info

                # Write BIDS
                bids_path = BIDSPath(
                    subject=sub_id, session=ses_id, run=run_id,
                    task=self.task_name, acquisition=acq_label,
                    datatype="nirs", root=self.output_root
                )
                write_raw_bids(raw, bids_path, overwrite=True, verbose=False)

            self.log_success(file_path.name, f"sub-{sub_id}_ses-{ses_id}")
            return True

        except Exception as e:
            self.log_error(file_path.name, e)
            return False

    def _batch_update_participants_tsv(self):
        """Updates participants.tsv with Age data."""
        tsv_path = self.output_root / "participants.tsv"
        if not tsv_path.exists(): return

        df = pd.read_csv(tsv_path, sep='\t')

        # Ensure age column exists and allows mixed types
        if 'age' not in df.columns: df['age'] = 'n/a'
        df['age'] = df['age'].astype(object)

        for index, row in df.iterrows():
            sub_id = str(row['participant_id']).replace('sub-', '')

            # Find matching age
            for key, meta in self.demo_lookup.items():
                if key.split('_')[0] == sub_id and 'age' in meta:
                    df.at[index, 'age'] = meta['age']
                    break

        df.to_csv(tsv_path, sep='\t', index=False, na_rep='n/a')
        print("✅ Participants.tsv updated.")