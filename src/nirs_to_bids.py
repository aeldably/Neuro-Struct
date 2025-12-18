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

def run_nirs_conversion(study_config):
    """
    Encapsulates the entire NIRS-to-BIDS workflow.
    1. Resolves paths for 'NIRS' modality.
    2. Loads specific metadata (Dyads, Demographics).
    3. Iterates and converts files.
    4. Runs post-processing (Sidecars, Coordinates).
    """

    # 1. SETUP PATHS
    try:
        source_dir = cfg.get_input_path(study_config, "NIRS")
        bids_root = cfg.get_output_path(study_config, "NIRS")
    except ValueError as e:
        print(f"❌ NIRS Config Error: {e}")
        return

    if not source_dir.exists():
        print(f"❌ Skipping NIRS: Source folder '{source_dir.name}' not found.")
        return

    bids_root.mkdir(parents=True, exist_ok=True)
    task_name = study_config.get("TaskName", "drawing")

    # LOAD METADATA
    dyad_lookup = loaders.load_dyad_mapping(cfg.DYAD_FILE)
    demo_lookup = loaders.load_demographics(cfg.PARTICIPANTS_FILE)
    snirf_files = sorted(list(source_dir.glob("*.snirf")))

    if not snirf_files:
        print("ℹ️  No NIRS files to process.")
        return

    print(f"🚀 Starting NIRS Conversion: {len(snirf_files)} files -> {bids_root.name}\n")

    # 3. PROCESSING LOOP
    for file_path in tqdm(snirf_files, desc="Converting NIRS", unit="file"):

        # Parse Filename
        sub_id, ses_id, run_id = utils.parse_filename(file_path.name)
        if not sub_id or not ses_id:
            tqdm.write(f"⚠️ Skipping {file_path.name} (Invalid ID)")
            continue

        # Prepare Metadata
        dyad_num = dyad_lookup.get(str(int(sub_id)))
        acq_label = f"dyad{dyad_num}" if dyad_num else None

        lookup_key = f"{int(sub_id)}_{ses_id}"
        subject_meta = demo_lookup.get(lookup_key, {})

        try:
            # Capture MNE Logs
            f = io.StringIO()
            with redirect_stdout(f), warnings.catch_warnings(), mne.utils.use_log_level('error'):
                warnings.simplefilter("ignore")

                # Read Data
                raw = mne.io.read_raw_snirf(file_path, preload=False, verbose=False)
                raw.set_meas_date(None)
                if raw.get_montage(): raw.set_montage(raw.get_montage())

                # Inject Sex
                if 'sex' in subject_meta:
                    info = raw.info.get('subject_info') or {}
                    info['sex'] = subject_meta['sex']
                    raw.info['subject_info'] = info

                # Write BIDS
                bids_path = BIDSPath(
                    subject=sub_id, session=ses_id, run=run_id,
                    task=task_name, acquisition=acq_label,
                    datatype="nirs", root=bids_root
                )
                write_raw_bids(raw, bids_path, overwrite=True, verbose=False)

                # Patch Age (Participants.tsv)
                if 'age' in subject_meta:
                    tsv_path = bids_root / "participants.tsv"
                    if tsv_path.exists():
                        df = pd.read_csv(tsv_path, sep='\t')
                        mask = df['participant_id'] == f"sub-{sub_id}"
                        if mask.any():
                            df.loc[mask, 'age'] = subject_meta['age']
                            df.to_csv(tsv_path, sep='\t', index=False, na_rep='n/a')

            tqdm.write(f"✅ sub-{sub_id} (ses-{ses_id})")

        except Exception as e:
            tqdm.write(f"❌ sub-{sub_id} failed: {e}")

    # POST-PROCESSING
    print(f"\n{'-' * 40}")
    utils.generate_description(bids_root, study_config)
    utils.patch_nirs_coords(bids_root)
    print("✅ NIRS Pipeline Complete.\n")