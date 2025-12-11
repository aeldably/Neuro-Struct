import warnings
import pandas as pd
import mne
import io
from contextlib import redirect_stdout
from tqdm import tqdm
from mne_bids import BIDSPath, write_raw_bids

# Import custom modules from 'src'
from src import config as cfg
from src import loaders, utils


def main():
    """
    Main driver script for converting SNIRF files to BIDS format.

    Workflow:
    1. Load configuration and resolve paths relative to project root.
    2. Load participant metadata (Age/Sex) and dyad mappings into memory.
    3. Iterate through all SNIRF files (with Progress Bar):
       - Parse filename for BIDS entities (sub, ses, run).
       - Inject 'Sex' metadata into the Raw object (in-memory).
       - Write BIDS data using MNE-BIDS (silencing internal logs).
       - Patch 'Age' metadata into the participants.tsv (on-disk).
       - Log Success/Failure explicitly for every file.
    4. Generate final dataset description and apply sidecar hot-fixes.
    """

    try:
        study_config = cfg.load_study_config()
    except Exception as e:
        print(f"❌ {e}")
        return

    # Resolve input/output directories relative to the project root.
    paths_config = study_config.get("Paths", {})
    source_dir = cfg.resolve_path(paths_config.get("SourceDir", "dataset"))
    bids_root = cfg.resolve_path(paths_config.get("BidsRoot", "bids_dataset"))

    if not source_dir.exists():
        print(f"❌ Critical: Source folder '{source_dir}' does not exist.")
        return

    # Loading configuration
    task_name = study_config.get("TaskName", "drawing")

    # Pre-load CSVs into efficient lookup dictionaries
    dyad_lookup = loaders.load_dyad_mapping(cfg.DYAD_FILE)
    demo_lookup = loaders.load_demographics(cfg.PARTICIPANTS_FILE)

    # Find all source files
    snirf_files = sorted(list(source_dir.glob("*.snirf")))

    print(f"\n{'=' * 40}")
    print(f"🚀 BIDS CONVERSION STARTING")
    print(f"{'=' * 40}")
    print(f"📂 Source:    {source_dir.relative_to(cfg.PROJ_ROOT)}")
    print(f"📂 Output:    {bids_root.relative_to(cfg.PROJ_ROOT)}")
    print(f"🎯 Task:      {task_name}")
    print(f"👥 Resources: {len(dyad_lookup)} dyads, {len(demo_lookup)} participants")
    print(f"📄 Files:     {len(snirf_files)} found")
    print(f"{'-' * 40}\n")

    if not snirf_files:
        print("ℹ️  No files to process. Exiting.")
        return

    # PROCESSING LOOP
    # We use tqdm here to show a progress bar
    for file_path in tqdm(snirf_files, desc="Converting", unit="file"):

        # Extract BIDS entities (Subject, Session, Run)
        sub_id, ses_id, run_id = utils.parse_filename(file_path.name)

        if not sub_id: continue
        if not ses_id:
            # Prints warning above the bar
            tqdm.write(f"⚠️ Skipping {file_path.name} (Missing 'session')")
            continue

        # Metadata Lookups
        dyad_num = dyad_lookup.get(str(int(sub_id)))
        acq_label = f"dyad{dyad_num}" if dyad_num else None

        lookup_key = f"{int(sub_id)}_{ses_id}"
        subject_meta = demo_lookup.get(lookup_key, {})

        try:
            # Capture MNE's stdout to kill "Writing..." logs so they don't clutter our custom logs
            f = io.StringIO()
            with redirect_stdout(f), warnings.catch_warnings(), mne.utils.use_log_level('error'):
                warnings.simplefilter("ignore")

                # Read Raw Data
                raw = mne.io.read_raw_snirf(file_path, preload=False, verbose=False)
                raw.set_meas_date(None)
                if raw.get_montage(): raw.set_montage(raw.get_montage())

                # Inject 'Sex' (In-Memory)
                if 'sex' in subject_meta:
                    info = raw.info.get('subject_info') or {}
                    info['sex'] = subject_meta['sex']
                    raw.info['subject_info'] = info

                # Write BIDS Data
                bids_path = BIDSPath(
                    subject=sub_id, session=ses_id, run=run_id,
                    task=task_name, acquisition=acq_label,
                    datatype="nirs", root=bids_root
                )
                write_raw_bids(raw, bids_path, overwrite=True, verbose=False)

                # Inject 'Age' (On-Disk Patch)
                if 'age' in subject_meta:
                    tsv_path = bids_root / "participants.tsv"
                    if tsv_path.exists():
                        df = pd.read_csv(tsv_path, sep='\t')
                        mask = df['participant_id'] == f"sub-{sub_id}"
                        if mask.any():
                            df.loc[mask, 'age'] = subject_meta['age']
                            df.to_csv(tsv_path, sep='\t', index=False, na_rep='n/a')

            # Explicit Success Message (Prints above the progress bar)
            tqdm.write(f"✅ sub-{sub_id} (ses-{ses_id})")

        except Exception as e:
            # Explicit Failure Message
            tqdm.write(f"❌ sub-{sub_id} failed: {e}")

    print(f"\n{'-' * 40}")

    # Create dataset_description.json
    utils.generate_description(bids_root, study_config)

    # Fix missing coordinate descriptions
    utils.patch_nirs_coords(bids_root)

    print("✅ Conversion Complete.")


if __name__ == "__main__":
    main()