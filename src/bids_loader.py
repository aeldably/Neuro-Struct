import mne
from mne_bids import BIDSPath, read_raw_bids, get_entity_vals
from pathlib import Path
from typing import Tuple, Generator, List, Optional, Dict


# ==============================================================================
# LEVEL 1: ALL DATA (The Iterator)
# ==============================================================================
def iter_dataset(bids_root: Path, task_name: str, datatype="nirs") -> Generator[
    Tuple[str, str, mne.io.Raw], None, None]:
    """
    Loops over the whole .SNIRF files, and yields every single file in the dataset, one by one.
    """
    subjects = sorted(get_entity_vals(bids_root, 'subject'))

    for sub in subjects:
        # Find sessions manually (robust method)
        sub_folder = bids_root / f"sub-{sub}"
        session_paths = sorted(sub_folder.glob("ses-*"))
        sessions = [p.name.split("-")[-1] for p in session_paths if p.is_dir()]

        for ses in sessions:
            # Construct a "Search Query"
            search_path = BIDSPath(
                subject=sub, session=ses, task=task_name,
                datatype=datatype, root=bids_root, suffix=datatype
            )

            # Find the actual file (handles 'acq-dyad1001' automatically)
            # match() returns a list of all files fitting the criteria
            matches = search_path.match()

            if not matches:
                continue

            # We take the first match -- one per session
            target_file = matches[0]

            try:
                raw = read_raw_bids(target_file, verbose=False)
                yield sub, ses, raw
            except Exception as e:
                print(f"❌ Error iterating sub-{sub} ses-{ses}: {e}")


# ==============================================================================
# LEVEL 2: INDIVIDUAL (Direct Access)
# ==============================================================================
def load_individual(bids_root: Path,
                    sub_id: str,
                    ses_id: str,
                    task_name: str) -> Optional[mne.io.Raw]:
    """
    Loads a specific subject/session immediately, handling dynamic acquisition tags.
    """
    # Create a generic search path
    search_path = BIDSPath(
        subject=sub_id,
        session=ses_id,
        task=task_name,
        datatype="nirs",
        root=bids_root,
        suffix="nirs"
    )

    # Find the specific file on disk (e.g. including 'acq-dyad1001')
    matches = search_path.match()

    if not matches:
        print(f"⚠️ File not found: sub-{sub_id} ses-{ses_id}")
        return None

    try:
        # Load the specific match found
        return read_raw_bids(matches[0], verbose=False)
    except Exception as e:
        print(f"❌ Failed to load individual sub-{sub_id}: {e}")
        return None


# ==============================================================================
# LEVEL 3: DYADIC (Paired Access)
# ==============================================================================
def load_dyad_pair(bids_root: Path,
                   dyad_id: str,
                   grouping_map: Dict[str, List[str]],
                   ses_id: str,
                   task_name: str) -> Optional[Tuple[mne.io.Raw, mne.io.Raw]]:
    """
    Loads two matched files for Hyperscanning analysis given a Dyad ID.

    Parameters:
    - dyad_id: The ID string (e.g., "1001")
    - grouping_map: The dictionary from loaders.load_dyad_grouping()
    """

    # Look up the subjects for this Dyad
    if dyad_id not in grouping_map:
        print(f"⚠️ Dyad ID '{dyad_id}' not found in the provided map.")
        return None

    subject_ids = grouping_map[dyad_id]

    # Validate Group Size
    if len(subject_ids) != 2:
        print(f"⚠️ Dyad error: Expected 2 subjects for Dyad {dyad_id}, got {len(subject_ids)}: {subject_ids}")
        return None

    sub_a, sub_b = subject_ids

    # Load both individuals (reusing Level 2 logic)
    raw_a = load_individual(bids_root, sub_a, ses_id, task_name)
    raw_b = load_individual(bids_root, sub_b, ses_id, task_name)

    # Success Check
    if raw_a is None or raw_b is None:
        print(f"⚠️ Incomplete Dyad pair {dyad_id} (sub-{sub_a}, sub-{sub_b})")
        return None

    return raw_a, raw_b


# ==============================================================================
# LEVEL 4: ALL DYADS (Batch Hyperscanning)
# ==============================================================================
def iter_dyads(bids_root: Path,
               grouping_map: Dict[str, List[str]],
               task_name: str) -> Generator[Tuple[str, str, mne.io.Raw, mne.io.Raw], None, None]:
    """
    Yields matched pairs for every Dyad in the dataset.

    Usage:
        for dyad_id, ses, raw_a, raw_b in iter_dyads(root, dyad_map, 'drawing'):
            corr = calculate_synchrony(raw_a, raw_b)
    """
    # Iterate through every Dyad in the map
    for dyad_id, subjects in grouping_map.items():

        # Find which sessions exist for the *first* subject (assuming partners match)
        # We look inside the folder of Subject A to discover sessions
        sub_a = subjects[0]
        sub_folder = bids_root / f"sub-{sub_a}"
        if not sub_folder.exists():
            continue

        session_paths = sorted(sub_folder.glob("ses-*"))
        sessions = [p.name.split("-")[-1] for p in session_paths if p.is_dir()]

        # 3. For each session, try to load the PAIR
        for ses in sessions:
            pair_data = load_dyad_pair(
                bids_root,
                dyad_id=dyad_id,
                grouping_map=grouping_map,
                ses_id=ses,
                task_name=task_name
            )

            # Only yield if we successfully got a valid pair
            if pair_data:
                raw_a, raw_b = pair_data
                yield dyad_id, ses, raw_a, raw_b