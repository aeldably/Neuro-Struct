import pandas as pd
from pathlib import Path
from typing import Dict


def load_demographics(csv_path: Path) -> Dict[str, dict]:
    """
    Reads the participants CSV and builds a lookup dictionary for Age and Sex.

    It constructs a unique composite key 'SubjectID_SessionID' (e.g., '101_01') to
    handle multi-visit studies correctly. It also normalizes gender strings
    ('male', 'F') into the specific integer codes required by MNE-BIDS.
    """
    if not csv_path.exists():
        # Fail silently with empty dict to allow processing without demographic data
        return {}

    try:
        # Read as string to preserve leading zeros in IDs if present
        df = pd.read_csv(csv_path, dtype=str)
        lookup = {}

        # MNE internal codes: 1=Male, 2=Female, 0=Unknown
        sex_map = {'male': 1, 'female': 2, 'm': 1, 'f': 2}

        for _, row in df.iterrows():
            # Normalize ID formats (strip zeros, ensure padding)
            p_id = str(int(row['p_id']))  # "101"
            visit = f"{int(row['visit']):02d}"  # "01"
            key = f"{p_id}_{visit}"  # "101_01"

            # Map gender string to integer code
            sex_code = sex_map.get(str(row['gender']).lower(), 0)

            lookup[key] = {'age': row['age'], 'sex': sex_code}

        return lookup

    except Exception as e:
        print(f"❌ Error reading Demographics CSV: {e}")
        return {}


def load_dyad_mapping(csv_path: Path) -> Dict[str, str]:
    """
    Loads a dyad mapping from a CSV file into a dictionary.

    This function reads a CSV file containing dyad and participant identifiers,
    normalizes the participant IDs by removing leading zeros, and maps each
    participant ID to its corresponding dyad ID. It ensures data is converted
    from a wide format to a long format before creating the mapping. If any
    errors occur during the process, an empty dictionary is returned.

    Parameters:
    csv_path : Path
        Path to the CSV file containing the dyad mapping. The file should have
        at least three columns: 'dyadID', 'pID1', and 'pID2'.

    Returns:
    Dict[str, str]
        A dictionary mapping participant IDs ('pID1' or 'pID2') to their
        associated dyad ID ('dyadID'). If the CSV file doesn't exist or an
        error occurs, an empty dictionary is returned.
    """
    if not csv_path.exists(): return {}

    try:
        df = pd.read_csv(csv_path, dtype=str)

        # Transform: [Dyad, P1, P2] -> [[Dyad, P1], [Dyad, P2]]
        long_df = df.melt(id_vars=['dyadID'], value_vars=['pID1', 'pID2'], value_name='sub_id')

        # Normalize IDs to match filenames (remove potential leading zeros from CSV)
        long_df['sub_id'] = long_df['sub_id'].apply(lambda x: str(int(x)) if pd.notna(x) else x)

        return long_df.set_index('sub_id')['dyadID'].to_dict()

    except Exception as e:
        print(f"❌ Error reading Dyad CSV: {e}")
        return {}

def load_dyad_grouping(csv_path: Path) -> Dict[str, list]:
    """
    Loads and processes dyadic grouping information from a CSV file.

    This function reads a CSV file containing dyadic IDs and participant IDs, processes the
    data to normalize participant IDs, and organizes it into a dictionary. The resulting
    dictionary maps each dyadic ID (dyadID) to a list of participant IDs.

    Parameters:
        csv_path (Path): The path to the CSV file containing the dyadic grouping data.

    Returns:
        Dict[str, list]: A dictionary where each key is a dyadID, and its value is a list of
        normalized participant IDs associated with that dyad. If the file doesn't exist or an
        error occurs during processing, an empty dictionary is returned.
    """
    if not csv_path.exists(): return {}

    try:
        df = pd.read_csv(csv_path, dtype=str)
        grouping = {}

        for _, row in df.iterrows():
            dyad_id = str(row['dyadID'])
            # Extract both participants, remove potential NaNs, normalize IDs
            subs = [row['pID1'], row['pID2']]
            clean_subs = [str(int(s)) for s in subs if pd.notna(s)]

            grouping[dyad_id] = clean_subs

        return grouping
    except Exception as e:
        print(f"❌ Error reading Dyad CSV: {e}")
        return {}