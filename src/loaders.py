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
    Reads the dyad list and creates a flat mapping of Subject ID -> Dyad ID.

    The input CSV usually has wide format (DyadID, PID1, PID2). This function
    melts it into a long format so we can instantly look up which dyad a
    specific subject belongs to (e.g., '101' -> '1001').
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
    Returns a dictionary mapping Dyad ID to a list of Subject IDs.
    Example: {'1001': ['101', '102'], '1002': ['103', '104']}
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