import re
from typing import Optional, Tuple, Any, Dict

from src import loaders
from src import config as cfg


class FilenameParser:
    """
    Centralized parser for extracting metadata from filenames.
    Separates Standard BIDS parsing (NIRS) from Artwork-specific parsing.
    """

    def __init__(self, dyad_map: Optional[Dict[str, str]] = None):
        """
        Args:
            dyad_map: Optional dictionary mapping Subject ID -> Dyad ID.
                      If None, loads automatically from config.
        """
        if dyad_map is None:
            # Load automatically if not provided
            self.dyad_lookup = loaders.load_dyad_mapping(cfg.DYAD_FILE)
        else:
            # Dependency Injection
            self.dyad_lookup = dyad_map

    # --- Helper Methods ---

    def _get_dyad_for_sub(self, sub_id_str: str) -> Optional[str]:
        """
        Safely looks up Dyad ID for a given Subject ID string.
        Handles padding differences (e.g., '01' vs '1') by normalizing to int-string.
        """
        try:
            # Normalize "01" -> "1" to match JSON keys
            lookup_key = str(int(sub_id_str))
            return self.dyad_lookup.get(lookup_key)
        except ValueError:
            return None

    # --- Standard BIDS Parsing (Subject, Session, Run) ---

    def parse_subject(self, filename: str) -> Optional[str]:
        """Extracts the Subject ID (e.g., 'sub-01' -> '01')."""
        match = re.search(r"sub-?(\d+)", filename, re.IGNORECASE)
        if match:
            return match.group(1).zfill(2)
        return None

    def parse_session(self, filename: str) -> Optional[str]:
        """Extracts the Session ID (e.g., 'ses-01', 'session-1')."""
        # 1. Special Case: session-a_01 -> ses-01
        special_match = re.search(r"session-[a-z]_(\d+)", filename, re.IGNORECASE)
        if special_match:
            return special_match.group(1).zfill(2)

        # 2. Standard Case: ses-01 or session-1 -> ses-01
        standard_match = re.search(r"(?:ses|session)-?(\d+)", filename, re.IGNORECASE)
        if standard_match:
            return standard_match.group(1).zfill(2)
        return None

    def parse_run(self, filename: str) -> str:
        """Extracts Run ID, defaulting to '01' if not found."""
        # Explicit run-XX
        run_match = re.search(r"run-?(\d+)", filename, re.IGNORECASE)
        if run_match:
            return f"{int(run_match.group(1)):02d}"

        # Implicit mapping: session-a -> run 01, session-b -> run 02
        special_match = re.search(r"session-([a-z])", filename, re.IGNORECASE)
        if special_match:
            char = special_match.group(1).lower()
            run_num = ord(char) - 96  # 'a' is 97, so 97-96=1
            return f"{run_num:02d}"

        return "01"

    def parse_common_components(self, filename: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Master method for Standard BIDS files (e.g. NIRS).
        Returns: (sub, ses, run)
        """
        sub = self.parse_subject(filename)
        ses = self.parse_session(filename)

        if not sub or not ses:
            return None, None, None

        run = self.parse_run(filename)
        return sub, ses, run

    # --- Specialized Parsing (Artworks & Coordinates) ---

    def parse_artwork_filename(self, filename: str) -> Optional[Dict[str, Any]]:
        """
        Parses specific naming conventions for Artwork (Dyad vs Solo).
        Returns: Dict with keys ['sub', 'dyad', 'ses', 'task', 'task_num'] or None.
        """
        # Strategy 1: DYAD CASE (dyad-1001-D1_session-1.tif)
        dyad_match = re.search(r"dyad-(\d+)-D(\d+)_session-(\d+)", filename, re.IGNORECASE)
        if dyad_match:
            return {
                "sub": None,
                "dyad": dyad_match.group(1),
                "ses": dyad_match.group(3).zfill(2),
                "task": "together",
                "task_num": dyad_match.group(2)
            }

        # Strategy 2: SOLO CASE (sub-102-solo_session-1.tif)
        solo_match = re.search(r"sub-(\d+)-solo_session-(\d+)", filename, re.IGNORECASE)
        if solo_match:
            sub_str = solo_match.group(1)
            dyad_id = self._get_dyad_for_sub(sub_str)  # Use helper

            return {
                "sub": sub_str.zfill(2),
                "dyad": dyad_id,
                "ses": solo_match.group(2).zfill(2),
                "task": "solo",
                "task_num": None
            }

        return None

    def parse_coordinates_folder(self, folder_name: str) -> Optional[Dict[str, Any]]:
        """
        Parses the coordinate folder name to extract subject and session.
        Format: sub-101_session-1
        """
        match = re.search(r"^sub-(\d+)_session-(\d+)$", folder_name, re.IGNORECASE)

        if match:
            sub_str = match.group(1)
            dyad_id = self._get_dyad_for_sub(sub_str)  # Use helper

            return {
                "sub": sub_str.zfill(2),
                "ses": match.group(2).zfill(2),
                "dyad": dyad_id
            }

        return None