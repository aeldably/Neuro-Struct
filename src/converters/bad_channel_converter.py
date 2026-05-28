import re
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Tuple

from src.converters.base_converter import BaseConverter
from src import config as cfg

logger = logging.getLogger(__name__)


class BadChannelConverter(BaseConverter):
    """
    Post-processor that patches channels.tsv files in place inside
    outputs/BrainImaging, marking channels flagged during visual inspection
    as 'bad' based on an exclusion list CSV.

    No files are copied — patching happens directly on the output files.
    Patching is idempotent: channels already marked 'bad' are skipped.
    """

    SESSION_MAP = {
        "1": "01",
        "2": "02",
        "3": "03",
        "4": "04",
        "5": "05",
        "6": "06",
    }

    RECORDING_MAP = {
        "a": "01",
        "b": "02",
    }

    BAD_STATUS      = "bad"
    BAD_DESCRIPTION = "Visual inspection: major motion artifacts and/or no cardiac oscillation"

    def __init__(self, study_config: Dict):
        super().__init__(
            study_config=study_config,
            input_key="NIRS",
            output_key="NIRS",
            file_extensions=["**/*_channels.tsv"]
        )
        # Override: patch in place inside the output directory
        self.source_dir = self.output_root
        self.exclusions: Dict[Tuple, set] = {}
        self.exclusions_path = cfg.PROJ_ROOT / "excludedChannels_visualInspection.csv"

    #  Overrides 

    def _gather_files(self):
        """Recursively gather all channels.tsv files from the output directory."""
        return sorted(self.source_dir.glob("**/*_channels.tsv"))

    def _pre_run_setup(self):
        """Load and index the exclusion list by (sub, ses, run) before processing."""
        if not self.exclusions_path.exists():
            raise FileNotFoundError(f"Exclusion CSV not found: {self.exclusions_path}")

        df = pd.read_csv(self.exclusions_path, dtype=str)

        for _, row in df.iterrows():
            sub     = str(row["ID"]).strip().zfill(3)
            ses_raw = str(row["session"]).strip()
            rec_raw = str(row["recording"]).strip().lower()
            channel = str(row["channel"]).strip()

            ses = self.SESSION_MAP.get(ses_raw)
            if not ses:
                logger.warning(f"⚠️  Unknown session '{ses_raw}' in exclusion CSV — skipping row (ID={sub})")
                continue

            run = self.RECORDING_MAP.get(rec_raw)
            if not run:
                logger.warning(f"⚠️  Unknown recording '{rec_raw}' in exclusion CSV — skipping row (ID={sub}, ses={ses})")
                continue

            key = (sub, ses, run)
            self.exclusions.setdefault(key, set()).add(channel)

        logger.info(f"   📋 Loaded exclusions for {len(self.exclusions)} subject/session/run combinations")

    #  Helpers 

    def _parse_channels_filename(self, filename: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Extracts (sub, ses, run) from a channels.tsv filename.
        e.g. sub-101_ses-02_task-drawing_acq-dyad1001_run-01_channels.tsv
        """
        sub = re.search(r"sub-(\d+)", filename)
        ses = re.search(r"ses-(\d+)", filename)
        run = re.search(r"run-(\d+)", filename)

        if not sub or not ses or not run:
            return None, None, None

        return sub.group(1), ses.group(1), run.group(1)

    def _patch_channels(self, file_path: Path, bad_channels: set) -> Tuple[int, int]:
        """
        Patches status and status_description for bad channels in a TSV file.
        Matches on channel name prefix (e.g. S1_D2 matches S1_D2 760 and S1_D2 850).
        Skips channels already marked 'bad' (idempotent).

        Returns (patched_count, skipped_count).
        """
        df = pd.read_csv(file_path, sep="\t")

        patched_count = 0
        skipped_count = 0

        for idx, row in df.iterrows():
            channel_name = str(row["name"])
            for bad_ch in bad_channels:
                if channel_name.startswith(bad_ch):
                    if str(row["status"]).strip().lower() == self.BAD_STATUS:
                        skipped_count += 1
                    else:
                        df.at[idx, "status"]             = self.BAD_STATUS
                        df.at[idx, "status_description"] = self.BAD_DESCRIPTION
                        patched_count += 1

        df.to_csv(file_path, sep="\t", index=False)
        return patched_count, skipped_count

    #  Core 

    def _process_single_file(self, file_path: Path) -> bool:
        # Parse filename
        sub, ses, run = self._parse_channels_filename(file_path.name)
        if not sub or not ses or not run:
            self.log_error(file_path.name, "Could not parse filename")
            return False

        try:
            key          = (sub, ses, run)
            bad_channels = self.exclusions.get(key, set())

            if bad_channels:
                patched, skipped = self._patch_channels(file_path, bad_channels)

                if skipped and not patched:
                    self.log_success(file_path.name, f"already patched ({skipped} channels, no changes made)")
                elif skipped:
                    self.log_success(file_path.name, f"{patched} channels patched, {skipped} already bad")
                else:
                    self.log_success(file_path.name, f"{patched} channels marked bad")
            else:
                self.log_success(file_path.name, "no bad channels flagged")

            return True

        except Exception as e:
            self.log_error(file_path.name, e)
            return False