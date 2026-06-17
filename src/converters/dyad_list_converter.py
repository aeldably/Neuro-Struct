import json
import logging
import pandas as pd
from pathlib import Path
from typing import Dict

from src.converters.base_converter import BaseConverter

logger = logging.getLogger(__name__)


class DyadListConverter(BaseConverter):
    """
    Converts dyadlist.csv into two BIDS-inspired JSON files:
      - dyadlist.json       : pure data, keyed by dyadID
      - dyadlist.meta.json  : sidecar with variable descriptions and types

    Follows the BIDS philosophy of separating data from its documentation.
    Fails explicitly if a dyadID falls outside known ranges.
    """

    # Sidecar metadata — based on study codebook
    METADATA = {
        "dyadID": {
            "Description": (
                "ID number for each dyad. Same generation dyads are in range "
                "1001-1030 and intergenerational dyads are in range 2001-2031."
            ),
            "Values": "1001-1030, 2001-2031",
            "Type": "categorical"
        },
        "pID1": {
            "Description": (
                "Participant ID of one dyad member. Younger adults were assigned "
                "IDs in ranges 101-146 or 201-245 and older adults were assigned "
                "IDs in range 301-331."
            ),
            "Values": "101-146, 201-245, 301-331",
            "Type": "categorical"
        },
        "pID2": {
            "Description": (
                "Participant ID of second dyad member. Younger adults were assigned "
                "IDs in ranges 101-146 or 201-245 and older adults were assigned "
                "IDs in range 301-331."
            ),
            "Values": "101-146, 201-245, 301-331",
            "Type": "categorical"
        }
    }

    def __init__(self, study_config: Dict):
        super().__init__(
            study_config=study_config,
            input_key="Dyad",
            output_key="Dyad",
            file_extensions=["*.csv"]
        )

    # Helpers

    def _resolve_generation_type(self, dyad_id: int) -> str:
        """
        Derives generation type from dyadID range.
        Raises ValueError if dyadID is outside known ranges.
        """
        if 1001 <= dyad_id <= 1030:
            return "same-generation"
        elif 2001 <= dyad_id <= 2031:
            return "intergenerational"
        else:
            raise ValueError(
                f"dyadID {dyad_id} is outside known ranges (1001-1030, 2001-2031)"
            )

    # Core

    def _process_single_file(self, file_path: Path) -> bool:
        try:
            df = pd.read_csv(file_path, dtype=str)

            # Validate expected columns
            expected = {"dyadID", "pID1", "pID2"}
            missing  = expected - set(df.columns)
            if missing:
                self.log_error(file_path.name, f"Missing columns: {missing}")
                return False

            # Build data block — keyed by dyadID
            data = {}
            for _, row in df.iterrows():
                data[row["dyadID"]] = {
                    "pID1":            row["pID1"],
                    "pID2":            row["pID2"],
                }


            # Write dyadlist.json metadata file
            meta_path = self.output_root / "dyadlist.json"
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(self.METADATA, f, indent=4)
            self.log_success(file_path.name, "dyadlist.json")

            logger.info(f"   📋 {len(data)} dyads written")
            return True

        except Exception as e:
            self.log_error(file_path.name, e)
            return False