import logging
from docx import Document
from pathlib import Path
from typing import Dict

from src.converters.base_converter import BaseConverter
from src import config as cfg

logger = logging.getLogger(__name__)


class DocxToTxtConverter(BaseConverter):
    """
    Converts .docx interview files to .txt format.
    Reads from outputs/Qual/exit_interviews and writes to
    outputs/Qual/exit_interviews_txt.

    - Preserves UTF-8 encoding for German and English text
    - Extracts plain text only (no tables, images, or formatting)
    - Skips empty paragraphs to keep output clean
    - Idempotent: overwrites existing .txt files on re-run
    """

    def __init__(self, study_config: Dict):
        super().__init__(
            study_config=study_config,
            input_key="QualTxt",
            output_key="QualTxt",
            file_extensions=["*.docx"]
        )
        # Source is in outputs/, not inputs/ — override source_dir
        self.source_dir = cfg.get_output_path(study_config, "QualTxt")

    # Core

    def _process_single_file(self, file_path: Path) -> bool:
        try:
            # Extract text from docx
            doc        = Document(file_path)
            paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]

            if not paragraphs:
                logger.warning(f"⚠️  No text content found in {file_path.name} — skipping")
                return False

            # Write .txt with same stem, UTF-8 for German/English
            output_path = self.output_root / file_path.with_suffix(".txt").name
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w", encoding="utf-8") as f:
                f.write("\n".join(paragraphs))

            self.log_success(file_path.name, output_path.name)
            return True

        except Exception as e:
            self.log_error(file_path.name, e)
            return False