import logging
import sys
from src import config as cfg
from src import jobs

import logging
import sys
import time
from pathlib import Path


def setup_logging(log_dir="logs"):  # Now takes a directory instead of a file
    """
    Configures logging to write to both console and a unique, timestamped file.
    """
    # Force Windows console to understand UTF-8 (emojis)
    if sys.stdout.encoding.lower() != 'utf-8':
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    # 1. Create a 'logs' folder if it doesn't exist
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    # 2. Generate a unique filename based on the current time
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file = Path(log_dir) / f"pipeline_{timestamp}.log"

    # 3. Create the file handler (using 'w' is safe now, because the file is brand new!)
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_format = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    return logger


def main():
    """
    Main function that serves as the entry point for the NeuroStruct pipeline.
    It handles the initialization of logging, configuration loading, and execution
    of predefined jobs. The function ensures the workflow completes successfully
    or gracefully handles any critical failures.

    Raises:
        SystemExit: Exits the program with a status code of 1 in case of pipeline
        failure due to an unhandled exception.
    """
    logger = setup_logging()

    logger.info("========================================")
    logger.info("   🚀 NeuroStruct Pipeline")
    logger.info("========================================")

    try:
        # Load the Map
        logger.info("📂 Loading configuration...")
        study_config = cfg.load_study_config()

        # Execute the Jobs
        jobs.run_all(study_config=study_config)

        logger.info("\n✨ Pipeline finished successfully.")

    except Exception as e:
        logger.error(f"❌ Critical Pipeline Failure: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()