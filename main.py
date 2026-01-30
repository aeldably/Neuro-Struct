import logging
import sys
from src import config as cfg
from src import jobs


# Setup Logging
def setup_logging(log_file="pipeline.log"):
    """
    Configures logging to write to both console (stdout) and a file.
    """
    # Create a custom logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # File Handler (Writes to disk)
    file_handler = logging.FileHandler(log_file, mode='w')  # 'w' overwrites each run, 'a' appends
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    # Console Handler (Writes to screen)
    console_handler = logging.StreamHandler(sys.stdout)
    console_format = logging.Formatter('%(message)s')  # Keep the console clean
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