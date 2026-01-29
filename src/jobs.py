from src.converters.nirs_converter import NirsConverter
from src.converters.artworks_converter import ArtworksConverter
from src.utils import run_inventory


def run_nirs_job(study_config):
    """
    Job 2: Run NIRS Conversion.
    Instantiates the Converter class and runs it.
    """
    if "NIRS" in study_config.get("Sources", {}):
        converter = NirsConverter(study_config)
        converter.run()
    else:
        print("ℹ️  Skipping NIRS (Not configured)")


def run_artworks_job(study_config):
    """
    Job 3: Run Artworks Conversion.
    Checks if 'RawArtworks' is defined in the config source list.
    """
    if "Art" in study_config.get("Sources", {}):
        converter = ArtworksConverter(study_config)
        converter.run()
    else:
        print("ℹ️  Skipping Artworks (Not configured)")


def run_all(study_config):
    """
    Master execution function.
    """
    run_inventory(study_config)
    # run_nirs_job(study_config)
    run_artworks_job(study_config)
    # run_behavior_job(study_config)