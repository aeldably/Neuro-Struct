

from nirs_to_bids import run_nirs_conversion
from src import config as cfg


def run_inventory_check(study_config):
    """
    Checks if all 'Source' folders defined in the JSON actually exist on disk.
    """
    print(f"\n{'=' * 50}")
    print(f"🚀 PIPELINE INVENTORY CHECK")
    print(f"{'=' * 50}")

    for key, folder_name in study_config.get("Sources", {}).items():
        try:
            path = cfg.get_input_path(study_config, key)
            status = "✅ Found" if path.exists() else "❌ Missing"

            # Count items if the folder exists
            count = len(list(path.glob("*"))) if path.exists() else 0
            print(f"📂 {key:<15} ({folder_name}): {status} ({count} items)")

        except Exception as e:
            print(f"⚠️ {key:<15} : Config Error. {e}")

    print(f"{'-' * 50}\n")


def main():
    """
    Master Pipeline Controller.
    """
    try:
        study_config = cfg.load_study_config()
    except Exception as e:
        print(f"❌ {e}")
        return

    # Check Global Health
    run_inventory_check(study_config)

    # Run Modules
    run_nirs_conversion(study_config)

    # Future modules go here:
    # run_behavior_conversion(study_config)
    # run_mocap_conversion(study_config)


if __name__ == "__main__":
    main()