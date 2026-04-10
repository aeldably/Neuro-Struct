import re
import pathlib

# SET TOO FALSE TO ACTUALLY RENAME FILES
DRY_RUN = False
# Using pathlib to make pathing bulletproof across Windows/Mac
FOLDER_PATH = pathlib.Path("inputs/RawArtworks")


def rename_files():
    # Regex for Dyads: handles dyad-1020_D2_session-5.tif or dyad-1020-D2_session-5.tif
    dyad_pattern = re.compile(r'dyad-(\d+)[_-]D(\d+)_session-(\d+)\.tif')

    # Regex for Solos: handles sub-127_solo_session-5 or sub-324-solo-session-2
    solo_pattern = re.compile(r'sub-(\d+)[_-]solo[_-]session-(\d+)\.tif')

    if not FOLDER_PATH.exists():
        print(f"❌ Error: Folder not found: {FOLDER_PATH.absolute()}")
        return

    count = 0
    skipped_ghosts = 0

    print(f"📂 Scanning: {FOLDER_PATH.absolute()}\n")

    for file_path in FOLDER_PATH.iterdir():
        # 1. Skip Directories
        if file_path.is_dir():
            continue

        filename = file_path.name

        # 2. Logic: Skip the "._" metadata files (The Ghosts)
        if filename.startswith('._'):
            skipped_ghosts += 1
            continue

        new_name = None

        # 3. Match Dyads
        dyad_match = dyad_pattern.match(filename)
        if dyad_match:
            id_num, d_num, sess_num = dyad_match.groups()
            new_name = f"dyad-{id_num}-D{d_num}_session-{sess_num}.tif"

        # 4. Match Solos
        solo_match = solo_pattern.match(filename)
        if solo_match:
            id_num, sess_num = solo_match.groups()
            new_name = f"sub-{id_num}-solo_session-{sess_num}.tif"

        # 5. Perform Rename
        if new_name and new_name != filename:
            target_path = FOLDER_PATH / new_name

            if DRY_RUN:
                print(f"[DRY RUN] {filename}  -->  {new_name}")
            else:
                file_path.rename(target_path)
                print(f"✅ Renamed: {filename}  -->  {new_name}")
            count += 1

    # Final Summary
    print("-" * 50)
    print(f"Final Tally:")
    print(f"   🔄 Files to Rename: {count}")
    print(f"   ⏭️  Mac Ghosts Ignored: {skipped_ghosts}")

    if count == 0:
        print("\n🎉 No files need renaming! Your naming is already standardized.")
    elif DRY_RUN:
        print(f"\n⚠️  Action Required: Set DRY_RUN = False in the script to apply these {count} changes.")


if __name__ == "__main__":
    rename_files()