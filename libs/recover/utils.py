import shutil
import os


def recopy_files(old_file, new_files):
    for f in new_files:
        shutil.copy2(old_file, f)
    os.remove(old_file)