import os
import sys


# Manually adding python package to path
def patch_sys_path():
    for entry in os.listdir("/tmp"):
        if entry.startswith("glue-python-libs"):
            zip_path = os.path.join("/tmp", entry, "src.zip")
            if os.path.exists(zip_path):
                print(f"Appending zip path: {zip_path}")
                sys.path.append(zip_path)
