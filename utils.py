import os
import gzip
import json


def load_files(data_dir,start_val,end_va):

    files = os.listdir(data_dir)
    files.sort()

    for file in files[start_val:end_val]:

        path = os.path.join(data_dir, file)

        try:

            with gzip.open(path, "rt", encoding="utf-8") as f:
                yield json.load(f)

        except Exception as e:


            print("Failed file:", file, e)
