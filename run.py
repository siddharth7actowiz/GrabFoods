import os

from config import *

files = os.listdir(DATA_DIR)
files.sort()

for s in range(start, end, files_batch):

    last = min(s + files_batch, end)
    command="start cmd /k python main.py " + str(s) + " " + str(last)
    with open("execute.bat","a") as b:
        b.write(command+'\n')