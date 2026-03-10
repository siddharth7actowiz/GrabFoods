DATA_DIR = r"D:\json_tasks\pdp\PDP"
import os
USER="root"
PASSWORD="sid"
HOST="localhost"
PORT=3306
DB="grabfood"
files_batch=10000
start=0
end=len(os.listdir(DATA_DIR))
BATCH_SIZE = 500
TABLE_NAME="GrabFoodRest"
total_threads=8
