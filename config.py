DATA_DIR = r"D:\json_tasks\pdp\PDP"

USER="root"
PASSWORD="sid"
HOST="localhost"
PORT=3306
DB="grabfood"
files_batch=10000
start=0
end=len(os.listdir(DATA_DIR))
BATCH_SIZE = 500
tab1="pdp_restaurant_table"
tab2="pdp_menu_items_table"
total_threads=end//files_batch

BATCH_SIZE = 500
