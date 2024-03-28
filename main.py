import os
import glob
import subprocess
import pandas as pd
import tqdm
from rich.console import Console
from rich.table import Table

# constants
PGUSER = os.getenv('PGUSER')
PGPASSWORD = os.getenv('PGPASSWORD')
PGHOST = os.getenv('PGHOST')
PGDATABASE = os.getenv('PGDATABASE')
COLUMNS = [
    "dispatching_base_num", 
    "pickup_datetime", 
    "dropoff_datetime", 
    "pickup_location_id", 
    "dropoff_location_id", 
    "legacy_shared_ride_flag", 
    "affiliated_base_num"]

suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
def convert_bytes(nbytes):
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])


# Create database schema
cmd = f"cat './database_files/00-database-schema.sql' | PGPASSWORD={PGPASSWORD} psql -h {PGHOST} -U {PGUSER}"
result = subprocess.run([cmd], shell=True, capture_output=True, text=True)
# print(result) # 'CREATE DATABASE\n'

cmd = f"cat './database_files/01-fhv-schema.sql' | PGPASSWORD={PGPASSWORD} psql -h {PGHOST} -U {PGUSER} -d {PGDATABASE}"
result = subprocess.run([cmd], shell=True, capture_output=True, text=True)
# print(result) # 'CREATE TABLE\nCREATE TABLE\nCREATE TABLE\nCREATE INDEX\n'


table = Table(title="fhv tripdata")
table.add_column('File name')
table.add_column('Total number of rows')
table.add_column('Total Memory Consumption')
table.add_column('Staging Status')
table.add_column('Populate Status')

fhv_tripdata_files = tqdm.tqdm(glob.glob('data/fhv_tripdata_*.parquet')[1:3], desc ="Loading fhv tridata file into the database")
for src_file in fhv_tripdata_files:
    dst_file = src_file.replace(".parquet", ".csv")

    df = pd.read_parquet(src_file, )
    df['PUlocationID'] = df['PUlocationID'].astype('Int64')
    df['DOlocationID'] = df['DOlocationID'].astype('Int64')
    df['SR_Flag'] = df['SR_Flag'].astype('Int64')
    df.to_csv(dst_file, index=False, header=False)

    copy_cmd = f'cat {dst_file} | PGPASSWORD={PGPASSWORD} psql -h {PGHOST} -d {PGDATABASE} -U {PGUSER} -c "COPY fhv_trips_staging ({','.join(COLUMNS)}) FROM stdin CSV HEADER;"'
    copy_result = subprocess.run([copy_cmd], shell=True, capture_output=True, text=True)

    populate_cmd = f"PGPASSWORD={PGPASSWORD} psql -h {PGHOST} -d {PGDATABASE} -U {PGUSER} -f ./setup_files/populate_fhv_trips.sql"
    populate_result = subprocess.run([populate_cmd], shell=True, capture_output=True, text=True)
    
    table.add_row(src_file, str(len(df)), str(convert_bytes(df.memory_usage(index=True, deep=True).sum())), copy_result.stdout, populate_result.stdout)
    
    os.remove(dst_file)   

console = Console()
console.print(table)



# result = subprocess.run(["dir"], shell=True, capture_output=True, text=True)
# print(result.stdout)
