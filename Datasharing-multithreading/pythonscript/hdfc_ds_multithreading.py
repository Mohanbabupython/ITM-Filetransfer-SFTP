
#2418 Mohanbabu  query execution and creating parquet files using multithreading.
import threading
import os
import sys
import datetime
import traceback
import zipfile
from io import BytesIO
import psutil
import polars as pl
import pandas as pd
import oracledb
from dotenv import dotenv_values, load_dotenv
from database import get_db_settings
from decorator import parquet_logs_decorator

# Load environment variables
load_dotenv()
oracle_path = os.getenv('oracle_path')
print('oracle path', oracle_path)
env_path = os.getenv('env_path')
print('env_path', env_path)

config = dotenv_values('. env')
current_date_time = datetime.datetime.now()
date_time = current_date_time.strftime('%d-%m-%Y')

# To create folders
def create_folders():
    try:
        global date_folder, Query_execution_folder, zip_Parquet_folder, parquet_folder
        current_directory = os.getCWd()
        parquet_folder = os.path.join(current_directory,"parquet_folder")
        os.makedirs(parquet_folder, exist_ok = True)
        Query_execution_folder = os.path.join(current_directory,"Query_execution_folder")
        os.makedirs(Query_execution_folder, exist_ok=True)
        zip_Parquet_Folder =  os.path.jołn(current_directory, 'zip Parquet_folder')
        os.makedirs (zip_Parquet_Folder, exist_ok=True)
        folder_creation_status = "Folders created successfully,"
        print( 'Folder_creation_ status', folder_creation_status)

    except Exception as e:
        folder_creation_status = f"Error creating folders: {str(e)}"
        print('folder_creation_status', folder_creation_status)

create_folders()

# Query execution
def query():
    try:
        db_settings = get_db_settings()
        dsn_tns = oracledb.makedsn(db_settings['LIVE_IP'], db_settings[ 'ORACLE_PORT' ], db_settings['SID'])
        with oracledb.connect(user=db_settings['OWNER_ID'], password=db_settings ['PASSWORD'], dsn=dsn_tns) as conn:
            with conn.cursor() as cur:
                query_str = config.get('query', "").format (db_settings[ 'GLOBALUSER'])
                cur.execute(query_str)
                data = []
                for row in cur.fetchall ():
                    query_text = row[0].read() if isinstance(row[0], oracledb.LOB) else row[0]
                    query_text = query_text.replace(':as_schema', db_settings['DATASOURCE'])
                    data.append({'query': query_text, "file_name": row[1], "process_name": row[2]})
                return data
    except Exception as e:
        traceback.print_exc()

#Dataaase updste
def db_update(input_query):
    db_settings = get_db_settings()
    dsn_tns = oracledb.makedsn(db_settings['LIVE_IP'], db_settings[ 'ORACLE_PORT' ], db_settings['SID'])
    with oracledb.connect(user=db_settings['OWNER_ID'], password=db_settings ['PASSWORD'], dsn=dsn_tns) as conn:
        with conn.cursor() as cur:
            cur.execute(input_query)
            conn.commit()

# Using decorator to call function
@parquet_logs_decorator
def multi_parquet_creation(query_obj):
    start_tìme = datetime.datetime.now()
    run_date = start_tìme.strftime("%Y%m%d")
    query = query_obj['query']
    file_name = query_obj['file name']
    process_name = query_obj['process_name']
    Status = 'success'
    error_line_no = 'Response success'
    status_msg = 'Parquet file created successfully.'
    file_size_kb = ''
    zip_folder_path = ''
    parquet_path = os.path.join(date_folder, f"{file_name}.parquet")
    
    # Check if the file has already been created today
    if os.path.exists(parquet_path):
        status = 'skipped'
        status_msg = 'Parquet file already created today.'
        return{'status': status,'status_msg': status_msg, 'file name': file_name}
    try:
        db_settings = get_db_settings()
        dsn_tns = oracledb.makedsn(db_settings['LIVE_ IP'], db_settings['ORACLE_PORT'], db_settings['STD'])
        run_id = start_time.strftime("%Y%m%d")
        insert_run_date = start_time.strftime("%Y-%b-%d %H:%M:%S")
        
        # Create DSN and connect to the database
        with oracledb.connect(user=db_settings['OWNER ID'], password=db_settings['PASSWORD'], dsn=dsn_tns) as conn:
            with conn.cursor() as cur:
                new_query = config.get('new_query','')
                new_query_conf = new_query.format(db_settings['GLOBALUSER'], db_settings['GLOBALUSER'])
                cur.execute(new_query_conf)
                new_rows = cur.fetchall()
                new_data = []
                for row in new_rows:
                    new_row = [col.read() if isinstance(col, oracledb.LOB) else col for col in row]
                    new_data.append(new_row)
                new_df_pandas = pd.DataFrame(new_data, columns=[col[0] for col in cur.description])
                new_df = pl.DataFrame(new_df_pandas)
                if len(new_df) > 0:
                    new_status_flag = new_df['STATUS FLAG'][0]
                    new_file_name = new_df['FILE NAME'][0]
                else:
                    new_status_flag = None
                    new_file_name = None

                #Check quèry conditions.
                if file_name != new_file_name or (file name == new_file_name and (new_status_flag == 'P' or new_status_flag == 'E')):
                    check_query = config.get('check_query','')
                    check_query_conf = check_query.format (db_settings['GLOBALUSER'], file_name=file_name, process_name=process_name)
                    cur.execute(check_query_conf)
                    check_rows = cur.fetchall()
                    check_data = []
                
                    for row in check_rows:
                        check_row = [col.read() if isinstance(col, oracledb.LOB) else col for col in row]
                        check_data.append(check_row)
                        
                    check_df_pandas = pd.DataFrame(check_data, columns=[col[0] for col in cur.description])
                    check_df = pl.DataFrame (check_df_pandas)
                    
                    if check_df.shape [0] == 1:
                        pass
                    else:
                        log_insert_query_try = config.get('log_insert_query_try', '')
                        log_insert_query_conf = log_insert_query_try.format(
                            db_settings ['GLOBALUSER'],
                            process_name=process_name,
                            file_name=file_name,
                            run_id=run_id,
                            insert_run_date=insert_run_date
                        )
                        print("Executing Query:", log_insert_query_conf)
                        db_update(log_insert_query_conf)
                    cur.execute(query)
                    df_rows = cur.fetchall()
                    df_data = []
                    for row in df_rows:
                        df_row = [col.read() if isinstance(col, oracledb.LOB) else col for col in row]
                        df_data.append(df_row)
                    df_pandas = pd.DataFrame (df_data, columns=[col[0] for col in cur.description] )
                    df = pl.DataFrame(df_pandas)
                    df.write_parquet(parquet_path)

                    # Create a zip folder for today's date if it doesn't exist
                    Current_date_str = start_time.strftime("%Y-%m-%d")
                    zip_folder_path = os.path.join(zip_Parquet_folder, current_date_str)
                    os.makedirs(zip_folder_path, exist_ok=True)
                    
                    # Create a zip file for the single Parquet, file
                    zip_file_path = os.path.join(zip_folder_path, f"{file_name}.zip")
                    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                        zip_file.write(parquet_path, os.path.basename(parquet_path))
                        status='success'
                        error_line_no ='Response success'
                        status_msg = 'Parquet file created and zipped successfully.'
                        file_size_kb = os.path.getsize(parquet_path) / 1024

                        update_qry1 = config.get('update_qryl','')
                        update_qry_conf = update_qry1.format(db_settings['GLOBALUSER'], file_name=file_name)
                        db_update(update_qry_conf)


    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        error_line_no = exc_tb.tb_lineno
        error_line_no = f"Response Failed: {str(e)} at line no: {error_line_no}"
        status = 'failure'
        status_msg = 'Error in transferring files'
        file_size_kb =''
        zip_folder_path = 'Zip folder not created.'
        
        if "'" in error_line_no:
            error_line_no = error_line_no.replace ("'", '')
            log_insert_query_except = config.get('log_insert_query_except', '')
            log_insert_query_conf = log_insert_query_except.format(
            file_name = file_name
            run_id=run_id,
            insert_run_date=insert_run_date,
            error_line_no=error_line_no
            )
    finally :
        return {
            'start_time': start_time,
            'run_date': start_time,
            'status': status,
            'error_line_no': error_line_no,
            'status_msg': status_msg,
            'file_name': file_name,
            'file_size_kb': file_size_kb,
            'zip_folder_path': zip_folder_path,
            'process_name': process_name
        }

# Multithread function
def worker (datas) :
    return multi_parquet_creation(datas)

# Memory utilization percentage
def counts():
    global Count3, Counts3, Count2, Counts2, Count1, Counts1
    Count3= int(config.get('Count3',0))
    Counts3= int(config. get(' Counts3', 50))
    Count2= int(config. get('Count2',.50))
    Counts2= int(config.get('Counts2', 70))
    Count1= int(config.get(' Count1', 70))
    Counts1= int(config. get('Counts1', 85))
counts ()

if __name__ == "__main__":
    memory_utilization = psutil.virtual_memory().percent
    print ('Memory Utilization:', memory_utilization)
if int(Count3) <= memory_utilization <= int(Counts3):
    thread_count = 3
elif int(Count2) < memory_utilization <= int (Counts2):
    thread_count = 2
elif int(Count1) < memory_utilization <= int (Counts1):
    thread_count = 1
else:
    thread_count = 0
#print(f'Thread Count allocated:', {thread count})

# Get the start time for logging
start_time = datetime.datetime.now()
st_time = start_time.strftime('%H:%M:%S')
query_exec = os.path.join(Query_execution_folder, f'Query_execution_{date_time}. log')

with open(query_exec, 'a') as f:
    f.write(f"Start time:{st_time}\nMemory utilization: {memory_utilization}\n")

if thread_count > 0:
    data = query()
    threads = []
    for item in data:
        t = threading.Thread(target=worker, args=(item,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    # Log completion afterthreads join
    end_time = datetime.datetime.now()
    en_time = end_time.strftime('%H:%M:%S')
    with open (query_exec, 'a') as f:
        f.write(f"End_time: {en_time}\nQuery execution Completed.\n")
else:
    query_execution_status = 'Failed to run. Multiprocess ended due to high memory utilization!!!*'
    end_time = datetime.datetime. now()
    en_time = end_time.strftine('%H:%M:%S')
    with open(query_exec, 'a') as f:
        f.write(f"End_time: {en_time} \nMemory_utilization: {memory_utilization}\nQuery_ execution : {query_execution_status} \n")



