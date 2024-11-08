#2418 mohanbabu decorator function used for logfile creation..

import functools
import datetime
import pandas as pd
import os
from database import get_db_settings
import oracledb
from dotenv import dotenv_values

#db connection dsn tns

def db_update(input_query):
    db_settings = get_db_settings()
    dsn_tns = oracledb.makedsn(db_settings['LIVE IP'], db_settings[ 'ORACLE PORT'], db_settings['SID'])
    with oracledb.connect (user=db_settings ['OWNER ID'], password=db_settings['PASSWORD'], dsn=dsn_tns) as conn:
        with conn.cursor() as cur:
            cur.execute(input_query)
            conn.commit()

#using decorator function to, write logfiles
def parquet_logs_decorator(func):
    #A decorator that logs the execution details of a function.
    @functools.wraps(func)
    def parquet_logs(*args, **kwargs):
        config = dotenv_values('.env')
        current_directory = os.getcwd()
        parquet_log_folder = os.path.join(current_directory, 'Parquet_Log_Folder')
        os.makedirs(parquet_log_folder, exist_ok=True)
        
        return_data = func(*args, **kwargs)
        start_time = return_data.get('start_time', datetime.datetime.now())
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        log_dict = {
            '---RUN TIME--=' : return_data.get('run date',''),
            '--START_TIME--=': start_time,
            '--END_TIME---=' : end_time,
            '---DURATION--=' : duration,
            '--STATUS_RESPONSE--=': return_data.get('error line no',''),
            '-------STATUS------=': return_data.get('status',''),
            '----FILE_STATUS----=': return_data.get('status_msg',''),
            '-FILE_PATH_FILENAME-=':return_data.get('file_name',''),
            '----FILE_SIZE_kb----=':return_data.get('file_size_kb',''),
            '---ZIP_FOLDER_PATH--=':return_data.get('zip_folder_path','')
            '--PROCESS_NAME----- =':return_data.get('process_name',''),
            '**********************': '*********************************************************'
        }

        log_file = f'Parquet_Log_File{datetime.datetime.now().strftime("%Y_%m_%d")}.log'
        df_log = pd.DataFrame.from_dict(log_dict, orient='index')
        # Save the log to a file
        df_log.to_csv(os.path.join(parquet_log_folder, log_file), mode='a', header=False, sep='\t')
        # Convert df_log tO a string format for sQL
        df_log_string = df_log. to_string(index=True, header=False).replace ("'", "''")
        db_settings = get_db_settings()
        #db log query
        #db_log_query= f"update DS_DAILY_MIS_QUERY LOG SET Process_info ={df_log) Where process_name = '{return_data["process_name'1} and file
        db_log_query = config.get('db_log_query', '').format(db_settings['GLOBALUSER'], df_log_string, return_data.get('process name', ''))
        db_update(db_log_query) #UAT-DATASOURCE #0AT-GLOBALUSER
        return return_data

    return parquet_logs