from airflow.sdk import dag, task, task_group
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime
import pandas as pd
import os
import re
import string

@dag(
    start_date=datetime(2026,8,10),
    schedule='@daily'
)
def pipeline_mongo():

    wait_for_resource = FileSensor(fs_conn_id='fs_default', task_id='wait_for_resource', filepath='/opt/include/tiktok_google_play_reviews.csv')

    @task.branch
    def check_if_empty():
        if(os.stat('/opt/airflow/include/tiktok_google_play_reviews.csv').st_size == 0):
            return 'log_empty_file'

        return 'processing_group'
    
    @task.bash
    def log_empty_file():
        return "echo 'tiktok_google_play_reviews.csv is empty!'"

    
    @task_group(group_id='processing_group')
    def processing_group():

        dtypes_string_dates={'reviewId': "string", 
                'userName': "string", 
                'userImage': "string", 
                'content': "string", 
                'score': "Int64", 
                'thumbsUpCount': "string",
                'reviewCreatedVersion': "string", 
                'replyContent': "string",
                'at': "string",
                'repliedAt':"string"
                }

        @task
        def null_replace():

            # date_cols = ['at', 'repliedAt']

            try:
                df = pd.read_csv('/opt/airflow/include/tiktok_google_play_reviews.csv', dtype=dtypes_string_dates) #type:ignore
            except UnicodeDecodeError:
                df = pd.read_csv('/opt/airflow/include/tiktok_google_play_reviews.csv', dtype=dtypes_string_dates, encoding='latin-1') #type:ignore

            df.fillna('-', inplace=True)

            print(df.head(5))

            out_file = '/opt/airflow/include/cleaned_reviews.csv'
            df.to_csv(out_file, index=False)

            return out_file

        @task
        def remove_unnecessary_characters(path: str):
            path = '/opt/airflow/include/cleaned_reviews.csv'
            try:
                df = pd.read_csv(path, dtype=dtypes_string_dates) #type:ignore
            except UnicodeDecodeError:
                df = pd.read_csv(path, dtype=dtypes_string_dates, encoding='latin-1') #type:ignore

            pattern_to_replace = rf"[^a-zA-Z0-9\s{re.escape(string.punctuation)}]"

            df = df.replace(to_replace=pattern_to_replace, value='',regex=True)

            print(df.head(5))

            df.to_csv(path, index=False)
        
            return path


        remove_unnecessary_characters(null_replace()) #type:ignore
 
    wait_for_resource >> check_if_empty() >> [log_empty_file(), processing_group()] #type:ignore



pipeline_mongo()
        


            

