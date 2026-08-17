from airflow.sdk import dag, task, task_group, Asset
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

    wait_for_resource = FileSensor(fs_conn_id='fs_default', task_id='wait_for_resource', filepath='/opt/airflow/include/tiktok_google_play_reviews.csv')

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
        dtypes={'reviewId': "string", 
                'userName': "string", 
                'userImage': "string", 
                'content': "string", 
                'score': "Int64", 
                'thumbsUpCount': "string",
                'reviewCreatedVersion': "string", 
                'replyContent': "string",
                }

        date_cols = ['at', 'repliedAt']

        @task
        def null_replace(path: str):
            pd.set_option('display.max_columns', None)
            df = pd.read_csv(path, dtype=dtypes, parse_dates=date_cols) #type:ignore

            string_cols = df.select_dtypes(include=['string', 'object']).columns
            df[string_cols] = df[string_cols].fillna('-')

            # df.fillna('-', inplace=True)

            print(df.head(5))

            df.to_csv(path, index=False)

            return path

        @task
        def remove_unnecessary_characters():
            raw_file_path = '/opt/airflow/include/tiktok_google_play_reviews.csv'
            df = pd.read_csv(raw_file_path, dtype=dtypes, parse_dates=date_cols) #type:ignore
                
            pattern_to_replace = rf"[^\w\s{re.escape(string.punctuation)}]"

            df = df.replace(to_replace=pattern_to_replace, value='',regex=True)

            print(df.head(5))

            out_file = '/opt/airflow/include/cleaned_reviews.csv'
            df.to_csv(out_file, index=False)
        
            return out_file

        @task(outlets=[Asset("cleaned_reviews_asset")])
        def sort_data(path: str):
            # path = '/opt/airflow/include/cleaned_reviews.csv'
            df = pd.read_csv(path, dtype=dtypes, parse_dates=date_cols) #type:ignore
            
            df.sort_values(by='at', ascending=True, inplace=True)

            print(df.head(5))

            df.to_csv(path, index=False)
            
        sort_data(null_replace(remove_unnecessary_characters())) #type:ignore


    wait_for_resource >> check_if_empty() >> [log_empty_file(), processing_group()] #type:ignore


pipeline_mongo()
        


            

