from airflow.sdk import Asset, task, dag
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd

@dag(schedule=[Asset('cleaned_reviews_asset')])
def load_data_mongo():

    @task
    def load_dataframe():
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client['reviewsdb']

        date_columns = ['at', 'repliedAt']
        df = pd.read_csv('/opt/airflow/include/cleaned_reviews.csv', na_values='-', parse_dates=date_columns) #type:ignore
        db['tiktok_reviews_db'].insert_many(df.to_dict('records'))

    load_dataframe()

load_data_mongo()
