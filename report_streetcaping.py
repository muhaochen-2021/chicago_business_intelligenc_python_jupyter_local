import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 
#from psycopg2 import extras as ex
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm
import re
from sklearn.preprocessing import *
import datetime

def rp_street():

        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        # get data
        select_data = (
            """
                SELECT * FROM "taxi_trip_total";
            """)
        # create the table
        cursor.execute(select_data)
        result_1 = cursor.fetchall();
        # set dataframe
        df_2 = pd.DataFrame(result_1)
        df_2.columns = ["id","trip_id","trip_start_timestamp","trip_end_timestamp","pickup_centroid_latitude","pickup_centroid_longitude",
                        "dropoff_centroid_latitude","dropoff_centroid_longitude","pickup_zip_code","dropoff_zip_code"]
        df_2 = df_2[["trip_start_timestamp","trip_end_timestamp","pickup_zip_code","dropoff_zip_code"]]
        df_2.pickup_zip_code = df_2.pickup_zip_code.astype("int")
        df_2.dropoff_zip_code = df_2.dropoff_zip_code.astype("int")
        #divide into pickup and dropoff
        df_2_pick = df_2[["trip_start_timestamp","trip_end_timestamp","pickup_zip_code"]]
        df_2_pick.columns = ["trip_start_timestamp","trip_end_timestamp","zipcode"]
        df_2_off = df_2[["trip_start_timestamp","trip_end_timestamp","dropoff_zip_code"]]
        df_2_off.columns = ["trip_start_timestamp","trip_end_timestamp","zipcode"]
        #merge
        df_2_total = df_2_pick.append(df_2_pick)
        df_2_total = df_2_total.reset_index(drop=True)
        df_2_total["ymd"] = df_2_total["trip_start_timestamp"].apply(lambda x:x.strftime("%Y-%m-%d"))
        df_2_total = df_2_total[["ymd","zipcode"]]
        df_2_total["help"] = 0
        # record
        rp_df_2_total = df_2_total[['ymd',"zipcode","help"
                                ]].groupby(by=["ymd","zipcode"],dropna=True)["help"].count().reset_index()
        rp_df_2_total.columns=['ymd',"zipcode","trip_count"]
        # predict
        avg_df = rp_df_2_total.groupby(by=["zipcode"],dropna=True)["trip_count"].mean().reset_index()
        avg_df["predict_date"] = df_2["trip_start_timestamp"][0]+ datetime.timedelta(days=1)
        avg_df["predict_date"] = avg_df["predict_date"].apply(lambda x:x.strftime("%Y-%m-%d"))
        avg_df.columns = ["zipcode","predict_count","predict_date"]

        return avg_df