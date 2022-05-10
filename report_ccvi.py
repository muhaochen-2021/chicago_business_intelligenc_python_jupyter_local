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

def rp_ccvi():
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        # get data
        select_data = (
            """
                SELECT * FROM "neighborhood_community_zip";
            """)
        # create the table
        cursor.execute(select_data)
        result_1 = cursor.fetchall();
        # set dataframe
        df_1 = pd.DataFrame(result_1)
        df_1.columns = ["id","geo_id","zipcode","community","neighborhood"]
        df_1 = df_1[["zipcode","community","neighborhood"]]
        df_1.zipcode = df_1.zipcode.astype("int")
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
        # get data
        select_data = (
            """
                SELECT * FROM "covid_ccvi";
            """)
        # create the table
        cursor.execute(select_data)
        result_1 = cursor.fetchall();
        # set dataframe
        df_3 = pd.DataFrame(result_1)
        df_3.columns = ["id","community_area_name","ccvi_score","ccvi_category"]
        df_3 = df_3[["community_area_name","ccvi_category"]]
        df_3.columns = ["community","ccvi_category"]
        #divide into pickup and dropoff
        df_2_pick = df_2[["trip_start_timestamp","trip_end_timestamp","pickup_zip_code"]]
        df_2_pick.columns = ["trip_start_timestamp","trip_end_timestamp","zipcode"]
        df_2_off = df_2[["trip_start_timestamp","trip_end_timestamp","dropoff_zip_code"]]
        df_2_off.columns = ["trip_start_timestamp","trip_end_timestamp","zipcode"]
        # pickup
        df_2_pick_1 = df_2_pick.merge(df_1,how='inner', on='zipcode')
        df_2_pick_1["type"] = "pick"
        # drop
        df_2_drop_1 = df_2_off.merge(df_1,how='inner', on='zipcode')
        df_2_drop_1["type"] = "drop"
        #merge
        df_pick_drop = df_2_pick_1.append(df_2_drop_1)
        df_pick_drop = df_pick_drop.reset_index(drop=True)
        df_pick_drop
        #add ccvi
        df_pick_drop = df_pick_drop.merge(df_3,how='inner', on='community')
        # get hig ccvi_categore
        df_pick_drop_high_ccvi = df_pick_drop[df_pick_drop["ccvi_category"]=="HIGH"]
        df_pick_drop_high_ccvi = df_pick_drop_high_ccvi.reset_index(drop=True)
        # neighborhoodsccvi_category
        rp_df_pick_drop_high_ccvi = df_pick_drop_high_ccvi[['trip_start_timestamp',"neighborhood","type","ccvi_category"
                                ]].groupby(by=["trip_start_timestamp","neighborhood","type"],dropna=False)["ccvi_category"].count().reset_index()
        rp_df_pick_drop_high_ccvi.columns=['trip_start_timestamp',"neighborhood","type","ccvi_high_count"]

        return rp_df_pick_drop_high_ccvi