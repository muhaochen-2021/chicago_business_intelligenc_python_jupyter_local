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

def  rp_airport():
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
    df_1_pick = df_1.copy()
    df_1_pick.columns = ["pickup_zip_code","community","neighborhood"]
    df_1_drop = df_1.copy()
    df_1_drop.columns = ["dropoff_zip_code","community","neighborhood"]
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
    df_2 = df_2[["trip_id","trip_start_timestamp","trip_end_timestamp","pickup_zip_code","dropoff_zip_code"]]
    df_2.pickup_zip_code = df_2.pickup_zip_code.astype("int")
    df_2.dropoff_zip_code = df_2.dropoff_zip_code.astype("int")
    # get data
    select_data = (
        """
            SELECT * FROM "covid_weekly_case";
        """)
    # create the table
    cursor.execute(select_data)
    result_1 = cursor.fetchall()
    # set dataframe
    df_3 = pd.DataFrame(result_1)
    df_3.columns = ["id","dropoff_zip_code","week_number","week_start","week_end","cases_weekly","cases_cumulative"]
    df_3 = df_3[["dropoff_zip_code","week_start","cases_weekly"]]
    df_3["week_start"] = df_3["week_start"].apply(lambda x:x.strftime("%Y-%m-%d"))
    df_3 = df_3[df_3["dropoff_zip_code"]!="Unknown"]
    df_3.dropoff_zip_code = df_3.dropoff_zip_code.astype("int")
    df_2_total = df_2.merge(df_1_pick,how='inner', on='pickup_zip_code')
    df_2_total["ymd"] = df_2_total["trip_start_timestamp"].apply(lambda x:x.strftime("%Y-%m-%d"))
    df_2_total = df_2_total[df_2_total["community"] == "O'Hare"]
    df_2_total = df_2_total[["trip_id","ymd","dropoff_zip_code"]]
    df_2_total = df_2_total.merge(df_1_drop,how='inner', on='dropoff_zip_code')
    df_2_total = df_2_total[["trip_id","ymd","dropoff_zip_code","community"]]
    df_2_total = df_2_total.merge(df_3,how='inner', on='dropoff_zip_code')
    df_2_total = df_2_total[["community","trip_id","ymd","dropoff_zip_code","week_start","cases_weekly"]]
    # neighborhoodsccvi_category
    new_w = df_2_total.groupby(by=["dropoff_zip_code"],dropna=False)["week_start"].max().reset_index()
    new_w = new_w.merge(df_2_total,how='left', on=['week_start',"dropoff_zip_code"])
    return new_w