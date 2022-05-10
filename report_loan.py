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

def rp_loan():

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
                SELECT * FROM "building_permits";
            """)
        # create the table
        cursor.execute(select_data)
        result_1 = cursor.fetchall();
        # set dataframe
        df_2 = pd.DataFrame(result_1)
        df_2.columns = ["id","permit_id","permit_type","total_fee","latitude","longitude","zipcode"]
        df_2 = df_2[["permit_id","permit_type","zipcode"]]
        df_2.zipcode = df_2.zipcode.astype("int")
        # get data
        select_data = (
            """
                SELECT * FROM "public_health_statistics";
            """)
        # create the table
        cursor.execute(select_data)
        result_1 = cursor.fetchall();
        # set dataframe
        df_3 = pd.DataFrame(result_1)
        df_3.columns = ["id","community_area_name","below_poverty_level","per_capita_income","unemployment"]
        df_3 = df_3[["community_area_name","per_capita_income"]]
        df_3.columns = ["community","per_capita_income"]
        pro_df_1 = df_3.merge(df_1,how='inner', on='community')
        pro_df_2 = pro_df_1.merge(df_2,how='inner', on='zipcode')
        pro_df_2 = pro_df_2[["permit_id","permit_type","per_capita_income","community"]]
        pro_df_2 = pro_df_2.reset_index(drop=True)
        rp_loan = pro_df_2[pro_df_2["permit_type"] == "PERMIT - NEW CONSTRUCTION"]
        rp_loan = rp_loan[rp_loan["per_capita_income"] < 30000]
        rp_loan = rp_loan.reset_index(drop=True)
        rp_loan

        return rp_loan