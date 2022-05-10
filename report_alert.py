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
import tensorflow as tf
#from tensorflow.keras import *
from tensorflow import keras
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers.embeddings import Embedding
from keras.preprocessing import sequence

def  rp_alert():
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        # get daily data
        select_daily_data = (
            """
                SELECT * FROM "covid_daily_case";
            """)
        # create the table
        cursor.execute(select_daily_data)
        result = cursor.fetchall()
        # set dataframe
        daily = pd.DataFrame(result)
        daily.columns = ["id_u","time","new","death"]
        daily_new = daily[["time","new"]]
        # predict 
        scaler = MinMaxScaler()
        scaled_close_ori = [daily_new["new"][i] for i in range(len(daily_new["new"])-1,-1,-1)]
        avg_item = np.average(scaled_close_ori[-10:])
        scaled_close_ori = scaled_close_ori + [avg_item]*10
        scaled_close = np.array(scaled_close_ori).reshape([len(scaled_close_ori),1])
        scaled_close = scaler.fit_transform(scaled_close)
        scaled_close = scaled_close.reshape(-1, 1)
        SEQ_LEN = 10

        def to_sequences(data, seq_len):
            d = []

            for index in range(len(data) - seq_len):
                d.append(data[index: index + seq_len])

            return np.array(d)

        def preprocess(data_raw, seq_len, train_split):

            data = to_sequences(data_raw, seq_len)

            num_train = int(train_split * data.shape[0])
            #edit
            num_train = -10

            X_train = data[:num_train, :-1, :]
            y_train = data[:num_train, -1, :]

            X_test = data[num_train:, :-1, :]
            y_test = data[num_train:, -1, :]

            return X_train, y_train, X_test, y_test


        X_train, y_train, X_test, y_test =\
        preprocess(scaled_close, SEQ_LEN, train_split = 0.95)
        WINDOW_SIZE = SEQ_LEN - 1
        model = keras.Sequential()
        model.add(keras.Input(shape=(WINDOW_SIZE,X_train.shape[-1])))
        model.add(LSTM(WINDOW_SIZE,dropout=0.2))
        model.add(Dense(30, activation='relu'))
        model.add(Dense(10, activation='relu'))
        model.add(Dense(units=1, activation="linear"))
        print(model.summary())
        BATCH_SIZE = 16
        model.compile(
            loss='mean_squared_error',
            optimizer='adam'
        )
        history = model.fit(
            X_train,
            y_train,
            epochs=50,
            batch_size=BATCH_SIZE,
            shuffle=False,
            validation_split=0.1
        )
        y_hat = model.predict(X_test)
        y_test_inverse = scaler.inverse_transform(y_test)
        y_hat_inverse = scaler.inverse_transform(y_hat)
        def alert_func(x):
            if x >300:
                return "high"
            elif 100 <x <300:
                return "medium"
            else:
                return "low"
        pre_list = []
        int_time = daily_new["time"][0]
        for i in y_hat_inverse.tolist():
            value = i[0]
            int_time += datetime.timedelta(days=1)
            pre_list.append([int_time,value])
        pre_list = [pre_list[i] for i in range(len(pre_list)-1,-1,-1)]
        pre_list_df = pd.DataFrame(pre_list)
        pre_list_df.columns=["time","new"]
        new_case_pre_df = pre_list_df.append(daily_new)
        new_case_pre_df["time"] = new_case_pre_df["time"].apply(lambda x:x.strftime("%Y-%m-%d"))
        new_case_pre_df.dropna()
        new_case_pre_df["new"] = new_case_pre_df["new"].apply(lambda x:int(round(x,2)))
        new_case_pre_df["alert"] = new_case_pre_df["new"].apply(lambda x:alert_func(x))
        #----new_case_pre_df
        # get data
        select_data = (
            """
                SELECT * FROM "taxi_trip_total";
            """)
        # create the table
        cursor.execute(select_data)
        result_1 = cursor.fetchall()
        # set dataframe
        df_2 = pd.DataFrame(result_1)
        df_2.columns = ["id","trip_id","trip_start_timestamp","trip_end_timestamp","pickup_centroid_latitude","pickup_centroid_longitude",
                        "dropoff_centroid_latitude","dropoff_centroid_longitude","pickup_zip_code","dropoff_zip_code"]
        df_2 = df_2[["trip_id","trip_start_timestamp","trip_end_timestamp","pickup_zip_code","dropoff_zip_code"]]
        #-----df_2["trip_id"].unique()
        # get data
        select_data = (
            """
                SELECT * FROM "neighborhood_community_zip";
            """)
        # create the table
        cursor.execute(select_data)
        result_1 = cursor.fetchall()
        # set dataframe
        df_1 = pd.DataFrame(result_1)
        df_1.columns = ["id","geo_id","zipcode","community","neighborhood"]
        df_1 = df_1[["zipcode","community","neighborhood"]]
        df_1.zipcode = df_1.zipcode.astype("int")
        df_1_pick = df_1.copy()
        df_1_pick.columns = ["pickup_zip_code","community","neighborhood"]
        df_1_drop = df_1.copy()
        df_1_drop.columns = ["dropoff_zip_code","community","neighborhood"]
        #-----df_1["neighborhood"].unique()
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
                SELECT * FROM "covid_weekly_case";
            """)
        # create the table
        cursor.execute(select_data)
        result_1 = cursor.fetchall();
        # set dataframe
        df_3 = pd.DataFrame(result_1)
        df_3.columns = ["id","zipcode","week_number","week_start","week_end","cases_weekly","cases_cumulative"]
        df_3 = df_3[["zipcode","week_start","cases_weekly"]]
        df_3["week_start"] = df_3["week_start"].apply(lambda x:x.strftime("%Y-%m-%d"))
        df_3 = df_3[df_3["zipcode"]!="Unknown"]
        df_3.zipcode = df_3.zipcode.astype("int")
        def alert_func_zip(x):
            if x >30:
                return "high"
            elif 10 <x <30:
                return "medium"
            else:
                return "low"
        # report 
        df_2_total = df_3.merge(df_1,how='inner', on='zipcode')
        new_w = df_2_total.groupby(by=["zipcode"],dropna=False)["week_start"].max().reset_index()
        new_w = new_w.merge(df_2_total,how='left', on=['week_start',"zipcode"])
        new_w["alert"] = new_w["cases_weekly"].apply(lambda x:alert_func_zip(x))
        return new_w