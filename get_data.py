import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 
#from psycopg2 import extras as ex
import requests
import pandas as pd
import numpy as np
from tqdm import tqdm
import re
import time
import geocoder

def get_data_star(wait_hour):
    sle_time = int(wait_hour)*60*60
    while(True):
        # set google api key
        api_key = "AIzaSyBYgX8z-i7rjcnizmhfmpgeByBjkYHLj_g"
        # design the required attributes from json
        TaxiTripsJsonRecords = {
            "trip_id":"Trip_id"
            ,"trip_start_timestamp":"Trip_start_timestamp"
            ,"trip_end_timestamp":"Trip_end_timestamp"
            ,"pickup_centroid_latitude":"Pickup_centroid_latitude"
            ,"pickup_centroid_longitude":"Pickup_centroid_longitude"
            ,"dropoff_centroid_latitude":"Dropoff_centroid_latitude"
            ,"dropoff_centroid_longitude":"Dropoff_centroid_longitude"
        }
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        print("GetTaxiTrips: Collecting Taxi Trips Data")
        # drop the table if exist
        cursor.execute('''drop table if exists taxi_trips''')
        db_connection.commit()
        create_table = (
            """
        CREATE TABLE IF NOT EXISTS "taxi_trips" (
                        "id"   SERIAL , 
                        "trip_id" VARCHAR(255) UNIQUE, 
                        "trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
                        "trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
                        "pickup_centroid_latitude" DOUBLE PRECISION, 
                        "pickup_centroid_longitude" DOUBLE PRECISION, 
                        "dropoff_centroid_latitude" DOUBLE PRECISION, 
                        "dropoff_centroid_longitude" DOUBLE PRECISION, 
                        "pickup_zip_code" VARCHAR(255), 
                        "dropoff_zip_code" VARCHAR(255), 
                        PRIMARY KEY ("id") 
                    )
            """)
        # create the table
        cursor.execute(create_table)
        db_connection.commit()
        # get the data
        #url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"
        url = "https://data.cityofchicago.org/api/id/wrvz-psew.json?$query=select%20*%2C%20%3Aid%20order%20by%20%60trip_start_timestamp%60%20desc%20limit%202000"
        print("try to connect")
        # requests
        #r = requests.get('https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500')
        r = requests.get(url)
        # get json
        r_json_content = r.json()
        print("successfully connect")
        # extract the attrs from the json
        content_list = []
        for i in r_json_content:
            break_log = 0
            tmp_dict = {}
            for j in list(TaxiTripsJsonRecords.keys()):
                try:
                    tmp_dict[TaxiTripsJsonRecords[j]] = i[j]
                except:
                    break_log = 1
                    break
            if break_log == 0:
                content_list.append(tmp_dict)
        # delete the info if useless
        content_list_after_delete = []
        for i in content_list:
            if (len(i['Trip_start_timestamp'])<23) or (len(i['Trip_end_timestamp'])<23):
                pass
            elif "" in list(i.values()):
                pass
            else:
                content_list_after_delete.append(i)
        # get zip code
        def get_zip_code(latitude,longitude):
            latlng =  str(latitude)+","+str(longitude)
            params = {
                "key": api_key,
                "latlng":latlng
            }
            base_url = "https://maps.googleapis.com/maps/api/geocode/json?"
            response = requests.get(base_url,params=params).json()
            tmp_con1 = str(response["results"])
            re_con_1 = "(6\d\d\d\d).*?types.*?postal_code"
            regex_start_1 = re.compile(re_con_1)
            re_content_1 = regex_start_1.findall(tmp_con1)
            zip_code = re_content_1[-1]
            return zip_code
        # get zip code
        content_list_after_zipcode = []
        for i in tqdm(content_list_after_delete):
            try:
                i["Pickup_zipcode"] = get_zip_code(i["Pickup_centroid_latitude"],i["Pickup_centroid_longitude"])
                i["Dropoff_zipcode"] = get_zip_code(i["Dropoff_centroid_latitude"],i["Dropoff_centroid_longitude"])
                content_list_after_zipcode.append(i)
            except:
                continue
        # values = [(pre_id,'d5e65b4514cd47fa97de827832c4942ca50064af', '2022-03-01T00:00:00.000', '2022-03-01T00:15:00.000',
        #             '41.899602111', '-87.633308037', '41.944226601', '-87.655998182', '60610', '60657')]
        # insert into postgre
        pre_id = 1
        insert_list = []
        for j in content_list_after_zipcode:
            item_1 = tuple([pre_id]+list(j.values()))
            insert_list.append(item_1)
            pre_id = pre_id+1
        # insert into postgre
        args =  ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", i).decode('utf-8')
                        for i in insert_list)

        cursor.execute("INSERT INTO taxi_trips VALUES " + (args))
        db_connection.commit()
        # design the required attributes from json
        TaxiTripsJsonRecords_network = {
            "trip_id":"Trip_id"
            ,"trip_start_timestamp":"Trip_start_timestamp"
            ,"trip_end_timestamp":"Trip_end_timestamp"
            ,"pickup_centroid_latitude":"Pickup_centroid_latitude"
            ,"pickup_centroid_longitude":"Pickup_centroid_longitude"
            ,"dropoff_centroid_latitude":"Dropoff_centroid_latitude"
            ,"dropoff_centroid_longitude":"Dropoff_centroid_longitude"
        }
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        print("GetTaxiTrips: Collecting Taxi Trips Data_Network")
        # drop the table if exist
        cursor.execute('''drop table if exists taxi_trips_network''')
        db_connection.commit()
        create_table = (
            """
        CREATE TABLE IF NOT EXISTS "taxi_trips_network" (
                        "id"   SERIAL , 
                        "trip_id" VARCHAR(255) UNIQUE, 
                        "trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
                        "trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
                        "pickup_centroid_latitude" DOUBLE PRECISION, 
                        "pickup_centroid_longitude" DOUBLE PRECISION, 
                        "dropoff_centroid_latitude" DOUBLE PRECISION, 
                        "dropoff_centroid_longitude" DOUBLE PRECISION, 
                        "pickup_zip_code" VARCHAR(255), 
                        "dropoff_zip_code" VARCHAR(255), 
                        PRIMARY KEY ("id") 
                    )
            """)
        # create the table
        cursor.execute(create_table)
        db_connection.commit()
        # get the data
        #url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"
        url = "https://data.cityofchicago.org/resource/m6dm-c72p.json?$query=select%20*%2C%20%3Aid%20order%20by%20%60trip_start_timestamp%60%20desc%20limit%202000"
        print("try to connect")
        # requests
        #r = requests.get('https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500')
        r = requests.get(url)
        # get json
        r_json_content = r.json()
        print("successfully connect")
        # extract the attrs from the json
        content_list = []
        for i in r_json_content:
            break_log = 0
            tmp_dict = {}
            for j in list(TaxiTripsJsonRecords_network.keys()):
                try:
                    tmp_dict[TaxiTripsJsonRecords_network[j]] = i[j]
                except:
                    break_log = 1
                    break
            if break_log == 0:
                content_list.append(tmp_dict)
        # delete the info if useless
        content_list_after_delete = []
        for i in content_list:
            if (len(i['Trip_start_timestamp'])<23) or (len(i['Trip_end_timestamp'])<23):
                pass
            elif "" in list(i.values()):
                pass
            else:
                content_list_after_delete.append(i)
        # get zip code
        def get_zip_code(latitude,longitude):
            latlng =  str(latitude)+","+str(longitude)
            params = {
                "key": api_key,
                "latlng":latlng
            }
            base_url = "https://maps.googleapis.com/maps/api/geocode/json?"
            response = requests.get(base_url,params=params).json()
            tmp_con1 = str(response["results"])
            re_con_1 = "(6\d\d\d\d).*?types.*?postal_code"
            regex_start_1 = re.compile(re_con_1)
            re_content_1 = regex_start_1.findall(tmp_con1)
            zip_code = re_content_1[-1]
            return zip_code
        # get zip code
        content_list_after_zipcode = []
        for i in tqdm(content_list_after_delete):
            try:
                i["Pickup_zipcode"] = get_zip_code(i["Pickup_centroid_latitude"],i["Pickup_centroid_longitude"])
                i["Dropoff_zipcode"] = get_zip_code(i["Dropoff_centroid_latitude"],i["Dropoff_centroid_longitude"])
                content_list_after_zipcode.append(i)
            except:
                continue
        # insert into postgre
        pre_id = 1
        insert_list = []
        for j in content_list_after_zipcode:
            item_1 = tuple([pre_id]+list(j.values()))
            insert_list.append(item_1)
            pre_id = pre_id+1
        # insert into postgre
        args =  ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", i).decode('utf-8')
                        for i in insert_list)

        cursor.execute("INSERT INTO taxi_trips_network VALUES " + (args))
        db_connection.commit()
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        # get taxi_trips data
        select_data = (
            """
                SELECT * FROM "taxi_trips";
            """)
        # create the table
        cursor.execute(select_data)
        result_taxi_trips = cursor.fetchall();
        # get taxi_trips data
        select_data = (
            """
                SELECT * FROM "taxi_trips_network";
            """)
        # create the table
        cursor.execute(select_data)
        result_taxi_trips_network = cursor.fetchall();
        # merge
        new_list_tuple = []
        id_u = 1
        for i in result_taxi_trips:
            new_list_tuple.append(tuple([id_u]+list(i[1:])))
            id_u += 1
        for i in result_taxi_trips_network:
            new_list_tuple.append(tuple([id_u]+list(i[1:])))
            id_u += 1
        insert_list = new_list_tuple
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        # drop the table if exist
        cursor.execute('''drop table if exists taxi_trip_total''')
        db_connection.commit()
        #create table
        create_table = (
            """
        CREATE TABLE IF NOT EXISTS "taxi_trip_total" (
                        "id"   SERIAL , 
                        "trip_id" VARCHAR(255) UNIQUE, 
                        "trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
                        "trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
                        "pickup_centroid_latitude" DOUBLE PRECISION, 
                        "pickup_centroid_longitude" DOUBLE PRECISION, 
                        "dropoff_centroid_latitude" DOUBLE PRECISION, 
                        "dropoff_centroid_longitude" DOUBLE PRECISION, 
                        "pickup_zip_code" VARCHAR(255), 
                        "dropoff_zip_code" VARCHAR(255), 
                        PRIMARY KEY ("id") 
                    )
            """)
        # create the table
        cursor.execute(create_table)
        db_connection.commit()
        # insert into postgre
        args =  ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", i).decode('utf-8')
                        for i in insert_list)

        cursor.execute("INSERT INTO taxi_trip_total VALUES " + (args))
        db_connection.commit()
        # design the required attributes from json
        building_permits_records = {
            "id":"permit_id"
            ,"permit_type":"permit_type"
            ,"total_fee":"total_fee"
            ,"latitude":"latitude"
            ,"longitude":"longitude"
        }
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        print("building_permits: Collecting building_permits")
        # drop the table if exist
        cursor.execute('''drop table if exists building_permits''')
        db_connection.commit()
        create_table = (
            """
        CREATE TABLE IF NOT EXISTS "building_permits" (
                        "id"   SERIAL , 
                        "permit_id" VARCHAR(255) UNIQUE, 
                        "permit_type" VARCHAR(255), 
                        "total_fee" DOUBLE PRECISION,
                        "latitude" VARCHAR(255), 
                        "longitude" VARCHAR(255), 
                        "zipcode" VARCHAR(255), 
                        PRIMARY KEY ("id") 
                    )
            """)
        # create the table
        cursor.execute(create_table)
        db_connection.commit()
        # get the data
        #url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"
        url = "https://data.cityofchicago.org/api/id/building-permits.json?$query=select%20*%2C%20%3Aid%20order%20by%20%60issue_date%60%20desc%20limit%201000"
        print("try to connect")
        # requests
        #r = requests.get('https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500')
        r = requests.get(url)
        # get json
        r_json_content = r.json()
        print("successfully connect")
        # extract the attrs from the json
        content_list = []
        for i in r_json_content:
            break_log = 0
            tmp_dict = {}
            for j in list(building_permits_records.keys()):
                try:
                    tmp_dict[building_permits_records[j]] = i[j]
                except:
                    break_log = 1
                    break
            if break_log == 0:
                content_list.append(tmp_dict)
        # delete the info if useless
        content_list_after_delete = []
        for i in content_list:
        #     if (i["permit_type"] != "PERMIT - NEW CONSTRUCTION"):
        #         pass
            if "" in list(i.values()):
                pass
            else:
                content_list_after_delete.append(i)
        # get zip code
        def get_zip_code(latitude,longitude):
            latlng =  str(latitude)+","+str(longitude)
            params = {
                "key": api_key,
                "latlng":latlng
            }
            base_url = "https://maps.googleapis.com/maps/api/geocode/json?"
            response = requests.get(base_url,params=params).json()
            tmp_con1 = str(response["results"])
            re_con_1 = "(6\d\d\d\d).*?types.*?postal_code"
            regex_start_1 = re.compile(re_con_1)
            re_content_1 = regex_start_1.findall(tmp_con1)
            zip_code = re_content_1[-1]
            return zip_code
        # get zip code
        content_list_after_zipcode = []
        for i in tqdm(content_list_after_delete):
            try:
                i["zipcode"] = get_zip_code(i["latitude"],i["longitude"])
                content_list_after_zipcode.append(i)
            except:
                continue
        # insert into postgre
        pre_id = 1
        insert_list = []
        for j in content_list_after_zipcode:
            item_1 = tuple([pre_id]+list(j.values()))
            insert_list.append(item_1)
            pre_id = pre_id+1
        # insert into postgre
        args =  ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s)", i).decode('utf-8')
                        for i in insert_list)

        cursor.execute("INSERT INTO building_permits VALUES " + (args))
        db_connection.commit()
        # design the required attributes from json
        public_health_statistics_records = {
            "community_area_name":"community_area_name"
            ,"below_poverty_level":"below_poverty_level"
            ,"per_capita_income":"per_capita_income "
            ,"unemployment":"unemployment"
        }
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        print("public_health_statistics: Collecting public_health_statistics")
        # drop the table if exist
        cursor.execute('''drop table if exists public_health_statistics''')
        db_connection.commit()
        create_table = (
            """
        CREATE TABLE IF NOT EXISTS "public_health_statistics" (
                        "id"   SERIAL , 
                        "community_area_name" VARCHAR(255) UNIQUE, 
                        "below_poverty_level" DOUBLE PRECISION, 
                        "per_capita_income" DOUBLE PRECISION,
                        "unemployment" DOUBLE PRECISION, 
                        PRIMARY KEY ("id") 
                    )
            """)
        # create the table
        cursor.execute(create_table)
        db_connection.commit()
        # get the data
        #url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"
        url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=200"
        print("try to connect")
        # requests
        #r = requests.get('https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500')
        r = requests.get(url)
        # get json
        r_json_content = r.json()
        print("successfully connect")
        # extract the attrs from the json
        content_list = []
        for i in r_json_content:
            break_log = 0
            tmp_dict = {}
            for j in list(public_health_statistics_records.keys()):
                try:
                    tmp_dict[public_health_statistics_records[j]] = i[j]
                except:
                    break_log = 1
                    break
            if break_log == 0:
                content_list.append(tmp_dict)
        # delete the info if useless
        content_list_after_delete = []
        for i in content_list:
        #     if (i["permit_type"] != "PERMIT - NEW CONSTRUCTION"):
        #         pass
            if "" in list(i.values()):
                pass
            else:
                content_list_after_delete.append(i)
        content_list_after_zipcode = content_list_after_delete
        # insert into postgre
        pre_id = 1
        insert_list = []
        for j in content_list_after_zipcode:
            item_1 = tuple([pre_id]+list(j.values()))
            insert_list.append(item_1)
            pre_id = pre_id+1
        # insert into postgre
        args =  ','.join(cursor.mogrify("(%s,%s,%s,%s,%s)", i).decode('utf-8')
                        for i in insert_list)

        cursor.execute("INSERT INTO public_health_statistics VALUES " + (args))
        db_connection.commit()
        # design the required attributes from json
        covid_daily_case_records = {
            "lab_report_date":"lab_report_date"
            ,"cases_total":"cases_total"
            ,"deaths_total":"deaths_total "
        }
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        print("covid_daily_case: Collecting covid_daily_case")
        # drop the table if exist
        cursor.execute('''drop table if exists covid_daily_case''')
        db_connection.commit()
        create_table = (
            """
        CREATE TABLE IF NOT EXISTS "covid_daily_case" (
                        "id"   SERIAL , 
                        "lab_report_date" TIMESTAMP WITH TIME ZONE, 
                        "cases_total" DOUBLE PRECISION, 
                        "deaths_total" DOUBLE PRECISION,
                        PRIMARY KEY ("id") 
                    )
            """)
        # create the table
        cursor.execute(create_table)
        db_connection.commit()
        # get the data
        #url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"
        url = "https://data.cityofchicago.org/resource/naz8-j4nc.json?$limit=200"
        print("try to connect")
        # requests
        #r = requests.get('https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500')
        r = requests.get(url)
        # get json
        r_json_content = r.json()
        print("successfully connect")
        # extract the attrs from the json
        content_list = []
        for i in r_json_content:
            break_log = 0
            tmp_dict = {}
            for j in list(covid_daily_case_records.keys()):
                try:
                    tmp_dict[covid_daily_case_records[j]] = i[j]
                except:
                    break_log = 1
                    break
            if break_log == 0:
                content_list.append(tmp_dict)
        # delete the info if useless
        content_list_after_delete = []
        for i in content_list:
        #     if (i["permit_type"] != "PERMIT - NEW CONSTRUCTION"):
        #         pass
            if "" in list(i.values()):
                pass
            else:
                content_list_after_delete.append(i)
        content_list_after_zipcode = content_list_after_delete
        # insert into postgre
        pre_id = 1
        insert_list = []
        for j in content_list_after_zipcode:
            item_1 = tuple([pre_id]+list(j.values()))
            insert_list.append(item_1)
            pre_id = pre_id+1
        # insert into postgre
        args =  ','.join(cursor.mogrify("(%s,%s,%s,%s)", i).decode('utf-8')
                        for i in insert_list)

        cursor.execute("INSERT INTO covid_daily_case VALUES " + (args))
        db_connection.commit()
        # design the required attributes from json
        covid_weekly_case_records = {
            "zip_code":"zip_code"
            ,"week_number":"week_number"
            ,"week_start":"week_start"
            ,"week_end":"week_end"
             ,"cases_weekly":"cases_weekly"
             ,"cases_cumulative":"cases_cumulative"
        }
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        print("covid_weekly_case: Collecting covid_weekly_case")
        # drop the table if exist
        cursor.execute('''drop table if exists covid_weekly_case''')
        db_connection.commit()
        create_table = (
            """
        CREATE TABLE IF NOT EXISTS "covid_weekly_case" (
                        "id"   SERIAL , 
                        "zip_code" VARCHAR(255), 
                        "week_number" DOUBLE PRECISION, 
                        "week_start" TIMESTAMP WITH TIME ZONE,
                        "week_end" TIMESTAMP WITH TIME ZONE,
                        "cases_weekly" DOUBLE PRECISION,
                        "cases_cumulative" DOUBLE PRECISION,

                        PRIMARY KEY ("id") 
                    )
            """)
        # create the table
        cursor.execute(create_table)
        db_connection.commit()
        # get the data
        #url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"
        url = "https://data.cityofchicago.org/api/id/yhhz-zm2v.json?$query=select%20*%2C%20%3Aid%20order%20by%20%60week_start%60%20desc%20limit%20500"
        print("try to connect")
        # requests
        #r = requests.get('https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500')
        r = requests.get(url)
        # get json
        r_json_content = r.json()
        print("successfully connect")
        # extract the attrs from the json
        content_list = []
        for i in r_json_content:
            break_log = 0
            tmp_dict = {}
            for j in list(covid_weekly_case_records.keys()):
                try:
                    tmp_dict[covid_weekly_case_records[j]] = i[j]
                except:
                    break_log = 1
                    break
            if break_log == 0:
                content_list.append(tmp_dict)
        # delete the info if useless
        content_list_after_delete = []
        for i in content_list:
            if (len(i['week_start'])<23) or (len(i['week_end'])<23):
                pass
            elif "" in list(i.values()):
                pass
            else:
                content_list_after_delete.append(i)
        content_list_after_zipcode = content_list_after_delete
        # insert into postgre
        pre_id = 1
        insert_list = []
        for j in content_list_after_zipcode:
            item_1 = tuple([pre_id]+list(j.values()))
            insert_list.append(item_1)
            pre_id = pre_id+1
        # insert into postgre
        args =  ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s)", i).decode('utf-8')
                        for i in insert_list)

        cursor.execute("INSERT INTO covid_weekly_case VALUES " + (args))
        db_connection.commit()
        # design the required attributes from json
        covid_ccvi_case_records = {
            "community_area_name":"community_area_name"
            ,"ccvi_score":"ccvi_score"
            ,"ccvi_category":"ccvi_category"
        }
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        print("covid_weekly_case: Collecting covid_ccvi")
        # drop the table if exist
        cursor.execute('''drop table if exists covid_ccvi''')
        db_connection.commit()
        create_table = (
            """
        CREATE TABLE IF NOT EXISTS "covid_ccvi" (
                        "id"   SERIAL , 
                        "community_area_name" VARCHAR(255), 
                        "ccvi_score" DOUBLE PRECISION, 
                        "ccvi_category" VARCHAR(255),

                        PRIMARY KEY ("id") 
                    )
            """)
        # create the table
        cursor.execute(create_table)
        db_connection.commit()
        # get the data
        #url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"
        url = "https://data.cityofchicago.org/resource/xhc6-88s9.json"
        print("try to connect")
        # requests
        #r = requests.get('https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500')
        r = requests.get(url)
        # get json
        r_json_content = r.json()
        print("successfully connect")
        # extract the attrs from the json
        content_list = []
        for i in r_json_content:
            break_log = 0
            tmp_dict = {}
            for j in list(covid_ccvi_case_records.keys()):
                try:
                    tmp_dict[covid_ccvi_case_records[j]] = i[j]
                except:
                    break_log = 1
                    break
            if break_log == 0:
                content_list.append(tmp_dict)
        # delete the info if useless
        content_list_after_delete = []
        for i in content_list:
            if "" in list(i.values()):
                pass
            else:
                content_list_after_delete.append(i)
        content_list_after_zipcode = content_list_after_delete
        # insert into postgre
        pre_id = 1
        insert_list = []
        for j in content_list_after_zipcode:
            item_1 = tuple([pre_id]+list(j.values()))
            insert_list.append(item_1)
            pre_id = pre_id+1
        # insert into postgre
        args =  ','.join(cursor.mogrify("(%s,%s,%s,%s)", i).decode('utf-8')
                        for i in insert_list)

        cursor.execute("INSERT INTO covid_ccvi VALUES " + (args))
        db_connection.commit()
        community_list = ['Ashburn', 'Rogers Park', 'Lake View', 'Jefferson Park', 'Archer Heights', 'South Shore', 'Austin', 'Grand Boulevard', 'West Garfield Park', 'West Pullman', 'Portage Park', 'Avondale', 'Burnside*', 'Brighton Park', 'South Lawndale', 'West Elsdon', 'West Ridge', 'Morgan Park', 'Lincoln Park', 'New City', 'North Lawndale', 'Kenwood', 'Auburn Gresham', 'Logan Square', 'Armour Square', 'Chicago Lawn', 'Norwood Park', 'Riverdale', 'Albany Park', 'Woodlawn', 'Mount Greenwood', 'Uptown', 'Near South Side', 'Douglas', 'South Chicago', 'West Lawn', 'Montclare', 'Humboldt Park', 'Avalon Park', 'Bridgeport', 'Near West Side', 'Beverly', 'North Park', 'Hegewisch', 'Pullman', 'Edgewater', 'Near North Side', 'Washington Park', 'West Englewood', 'Lincoln Square', 'Oakland', 'Roseland', 'Clearing', 'Belmont Cragin', 'South Deering', 'Englewood', 'Dunning', 'Loop', 'Forest Glen', 'Washington Heights', 'Chatham', 'Hyde Park', 'East Garfield Park', 'Garfield Ridge', 'West Town', 'Edison Park', "O'Hare", 'McKinley Park', 'Fuller Park*', 'Lower West Side', 'East Side', 'Hermosa', 'North Center', 'Calumet Heights', 'Irving Park', 'Greater Grand Crossing', 'Gage Park']
        print(len(community_list))
        df_loc_1 = pd.read_excel("./geo_loc.xlsx",sheet_name="Sheet1")
        df_loc_2 = pd.read_excel("./geo_loc.xlsx",sheet_name="Sheet2")
        df_list = []
        for i in range(61):
            b_list = df_loc_2.iloc[i].tolist()
            c_list = b_list[1].split(", ")
            for c in c_list:
                if c in community_list:
                    df_list.append([b_list[0],c])
        df_k = pd.DataFrame(df_list)
        df_k.columns=["zipcode","community"]
        df_to = df_k.merge(df_loc_1,how='inner', on='community')
        df_to = df_to.reset_index()
        df_to.columns=["geo_id","zipcode","community","neighborhood"]
        # design the required attributes from json
        neighborhood_community_zip_records = {
            "geo_id":"geo_id"
            ,"zipcode":"zipcode"
            ,"community":"community"
            ,"neighborhood":"neighborhood"
        }
        # connect to the postgresql
        db_connection = psycopg2.connect(host='127.0.0.1',dbname="chicago_business_intelligence", user="postgres" , password="12345")
        cursor = db_connection.cursor()
        print("neighborhood_community_zip: Collecting neighborhood_community_zip")
        # drop the table if exist
        cursor.execute('''drop table if exists neighborhood_community_zip''')
        db_connection.commit()
        create_table = (
            """
        CREATE TABLE IF NOT EXISTS "neighborhood_community_zip" (
                        "id"   SERIAL , 
                        "geo_id" VARCHAR(255), 
                        "zipcode" DOUBLE PRECISION, 
                        "community" VARCHAR(255),
                        "neighborhood" VARCHAR(255),

                        PRIMARY KEY ("id") 
                    )
            """)
        # create the table
        cursor.execute(create_table)
        db_connection.commit()
        # insert into postgre
        pre_id = 1
        insert_list = []
        for i in range(len(df_to)):
            item_1 = tuple([pre_id]+[int(df_to.iloc[i].tolist()[0].item()),
                                     int(df_to.iloc[i].tolist()[1].item()),
                                     str(df_to.iloc[i].tolist()[2]),
                                        str(df_to.iloc[i].tolist()[3])])
            insert_list.append(item_1)
            pre_id = pre_id+1
        # insert into postgre
        args =  ','.join(cursor.mogrify("(%s,%s,%s,%s,%s)", i).decode('utf-8')
                        for i in insert_list)

        cursor.execute("INSERT INTO neighborhood_community_zip VALUES " + (args))
        db_connection.commit()

        time.sleep(sle_time)
    
    return "ok"