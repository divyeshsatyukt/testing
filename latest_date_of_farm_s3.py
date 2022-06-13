#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  2 12:51:58 2022

@author: divyesh ranpariya
"""

import pymysql
import pandas as pd
import ast
import boto3
import pandas as pd
import os
import datetime


main_dir = "/home/satyukt/Projects/1000/micro/employee/Divyesh"



# reading active client list...
active_client_list=pd.read_csv(os.path.join(main_dir, "data/ActiveUsers_MonthlyData.csv"),usecols=['ClientID']).values.tolist()

active_list=[f[0] for f in active_client_list]



dt1 = datetime.datetime.now()

###############################################################################
# connect database to listout active farms..
print("connect database to listout active farms..")


dbname="sat2farmdb"
user="remote"
password="satelite"
host="34.125.75.120"
port=3306


conn = pymysql.connect(host=host, user=user,port=port,passwd=password, db=dbname)
cur = conn.cursor()

# cur.execute(f"SHOW COLUMNS FROM polygonStore")                                 ## 
# cur.execute(f"select id,clientID,polyinfo from polygonStore where active=1")   ## active = 1, farmid, cilentid, polyinfo
# cur.execute(f"select * from polygonStore")                                     ## All farm's details.
# cur.execute(f"select id,clientID,active from polygonStore")                    ## All farm's details (active category only).
cur.execute(f"select id,clientID,active from polygonStore where active = 1")
# cur.execute(f"select * from polygonStore where active = 1")


res = cur.fetchall()
cur.close()
conn.close()


#---------------------------------------
# Temp

#table1 = list(res)
#df_temp = pd.DataFrame(table1, columns = ['id', 'polyinfo', 'clientID', 'Time', 'area', 'active', 'croptype', 'cropstage', 'sowingdate', 'phone_Number', 'image_1', 'image_2', 'image_3', 'image_4', 'category', 'soil_variety', 'product'])
#df_temp.to_csv("/home/satyukt/Projects/1000/micro/employee/Divyesh/trash/temp.csv",index = False)

#---------------------------------------



fill_df = pd.DataFrame()

for row in res:
    if row[1] in active_list:
        #dict1=ast.literal_eval(row[2])
        #coordinates=dict1['geo_json']['geometry']['coordinates'][0]
        temp_df = pd.DataFrame([{"farm_id" : row[0], "client_id" : row[1]}])    # "coordinates" : coordinates
        fill_df = fill_df.append(temp_df)

print(fill_df)









##-------------
#extra_df = pd.DataFrame(list(res), columns=['polygon_id', 'polyinfo','clientID','Time','area','active','croptype','cropstage','sowingdate','phone_Number','image_1','image_2','image_3','image_4','category','soil_variety','product'])
#
##Storing data in csv based on today's date
#main_info_csv = "/home/satyukt/Projects/1000/micro/employee/Divyesh/data/info.csv"
#extra_df.to_csv(main_info_csv,index = False)
##-------------






###############################################################################

dt2 = datetime.datetime.now()
print(dt2-dt1)





s3 = boto3.resource(
    's3',
    region_name='us-east-1',
    aws_access_key_id= "AKIAXMFIWZ7AO7DFOCVK",
    aws_secret_access_key="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"
)

myBucket = s3.Bucket('data.satyukt.com') # for querying





###############################################################################
## SM

print("SM latest date checking...")


final_df = pd.DataFrame()

for i, row in fill_df.iterrows():
    client_id = row["client_id"]
    farm_id = row["farm_id"]    
    try:
        latest = [x.key for x in myBucket.objects.filter(Prefix=f"sat2farm/{client_id}/{farm_id}/satelite_data/SM/PNG/")]
        tmp = os.path.basename(max(latest))
        temp_df = pd.DataFrame([{"farm_id" : farm_id,
                                 "client_id" : client_id,
                                 "latest_png" : tmp,
                                 "lag_days" : (datetime.date.today() - datetime.date( int(tmp[3:7]), int(tmp[7:9]), int(tmp[9:11]) )).days }])
    except:
        temp_df = pd.DataFrame([{"farm_id" : farm_id,
                                 "client_id" : client_id,
                                 "latest_png" : "0",
                                 "lag_days" : 180}])
    final_df = final_df.append(temp_df)


sm_csv = "data/sm_latest_date.csv"
final_df.to_csv(os.path.join(main_dir, sm_csv), index = False)

print(final_df)
###############################################################################




dt3 = datetime.datetime.now()
print(dt3-dt2)







###############################################################################
## NDVI
print("NDVI latest date checking...")

final_df = pd.DataFrame()
for i, row in fill_df.iterrows():
    client_id = row["client_id"]
    farm_id = row["farm_id"]
    try:
        latest = [x.key for x in myBucket.objects.filter(Prefix=f"sat2farm/{client_id}/{farm_id}/satelite_data/NDVI/PNG/")]
        
        tmp = os.path.basename(max(latest))
        
        temp_df = pd.DataFrame([{"farm_id" : farm_id,
                                 "client_id" : client_id,
                                 "latest_png" : tmp,
                                 "lag_days" : (datetime.date.today() - datetime.date( int(tmp[5:9]), int(tmp[9:11]), int(tmp[11:13]) )).days }])
    except:
        temp_df = pd.DataFrame([{"farm_id" : farm_id,
                                 "client_id" : client_id,
                                 "latest_png" : "0",
                                 "lag_days" : 180}])

    final_df = final_df.append(temp_df)


ndvi_csv = "data/ndvi_latest_date.csv"
final_df.to_csv(os.path.join(main_dir, ndvi_csv), index = False)

print(final_df)
###############################################################################




dt4 = datetime.datetime.now()
print(dt4-dt3)






print("total time taken by the script : ", dt4-dt1)