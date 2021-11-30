# -*- coding: utf-8 -*-
"""
Created on Tue Nov 30 09:51:31 2021

@author: Data
"""


import os
import googlemaps
from datetime import datetime
import json
import pandas as pd
import re


# get the data that was previously scrapped from daft.ie see https://github.com/gerryof/Daft_Web_Scrapper
path = r"E:\Daft_Web_Scrapping\Data"

files = os.listdir(path)

dflist = []


for file in files:
    df = pd.read_csv(os.path.join(path , file))
    date = file.replace('.csv' , '')
    df['extract_date'] = pd.to_datetime(date)
    dflist.append(df)

data = pd.concat(dflist , ignore_index=True , axis =0 , sort=True)

data = data.drop(['Unnamed: 0' , 'urls' , 'pgps' ] , axis =1)

# 30/11/2021 the web scrapper incorrectly made the longtitude value positive. This workaround is to handle historical data
# but the script has been corrected. It also only works because I know all of the longtidue values for galway are negative.

data['long'] = data[(data['long'] > 0)].long * -1

# pull api key from startup variable 
api_key = os.environ.get("google_maps_api_key")

places = googlemaps.Client(key=api_key)

lat_lon = data[['lat', 'long']].drop_duplicates()

googled_col = ['lat' , 'long' , 'superm_count' , 'pub_count', 'restaurant_count' , 'school_count' , 'university_count' , 'dist_city_center_sec']
googled = []

city_center = (53.272536, -9.053182 )

for index, i in lat_lon.iterrows():

    try:
        supermarket = places.places_nearby((i["lat"],i["long"]), radius=1000, type="supermarket")
        supermarket_no = len(supermarket["results"])
    except:
        supermarket_no = None
    # this might be saying something about ireland but ever spot was hitting the max number of results for pubs within 1km :P
    try:
        pub = places.places_nearby((i["lat"],i["long"]), radius=200, type="pub")
        pub_no = len(pub["results"])
    except:
        pub_no = None
    
    try:    
        restr = places.places_nearby((i["lat"],i["long"]), radius=1000, type="restaurant")
        restr_no = len(restr["results"])
    except:
        restr_no = None
    
    try:    
        school = places.places_nearby((i["lat"],i["long"]), radius=1000, type="school")
        school_no = len(school["results"])
    except:
        school_no = None 
        
    try:
        uni = places.places_nearby((i["lat"],i["long"]), radius=1000, type="university")
        uni_count = len(uni["results"])
    except:
        uni_count = None
      
    try:    
        to_city_center = places.directions(city_center,(i["lat"], i["long"]), mode="transit")
        dist_city_center_sec = to_city_center[0]["legs"][0]["duration"]["value"]
    except:
        dist_city_center_sec = None
    
    googled.append([i["lat"], i["long"] , supermarket_no , pub_no , restr_no , school_no , uni_count , dist_city_center_sec])
    
    
googled_df = pd.DataFrame(googled)    
googled_df.columns = googled_col

file = r"E:\Google Places API\data\google_maps_dist_places.csv"

try:
    os.remove(file)
except:
    pass

googled_df.to_csv(file)

