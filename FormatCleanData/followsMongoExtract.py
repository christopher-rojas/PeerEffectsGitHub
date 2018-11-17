#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 26 09:23:15 2017

@author: christopherrojas

Take the follows data from the db and put into .csv files.
"""

import logging
# Set up logging
logging.basicConfig(filename='follows.log',level=logging.INFO,format='%(asctime)s %(message)s')

import pandas as pd
from pymongo import MongoClient
import unicodecsv as csv

def Follows(periods):
    """
    Find the follow events and put into a .csv file.
    Input: A date range of periods.
    """
    # Access the DB
    # Enter the host address
    MONGO_HOST = ""
    MONGO_DB = "GitHub_Archive"
    connection = MongoClient(MONGO_HOST)
    db = connection[MONGO_DB]
    # List in which to store follows
    data = []
    for period in periods:
        collection = db['GitHubArchive'+'-'+str(period.month)+'-'+str(period.year)]
        condition1 = (period.year<2012)or((period.year==2012)and(period.month<3))
        # Prior to March, 2012
        if condition1:
            pipeline = [
            { "$match" : { "type":"FollowEvent"} },     
            { "$project" :{ 
                "_id":0,
                "created_at":1,
                "alogin":"$actor.login",
                "tlogin":"$payload.target.login"}
                }          
            ]
            data1 = list(collection.aggregate(pipeline))
            for item in data1:            
                item['name'] = ''
                item['location'] = ''
                item['company'] = ''
                
            data += data1
            
        # March, 2012 
        elif (period.year==2012)and(period.month==3):
            
            pipeline1 = [
            { "$match" : { "type":"FollowEvent", "actor":{"$type":3} } },     
            { "$project" :{ 
                "_id":0,
                "created_at":1,
                "alogin":"$actor.login",
                "tlogin":"$payload.target.login"}
                }          
            ]
            data1 = list(collection.aggregate(pipeline1))
            for item in data1:            
                item['name'] = ''
                item['location'] = ''
                item['company'] = ''
            data += data1
            
            pipeline2 = [
            { "$match" : { "type":"FollowEvent", "actor":{"$type":2} } },     
            { "$project" :{ 
                "_id":0,
                "created_at":1,
                "alogin":"$actor",
                "tlogin":"$payload.target.login",
                "name":"$actor_attributes.name",
                "location":"$actor_attributes.location",
                "company":"$actor_attributes.company"}
                }          
            ]   
            data2 = list(collection.aggregate(pipeline2))
            data += data2
        # After March, 2012   
        else:
            pipeline = [
            { "$match" : { "type":"FollowEvent"} },     
            { "$project" :{ 
                "_id":0,
                "created_at":1,
                "alogin":"$actor",
                "tlogin":"$payload.target.login",
                "name":"$actor_attributes.name",
                "location":"$actor_attributes.location",
                "company":"$actor_attributes.company"}
                }          
            ]            
            data += list(collection.aggregate(pipeline))
            
        logging.info('Finished follows in period: ' + str(period))
    # Save the data to a .csv.        
    keys = ['created_at','alogin','tlogin','name','location','company']
    with open('follows.csv', 'wb') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

    logging.info('Created follows.csv.')
    
periods = pd.date_range(start='2011-02-01',end='2013-10-31',freq='M')
print periods
Follows(periods)
