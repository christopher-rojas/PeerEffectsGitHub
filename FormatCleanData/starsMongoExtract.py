#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 26 09:27:09 2017

@author: christopherrojas

Take the stars data from the db and put into .csv files.
"""

import logging
# Set up logging
logging.basicConfig(filename='stars.log',level=logging.INFO,format='%(asctime)s %(message)s')

import pandas as pd
from pymongo import MongoClient
import unicodecsv as csv

def Stars(periods):
    """
    Find the star events and put into a .csv file.
    Input: A date range of periods.    
    """
    # Access the DB
    # Enter the host address
    MONGO_HOST = ""
    MONGO_DB = "GitHub_Archive"
    connection = MongoClient(MONGO_HOST)
    db = connection[MONGO_DB]
        
    for period in periods:
        # List in which to store push events.
        data = []

        collection = db['GitHubArchive'+'-'+str(period.month)+'-'+str(period.year)]
        condition1 = (period.year<2012)or((period.year==2012)and(period.month<3))
        # Prior to March, 2012
        if condition1:
            pipeline = [
            { "$match" : { "type":"WatchEvent"} },     
            { "$project" :{ 
                "_id":0,
                "created_at":1,
                "alogin":"$actor.login",
                "repo1":"$payload.repo",
                "repo2":"$repo.name"}
                }          
            ]
            
            data1 = list(collection.aggregate(pipeline))
            # Here you need to deal with missing repos.
            for item in data1:
                if ('repo1' in item.keys()):
                    if (len(item['repo1'])>1):
                        item['repo']=item['repo1']
                    del item['repo1']
                if ('repo2' in item.keys()):
                    if (len(item['repo2'])>1):
                        item['repo']=item['repo2']
                    del item['repo2']
                if 'repo' not in item.keys():
                    item['repo']=''
                #item['stars']=''
                item['repo_created_at'] = ''
                item['repo_language'] = ''
                item['name'] = ''
                item['location'] = ''
                item['company'] = ''
                

            data += data1
        # March, 2012     
        elif (period.year==2012)and(period.month==3):
            
            pipeline1 = [
            { "$match" : { "type":"WatchEvent", "actor":{"$type":3} } },  
            { "$project" :{ 
                "_id":0,
                "created_at":1,
                "alogin":"$actor.login",
                "repo1":"$payload.repo",
                "repo2":"$repo.name"}
                }          
            ]
            data1 = list(collection.aggregate(pipeline1))
            
            for item in data1:             
                if ('repo1' in item.keys()):
                    if (len(item['repo1'])>1):
                        item['repo']=item['repo1']
                    del item['repo1']
                if ('repo2' in item.keys()):
                    if (len(item['repo2'])>1):
                        item['repo']=item['repo2']
                    del item['repo2']
                if 'repo' not in item.keys():
                    item['repo']=''
                #item['stars']=''
                item['repo_created_at'] = ''
                item['repo_language'] = ''
                item['name'] = ''
                item['location'] = ''
                item['company'] = ''

            data += data1
            
            pipeline2 = [
            { "$match" : { "type":"WatchEvent", "actor":{"$type":2}, "repository.url": {"$exists": True} } },     
            { "$project" :{ 
                "_id":0,
                "created_at":1,
                "alogin":"$actor",
                "url":1,
                "repo_created_at":"$repository.created_at",
                "repo_language":"$repository.language",
                "name":"$actor_attributes.name",
                "location":"$actor_attributes.location",
                "company":"$actor_attributes.company"}
                }          
            ]   
            data2 = list(collection.aggregate(pipeline2))
            
            for item in data2:
                split = item['url'].split("/")
                repo = split[3] + "/" + split[4]
                item['repo'] = repo
                del item['url']

            data += data2
        # After March, 2012    
        else:
            pipeline = [
            { "$match" : { "type":"WatchEvent", "repository.url": {"$exists": True} } },     
            { "$project" :{ 
                "_id":0,
                "created_at":1,
                "alogin":"$actor",
                "url":1,
                "repo_created_at":"$repository.created_at",
                "repo_language":"$repository.language",
                "name":"$actor_attributes.name",
                "location":"$actor_attributes.location",
                "company":"$actor_attributes.company"}
                }          
            ]
            data2 = list(collection.aggregate(pipeline))
            
            for item in data2:
                split = item['url'].split("/")
                repo = split[3] + "/" + split[4]
                item['repo'] = repo
                del item['url']
          
            data += data2
        
        logging.info('Finished stars in period: ' + str(period))
        # Save the data to a .csv.            
        keys = ['created_at','alogin','repo','repo_created_at','repo_language','name','location','company']
        with open('stars'+'-'+str(period.month)+'-'+str(period.year)+'.csv', 'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)
        
        logging.info('Created stars.csv.')
        
periods = pd.date_range(start='2011-02-01',end='2013-10-31',freq='M')
Stars(periods)
