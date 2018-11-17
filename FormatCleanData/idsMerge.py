#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri May  5 14:31:16 2017

@author: christopherrojas

Merge the ids into each of the events.
"""
import pandas as pd
import numpy as np

data_dir = ""

print "load IDs"

userIDs = pd.read_csv(data_dir+'userID_Mapping.csv')
repoIDs = pd.read_csv(data_dir+'repoID_Mapping.csv')
user_repo_events = ['stars']

for event_type_name in user_repo_events:
    
    print event_type_name
    event_type = pd.read_csv(data_dir+str(event_type_name)+'.csv')
    event_type = pd.merge(event_type,userIDs,on='alogin',how='left')
    event_type = pd.merge(event_type,repoIDs,on ='repo',how='left')
    event_type.drop(['alogin','repo'],axis=1,inplace=True)
    event_type.to_csv(data_dir+str(event_type_name)+'.csv',index=False)
    event_type = np.nan
    
    print event_type_name + "_first"
    event_type_first = pd.read_csv(data_dir+event_type_name+'_first.csv')
    event_type_first = pd.merge(event_type_first,userIDs,on='alogin',how='left')
    event_type_first = pd.merge(event_type_first,repoIDs,on ='repo',how='left')
    event_type_first.drop(['alogin','repo'],axis=1,inplace=True)
    event_type_first.to_csv(data_dir+str(event_type_name)+'_first.csv',index=False)
    event_type_first = np.nan 

print "follows"
follows = pd.read_csv(data_dir+'follows.csv')
userIDs.rename(columns={'userID':'auserID'},inplace=True)
follows = pd.merge(follows,userIDs,on='alogin',how='left')
userIDs.rename(columns={'alogin':'tlogin','auserID':'tuserID'},inplace=True)
follows = pd.merge(follows,userIDs,on='tlogin',how='left')
userIDs.rename(columns={'tlogin':'alogin','tuserID':'userID'},inplace=True)
follows.drop(['alogin','tlogin'],axis=1,inplace=True)
follows.to_csv(data_dir+'follows.csv',index=False)
follows = np.nan

print "follows_first"
follows_first = pd.read_csv(data_dir+'follows_first.csv')
userIDs.rename(columns={'userID':'auserID'},inplace=True)
follows_first = pd.merge(follows_first,userIDs,on='alogin',how='left')
userIDs.rename(columns={'alogin':'tlogin','auserID':'tuserID'},inplace=True)
follows_first = pd.merge(follows_first,userIDs,on='tlogin',how='left')
userIDs.rename(columns={'tlogin':'alogin','tuserID':'userID'},inplace=True)
follows_first.drop(['alogin','tlogin'],axis=1,inplace=True)

# Build the ids for dyad
dyads = np.arange(0,len(follows_first),1)
follows_first['dyadID'] = dyads
follows_first['dyadID'] = follows_first['dyadID'].astype('int')

follows_first.to_csv(data_dir+'follows_first.csv',index=False)
follows_first = np.nan
