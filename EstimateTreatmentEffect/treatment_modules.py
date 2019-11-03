# -*- coding: utf-8 -*-
"""
Created on Thu Mar 22 16:51:31 2018

@author: christopherrojas
"""
import pandas as pd
import numpy as np
import os.path

## Events Functions
def convertDates(data,frequency,exact,zeroHour='2013-01-01 00:00:00'):
    convert = pd.DataFrame()
    convert['created_at'] = list(data)
    convert['created_at'].fillna('1970-01-01T00:00:00Z',inplace=True)
    convert['created_at'] = convert['created_at'].astype('datetime64[s]')   
    convert['zeroHour'] = zeroHour
    convert['zeroHour'] = convert['zeroHour'].astype('datetime64[s]')
    convert['cperiod'] = convert['created_at'] - convert['zeroHour']
    convert['cperiod'] = convert['cperiod'] / np.timedelta64(frequency[0], frequency[1])
    if exact==True:    
        convert['cperiod'] = 1+convert['cperiod'].round(6)
    else:
        convert['cperiod'] = 1+np.floor(convert['cperiod'].round(12))
    return list(convert['cperiod'])

def loadFollows(events_directory,exact=False):
    follows = pd.read_csv(events_directory+"FollowEvent"+'.csv')
    follows['created_at'] = convertDates(follows['created_at'],[1,'M'],exact)
    follows = follows[follows.auserID!=6]
    follows = follows[follows.tuserID!=6]
    return follows
    
def loadEvents(behavior_num,events_directory,exact=False):
    behavior_types = {0: 'WfcEvent', 
                               1: 'WatchEvent', 
                               2: 'ForkEvent', 
                               3: 'ContributionEvent',
                               4: 'WfEvent'}
    behavior = behavior_types[behavior_num]
    events = pd.read_csv(events_directory+behavior+'.csv') 
    events['created_at'] = convertDates(events['created_at'],[1,'M'],exact)
    events = events[(events.repoID>0)&(events.userID!=6)]
    return events
    
def loadExitsUsers(exits_directory,time_after,exact=False):
    exits = pd.read_csv(exits_directory+'exitDatesUsers.csv')
    exits['exited_at'] = convertDates(exits['exited_at'],[1,'M'],exact)
    exits['exited_at'] = exits['exited_at'] + time_after   
    return exits
    
def loadExitsRepos(exits_directory,time_after,exact=False):
    exits = pd.read_csv(exits_directory+'exitDatesRepos.csv')
    exits['exited_at'] = convertDates(exits['exited_at'],[1,'M'],exact)
    exits['exited_at'] = exits['exited_at'] + time_after   
    return exits

def loadExperienceUsers(joins_directory,current_period,exact=False):
    joins = pd.read_csv(joins_directory+'joinDatesUsers.csv')
    joins['joined_at'] = convertDates(joins['joined_at'],[1,'M'],exact)
    joins['experience'] = current_period - joins['joined_at']
    
    return joins[['userID','experience']]
    
## SVD Functions 
def loadItemMapping(period,behavior_num,svd_directory):
    behavior_types = {0: 'WfcEvent', 
                               1: 'WatchEvent', 
                               2: 'ForkEvent', 
                               3: 'ContributionEvent',
                               4: 'WfEvent'}
    behavior = behavior_types[behavior_num]
    itemMapping = pd.read_csv(svd_directory+behavior+"/"+str(period)+"/"+'itemMapping.csv',header=None,delim_whitespace=True)
    itemMapping.columns = ['MMLrepoID','repoID']
    itemMapping['repoID'] = itemMapping['repoID'].astype('int')
    itemMapping['MMLrepoID'] = itemMapping['MMLrepoID'].astype('int')
    return itemMapping
    
def loadUserMapping(period,behavior_num,svd_directory):
    behavior_types = {0: 'WfcEvent', 
                               1: 'WatchEvent', 
                               2: 'ForkEvent', 
                               3: 'ContributionEvent',
                               4: 'WfEvent'}
    behavior = behavior_types[behavior_num]
    userMapping = pd.read_csv(svd_directory+behavior+"/"+str(period)+"/"+'userMapping.csv',header=None,delim_whitespace=True)
    userMapping.columns = ['MMLuserID','userID']
    userMapping['userID'] = userMapping['userID'].astype('int')
    userMapping['MMLuserID'] = userMapping['MMLuserID'].astype('int')
    return userMapping
    
def loadItemFactors(period,behavior_num,svd_directory):
    behavior_types = {0: 'WfcEvent', 
                               1: 'WatchEvent', 
                               2: 'ForkEvent', 
                               3: 'ContributionEvent',
                               4: 'WfEvent'}
    behavior = behavior_types[behavior_num]
    
    # Load the information on number of users and factors
    info = pd.read_csv(svd_directory+behavior+"/"+str(period)+"/"+'model.csv',header=None,delim_whitespace=True,skiprows=2,nrows=1)
    info.columns = ['users','factors']
    nUsers = info['users'].values[0]
    nFactors = info['factors'].values[0]    
    
    model = pd.read_csv(svd_directory+behavior+"/"+str(period)+"/"+'model.csv',header=None,delim_whitespace=True,skiprows=3)    
    itemFactors = model.ix[nUsers*nFactors+1:]
    itemFactors.columns = ['MMLrepoID','num','val']
    
    return itemFactors
    
def loadUserFactors(period,behavior_num,svd_directory,nFactors=100):
    behavior_types = {0: 'WfcEvent', 
                               1: 'WatchEvent', 
                               2: 'ForkEvent', 
                               3: 'ContributionEvent',
                               4: 'WfEvent'}
    behavior = behavior_types[behavior_num]
    
    # Load the information on number of users and factors
    model = pd.read_csv(svd_directory+behavior+'/models/user_factors_'+str(nFactors)+'_'+str(period)+'.csv')           
    
    return model
    
def userFactorVariances(user_factors):
    factor_columns = [str(x) for x in range(0,user_factors.shape[1]-1)]
    variances = user_factors[factor_columns].var()
    return variances
    
def normalizeUserFactors(user_factors):
    
    factor_columns = [str(x) for x in range(0,user_factors.shape[1]-1)]
    user_factors['norm'] = np.sqrt(np.square(user_factors[factor_columns]).sum(axis=1))
    user_factors['norm'] = 1/user_factors['norm']
    user_factors[factor_columns] = user_factors[factor_columns].multiply(user_factors['norm'],axis="index")
    user_factors.drop('norm',axis=1,inplace=True)
    
    return user_factors
    
def getNorms(user_factors):
    
    norms = user_factors[['userID']]
    factor_columns = [str(x) for x in range(0,user_factors.shape[1]-1)]
    norms['norm'] = np.sqrt(np.square(user_factors[factor_columns]).sum(axis=1))
    
    return norms
    
def loadUserPredictions(period,behavior_num,svd_directory):
    behavior_types = {0: 'WfcEvent', 
                               1: 'WatchEvent', 
                               2: 'ForkEvent', 
                               3: 'ContributionEvent',
                               4: 'WfEvent'}
    behavior = behavior_types[behavior_num]
    predictions = pd.read_csv(svd_directory+behavior+"/"+str(period)+"/"+'predictions_formatted.csv')    
    
    return predictions
    
def loadNearestNeighborIndices(period,behavior_num,svd_directory):
    behavior_types = {0: 'WfcEvent', 
                               1: 'WatchEvent', 
                               2: 'ForkEvent', 
                               3: 'ContributionEvent',
                               4: 'WfEvent'}
    behavior = behavior_types[behavior_num]
    nearest_neighbor_indices = pd.read_csv(svd_directory+behavior+"/"+str(period)+"/"+'indices.csv',header=None)    
    
    return nearest_neighbor_indices

def loadNearestNeighborDistances(period,behavior_num,svd_directory):
    behavior_types = {0: 'WfcEvent', 
                               1: 'WatchEvent', 
                               2: 'ForkEvent', 
                               3: 'ContributionEvent',
                               4: 'WfEvent'}
    behavior = behavior_types[behavior_num]
    nearest_neighbor_indices = pd.read_csv(svd_directory+behavior+"/"+str(period)+"/"+'distances.csv',header=None)    
    
    return nearest_neighbor_indices  
    
## Iterator Functions
def getStatus(output_directory,my_file):
    if os.path.isfile(output_directory+my_file):
        current_status = pd.read_csv(my_file)
        iterator = current_status['iterator'].values[0]
    else: 
        iterator = 0
        current_status = pd.DataFrame()
        current_status['iterator'] = [iterator]
    return iterator, current_status
    
def updateStatus(iterator,current_status,output_directory,my_file):
    iterator+=1
    current_status['iterator'] = [iterator]
    current_status.to_csv(output_directory+my_file,index=False,header=True)
    return iterator, current_status
    
def saveChunk(current_chunk,iterator,output_directory,output_file):
    # Save/append the data
    if (iterator==0):
        with open(output_directory+output_file, 'w') as f:
            current_chunk.to_csv(f,index=False,header=True)
    else:
        with open(output_directory+output_file, 'a') as f:
            current_chunk.to_csv(f,index=False,header=False)
    