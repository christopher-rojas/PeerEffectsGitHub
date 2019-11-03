# -*- coding: utf-8 -*-
"""
Created on Thu Mar 22 16:51:31 2018

@author: christopherrojas
"""
import pandas as pd
import numpy as np

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

def loadFollows(path,exact=False):
    follows = pd.read_csv(path)
    follows['created_at'] = convertDates(follows['created_at'],[1,'M'],exact)
    return follows
    
def loadEvents(events_directory,exact=False):
    events = pd.read_csv(events_directory+'stars_first.csv') 
    events['created_at'] = convertDates(events['created_at'],[1,'M'],exact)
    return events
    
def loadExitsUsers(exits_directory,time_after,exact=False):
    exits = pd.read_csv(exits_directory+'exitDatesUsers.csv')
    exits['exited_at'] = convertDates(exits['exited_at'],[1,'M'],exact)
    exits['exited_at'] = exits['exited_at'] + time_after   
    return exits

def loadExperienceUsers(joins_directory,current_period,exact=False):
    joins = pd.read_csv(joins_directory+'joinDatesUsers.csv')
    joins.rename(columns={'joined_at_arch':'joined_at'},inplace=True)
    joins = joins[['userID','joined_at']]
    joins['joined_at'] = convertDates(joins['joined_at'],[1,'M'],exact)
    joins['experience'] = current_period - joins['joined_at']
    
    return joins[['userID','experience']]
       
def loadUserFactors(period,wrmf_directory):
    # Load the information on number of users and factors
    model = pd.read_csv(wrmf_directory+'stars_user_factors_'+str(period)+'.csv')
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
    
def map_ids(row, mapper):
    return mapper[row]
        
def dropFollows(currentUsers,follows):
    
    follows.columns = ['auserID','a2userID','bad_match']
    currentUsers = pd.merge(currentUsers,follows,on=['auserID','a2userID'],how='left')
    currentUsers = currentUsers[currentUsers.bad_match != 1]
    currentUsers.drop('bad_match',axis=1,inplace=True)
    follows.columns = ['a2userID','auserID','bad_match']  
    currentUsers = pd.merge(currentUsers,follows,on=['auserID','a2userID'],how='left')
    currentUsers = currentUsers[currentUsers.bad_match != 1]
    currentUsers.drop('bad_match',axis=1,inplace=True)        
    follows.columns = ['auserID','a2userID','bad_match']
       
    return currentUsers
     
def saveData(data,counter,output_name):
    # save/append the fully processed panel chunk
    if counter==0:
        with open(output_name, 'w') as f:
            data.to_csv(f,index=False,header=True)
    else:
        with open(output_name, 'a') as f:
            data.to_csv(f,index=False,header=False)