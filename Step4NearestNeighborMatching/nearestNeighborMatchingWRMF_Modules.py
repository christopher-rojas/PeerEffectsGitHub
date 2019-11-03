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
    
def latentFactorsDistance(agent1_ids,agent2_ids,agent1,agent2,userFactors,factorVars):
    
    # Compute the total weighted (squared) distance between two agents' WRMF preference vectors.    
    
    factor_columns = [str(x) for x in range(0,userFactors.shape[1]-1)]
    agent1_factor_columns = [agent1+str(x) for x in range(0,userFactors.shape[1]-1)]     
    agent2_factor_columns = [agent2+str(x) for x in range(0,userFactors.shape[1]-1)] 

    # Merge the user preference factors onto the links data
    userFactors.columns = agent1_factor_columns+[agent1+'userID']
    agent1_factors = pd.merge(agent1_ids,userFactors,on=agent1+'userID',how='left')
    agent1_factors = agent1_factors[agent1_factor_columns].values
    agent1_factors_T = agent1_factors.transpose()    
    
    userFactors.columns = agent2_factor_columns+[agent2+'userID']
    agent2_factors = pd.merge(agent2_ids,userFactors,on=agent2+'userID',how='left')
    agent2_factors = agent2_factors[agent2_factor_columns].values
    agent2_factors_T = agent2_factors.transpose()     
    
    userFactors.columns = factor_columns+['userID']

    # Compute the weighted number of likes by each
    num_both1 = factorVars.dot(agent1_factors_T)
    num_both1 = agent1_factors.dot(num_both1)
    num_both1 = num_both1.diagonal()
    
    num_both2 = factorVars.dot(agent2_factors_T)
    num_both2 = agent2_factors.dot(num_both2)
    num_both2 = num_both2.diagonal()
    
    num_both1 = np.array([num_both1,]*len(agent2_ids))
    num_both1 = num_both1.transpose()
    num_both2 = np.array([num_both2,]*len(agent1_ids))
    
    # Compute the weighted number of likes by both  
    num_both = factorVars.dot(agent2_factors_T)
    num_both = agent1_factors.dot(num_both) 
    num_both = -2*num_both
    
    wrmf_distance = num_both1+num_both2+num_both    
    
    return wrmf_distance
    
def otherAttributeDistance(agent1_ids,agent2_ids,agent1,agent2,otherCovs,otherVars,variableName):
    
    # Compute the weighted (squared) distance between an attribute (besides WRMF) of two agents
    
    current = otherCovs[variableName]
    currentVar = otherVars[variableName]
    
    # Merge the number of repos onto the links data
    current.rename(columns={'userID':agent1+'userID',variableName:agent1+'_'+variableName},inplace=True)
    agent1_variable = pd.merge(agent1_ids,current,on=agent1+'userID',how='left')
    current.rename(columns={agent1+'userID':agent2+'userID',agent1+'_'+variableName:agent2+'_'+variableName},inplace=True)    
    agent2_variable = pd.merge(agent2_ids,current,on=agent2+'userID',how='left')
    current.rename(columns={agent2+'userID':'userID',agent2+'_'+variableName:variableName},inplace=True)
    
    num1 = np.array([agent1_variable[agent1+'_'+variableName],]*len(agent2_ids)).transpose()   
    num2 = np.array([agent2_variable[agent2+'_'+variableName],]*len(agent1_ids))
    num_diff = num1 - num2
    num_diff = np.square(num_diff)
    num_diff = currentVar*num_diff
    
    return num_diff
    
def computeWeightedDistance(agent1_ids,agent2_ids,agent1,agent2,otherCovs,prefs,prefs_vars,otherVars):

    # Compute total weighted distance between two agents.

    distance = latentFactorsDistance(agent1_ids,agent2_ids,agent1,agent2,prefs,prefs_vars)
    
    for variable in otherCovs.keys():
        attribute_distance = otherAttributeDistance(agent1_ids,agent2_ids,agent1,agent2,otherCovs,otherVars,variable)
        distance += attribute_distance
    
    return distance
    
def GenerateMatches(subUsers,otherCovs,valid_users,prefs,prefs_vars,otherVars,prefType,follows,num_per,num_sample=5000):
    
    # Compute the nearest neighbors for a group of agents.
    
    totalMatches = pd.DataFrame()
    iteration = 0
    finishedUsers = False
    valid_users_df = pd.DataFrame(valid_users)
    valid_users_df.columns=['a2userID']
    rankings = range(0,num_per)*len(subUsers)
    
    while finishedUsers==False:
        
        print "a2Users Iterator: " + str(iteration)
        
        # Grab the current batch of dyads. num_sample must be larger than num_per
        if (iteration+1)*num_sample < len(valid_users):
            potentialMatches = valid_users_df.iloc[iteration*num_sample:(iteration+1)*num_sample]
        else:
            potentialMatches = valid_users_df.iloc[iteration*num_sample:]
            finishedUsers=True
            
        # You need to create the distances between them.
        a_a2_distances = computeWeightedDistance(subUsers[['auserID']],potentialMatches[['a2userID']],'a','a2',otherCovs,prefs,prefs_vars,otherVars)
        
        # Get the column indices of top num_per closest users for each user so far.
        # The row index gives you auser, column index a2user.
        num_cols = np.shape(a_a2_distances)[1]
        current_num_per = min(num_per,num_cols)
        min_distances_arg = np.argpartition(a_a2_distances,current_num_per)[:,:current_num_per]
        min_distances_arg = min_distances_arg.flatten()
        closest_a2_users = potentialMatches['a2userID'].values[min_distances_arg]
        closest_a2_users = np.transpose(closest_a2_users)
        min_distances = np.partition(a_a2_distances,current_num_per)[:,:current_num_per]
        min_distances = min_distances.flatten()
        min_distances = np.transpose(min_distances)
        
        currentUsers = subUsers.loc[subUsers.index.repeat(current_num_per)].reset_index(drop=True)       
        currentUsers['a2userID'] = closest_a2_users
        currentUsers['distance'] = min_distances
        
        # Drop users who are directly connected to each other in the social network,
        # prior to the end of current period.
        follows.columns = ['auserID','a2userID','bad_match']
        currentUsers = pd.merge(currentUsers,follows,on=['auserID','a2userID'],how='left')
        currentUsers = currentUsers[currentUsers.bad_match != 1]
        currentUsers.drop('bad_match',axis=1,inplace=True)
        follows.columns = ['a2userID','auserID','bad_match']  
        currentUsers = pd.merge(currentUsers,follows,on=['auserID','a2userID'],how='left')
        currentUsers = currentUsers[currentUsers.bad_match != 1]
        currentUsers.drop('bad_match',axis=1,inplace=True)        
        follows.columns = ['auserID','a2userID','bad_match']
        
        # The observations that remain are valid matches.
        if iteration>0:
            totalMatches = pd.concat([totalMatches,currentUsers],axis=0,ignore_index=True)
        else:
            totalMatches = currentUsers.copy()
        totalMatches = totalMatches[['auserID','a2userID','distance']]
        
        # Sort by agent ID number and distance.
        totalMatches.sort_values(by=['auserID','distance'],inplace=True)
        totalMatches = totalMatches.groupby('auserID').head(num_per).reset_index()
        totalMatches = totalMatches[['auserID','a2userID','distance']]
           
        iteration+=1
        
    totalMatches['ranking'] = rankings
    return totalMatches
    
def CountA_TreatmentsOutcomes(aUsersChunk, follows, leader_events, follower_events, period, maxTreatment=3):
    
    currentIDs = list(aUsersChunk.auserID.unique())
    currentFollows = follows[follows.auserID.isin(currentIDs)]
        
    # Merge on the treatments, stars by leaders
    treatments = pd.merge(currentFollows,leader_events,on=['tuserID'],how='left')
    
    # Merge on the outcomes, stars by followers
    treatments_adoptions = pd.merge(treatments,follower_events,on=['auserID','repoID'],how='left')

    # Only keep the observation if several conditions are met:
    # First, the follower has not starred the repo by the beginning of the period
    treatments_adoptions = treatments_adoptions[~(treatments_adoptions.acreated_at < period)] 
    # Second, the follower has not starred it before link creation
    treatments_adoptions = treatments_adoptions[~(treatments_adoptions.acreated_at < treatments_adoptions.created_at)]   
    # Third, the follower has not starred it before the leader
    treatments_adoptions = treatments_adoptions[~(treatments_adoptions.acreated_at < treatments_adoptions.tcreated_at)]
    treatments_adoptions.rename(columns={'acreated_at':'a_adopted_at'},inplace=True)
    
    # Add the cumulative number of leader adoptions, which is number of treatments
    treatments_adoptions.sort_values(by=['auserID','tcreated_at'],inplace=True)
    treatments_adoptions['treatment_num'] = treatments_adoptions.groupby(['auserID','repoID']).tcreated_at.cumcount()
    treatments_adoptions['treatment_num'] = treatments_adoptions['treatment_num']+1
    treatments_adoptions = treatments_adoptions[['auserID','repoID','a_adopted_at','tcreated_at','treatment_num']]
        
    # The maximum number of treatments
    treatments_adoptions = treatments_adoptions[treatments_adoptions.treatment_num<=maxTreatment]

    # Set the treatment time equal to the last time the agent is treated on the item
    treatment_times = treatments_adoptions.groupby(['auserID','repoID']).tcreated_at.max().reset_index()
    treatments_adoptions.drop('tcreated_at',axis=1,inplace=True)
    treatments_adoptions = pd.merge(treatments_adoptions,treatment_times,on=['auserID','repoID'],how='left')        
    
    return treatments_adoptions
    
def RemoveBadMatches(a_a2_treat_outs, follows, leader_events, follower_events, period):
    
    currentIDs = list(a_a2_treat_outs['a2userID'].unique())
    currentFollows = follows[follows.auserID.isin(currentIDs)]   
    
    bad_treatments = pd.merge(currentFollows, leader_events,on=['tuserID'],how='left')
    # Merge on the outcomes, stars by followers
    bad_treatments_adoptions = pd.merge(bad_treatments,follower_events,on=['auserID','repoID'],how='left')

    # Only keep the observation if several conditions are met:
    # First, the follower has not starred the repo by the beginning of the period
    bad_treatments_adoptions = bad_treatments_adoptions[~(bad_treatments_adoptions.acreated_at < period)] 
    # Second, the follower has not starred it before link creation
    bad_treatments_adoptions = bad_treatments_adoptions[~(bad_treatments_adoptions.acreated_at < bad_treatments_adoptions.created_at)]   
    # Third, the follower has not starred it before the leader
    bad_treatments_adoptions = bad_treatments_adoptions[~(bad_treatments_adoptions.acreated_at < bad_treatments_adoptions.tcreated_at)]
    bad_treatments_adoptions.rename(columns={'acreated_at':'a_adopted_at'},inplace=True)
    
    bad_treatments_adoptions = bad_treatments_adoptions.groupby(['auserID','repoID']).tuserID.nunique().reset_index()
    bad_treatments_adoptions.rename(columns={'tuserID':'treatment_num2', 'auserID':'a2userID'},inplace=True)
    
    if len(bad_treatments) > 0:
        a_a2_treat_outs = pd.merge(a_a2_treat_outs,bad_treatments_adoptions,on=['a2userID','repoID'],how='left')
        a_a2_treat_outs = a_a2_treat_outs[~(a_a2_treat_outs.treatment_num2 >= a_a2_treat_outs.treatment_num)]
        a_a2_treat_outs.drop('treatment_num2',axis=1,inplace=True)
    
    return a_a2_treat_outs
    
def AddOutcomes(a_a2_treat_outs, follower_events, period):
    
    potentialMatchFollowerEvents = follower_events.rename(columns={'auserID':'a2userID','acreated_at':'a2_adopted_at'})
    a_a2_treat_outs = pd.merge(a_a2_treat_outs,potentialMatchFollowerEvents,on=['a2userID','repoID'],how='left')
        
    # Drop if the matched user adopts before start of current period, or most recent treatment time, even if they are untreated        
    a_a2_treat_outs = a_a2_treat_outs[~(a_a2_treat_outs.a2_adopted_at < period)]        
    a_a2_treat_outs = a_a2_treat_outs[~(a_a2_treat_outs.a2_adopted_at < a_a2_treat_outs.tcreated_at)]
    
    return a_a2_treat_outs
    
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