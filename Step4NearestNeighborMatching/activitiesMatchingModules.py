# -*- coding: utf-8 -*-
"""
Match nearest neighbors based on sparse activity vectors (very inefficient)
"""

import pandas as pd
import numpy as np

def activitiesDistance(agent1_ids,agent2_ids,agent1,agent2,uid_to_idx,likes,repo_vars):
    
     # Get the matrix of likes for each subset of agents
    agent1_idx = [uid_to_idx.get(key) for key in list(agent1_ids[agent1+'userID'])]
    agent2_idx = [uid_to_idx.get(key) for key in list(agent2_ids[agent2+'userID'])]
    
    agent1_likes = likes[agent1_idx,]
    agent1_likes_T = agent1_likes.transpose()
    agent2_likes = likes[agent2_idx,]
    agent2_likes_T = agent2_likes.transpose()
    
    # Compute the weighted number of likes by each
    num_both1 = repo_vars.dot(agent1_likes_T)
    num_both1 = agent1_likes.dot(num_both1)
    num_both1 = num_both1.diagonal()
    
    num_both2 = repo_vars.dot(agent2_likes_T)
    num_both2 = agent2_likes.dot(num_both2)
    num_both2 = num_both2.diagonal()
    
    num_both1 = np.array([num_both1,]*len(agent2_ids))
    num_both1 = num_both1.transpose()
    num_both2 = np.array([num_both2,]*len(agent1_ids))
    
    # Compute the weighted number of likes by both  
    num_both = repo_vars.dot(agent2_likes_T)
    num_both = agent1_likes.dot(num_both)
    num_both = num_both.todense()
    num_both = -2*num_both
    
    acts_distance = num_both1 + num_both2 + num_both
    
    return acts_distance

def otherAttributeDistance(agent1_ids,agent2_ids,agent1,agent2,otherCovs,otherVars,variableName):
    
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
    
def computeWeightedDistance(agent1_ids,agent2_ids,agent1,agent2,otherCovs,prefs,prefs_vars,otherVars,prefType,index):

    # First compute the distance between their preference types.
    distance = activitiesDistance(agent1_ids,agent2_ids,agent1,agent2,index,prefs,prefs_vars)
    
    for variable in otherCovs.keys():
        attribute_distance = otherAttributeDistance(agent1_ids,agent2_ids,agent1,agent2,otherCovs,otherVars,variable)
        distance += attribute_distance
    
    return distance
   
def GenerateMatches(subUsers,otherCovs,valid_users,prefs,prefs_vars,otherVars,prefType,follows,index=None,num_per = 51,num_sample=5000):
    
    totalMatches = pd.DataFrame()
    iteration = 0
    finishedUsers = False
    valid_users_df = pd.DataFrame(valid_users)
    valid_users_df.columns=['a2userID']
    rankings = range(0,num_per)*len(subUsers)
    
    while finishedUsers==False:
        
        # Generate users to match. Keep the top num_per as of each iteration.
        print "a2Users Iterator: " + str(iteration)
        
        # Grab the current batch of dyads. num_sample must be larger than num_per
        if (iteration+1)*num_sample < len(valid_users):
            potentialMatches = valid_users_df.iloc[iteration*num_sample:(iteration+1)*num_sample]
        else:
            potentialMatches = valid_users_df.iloc[iteration*num_sample:]
            finishedUsers=True
            
        # You need to create the distances between them.
        a_a2_distances = computeWeightedDistance(subUsers[['auserID']],potentialMatches[['a2userID']],'a','a2',otherCovs,prefs,prefs_vars,otherVars,prefType,index)
        
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
        
        # Sort by auserID, D
        totalMatches.sort_values(by=['auserID','distance'],inplace=True)
        totalMatches = totalMatches.groupby('auserID').head(num_per).reset_index()
        totalMatches = totalMatches[['auserID','a2userID','distance']]
           
        iteration+=1
        
    totalMatches['ranking'] = rankings
    return totalMatches