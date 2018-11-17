# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 17:20:36 2018

@author: christopherrojas

Match nearest neighbors based on preference vector distance.
"""

import pandas as pd
import numpy as np
import nearestNeighborMatchingWRMF_Modules

################### PARAMETERS THAT NEED TO BE SET
# Time period, number of matches to keep per agent,
# and the number of months after last agent activity when we consider them to exit.
period = 1
numNearestNeighbors = 51
numMonthsExit = 6

# Enter the directories where the preferences are stored, and the event data files.
wrmf_dir = ""
data_dir = ""
####################

#################### LOAD THE DATA
# Load the user factors
userFactors = nearestNeighborMatchingWRMF_Modules.loadUserFactors(period,wrmf_dir)

# Normalize the factor vectors
userFactors = nearestNeighborMatchingWRMF_Modules.normalizeUserFactors(userFactors)
wrmf_users = list(userFactors['userID'].unique())
print len(wrmf_users)

# Load the behaviors which were used to learn preferences,
# and count the total number per user.
events = nearestNeighborMatchingWRMF_Modules.loadEvents(data_dir)
events = events[['userID','repoID','created_at']]
events = events[events.created_at<=period]

# Load the agents who have not exited. 
exits = nearestNeighborMatchingWRMF_Modules.loadExitsUsers(data_dir,numMonthsExit)
exits = exits[exits.exited_at >= period]
non_exit_users = list(exits['userID'].unique())
print len(non_exit_users)

# Load the time experience of each agent
experience = nearestNeighborMatchingWRMF_Modules.loadExperienceUsers(data_dir,period)

# Load the follows
follows = nearestNeighborMatchingWRMF_Modules.loadFollows(data_dir)
follows = follows[follows.created_at<=period]
follows = follows[['auserID','tuserID','created_at']]
####################

#################### USERS TO MATCH
# The valid users for matching; i.e., ones who may have been treated.
users = list(follows[(follows.auserID.isin(list(set(wrmf_users)&set(non_exit_users))))&(follows.tuserID.isin(non_exit_users))].auserID.unique())
users = pd.DataFrame(users)
users.columns=['auserID']

valid_users = list(users.auserID.unique())
print len(valid_users)
userFactors = userFactors[userFactors.userID.isin(valid_users)]
####################

#################### OTHER DATA FOR MATCHING
# Don't need covariates for agents who exit
events = events[events.userID.isin(non_exit_users)]
experience = experience[experience.userID.isin(non_exit_users)]
follows = follows[(follows.auserID.isin(non_exit_users))]
follows = follows[(follows.tuserID.isin(non_exit_users))]

## OUT-DEGREE
outDegree = follows[follows.created_at<period].groupby('auserID').tuserID.count().reset_index()
outDegree.rename(columns={'tuserID':'outDegree'},inplace=True)
outDegree['outDegree'] = np.log(outDegree['outDegree']+1)

# Average out-degree per follower
avgLeaderOut = outDegree.rename(columns={'auserID':'tuserID'})
avgLeaderOut = pd.merge(follows[['auserID','tuserID']],avgLeaderOut,on='tuserID',how='left')
avgLeaderOut['outDegree'].fillna(0,inplace=True)
avgLeaderOut = avgLeaderOut.groupby('auserID').outDegree.mean().reset_index()
avgLeaderOut.rename(columns={'outDegree':'avgLeaderOut'},inplace=True)

# Keep the ones for valid users only
avgLeaderOut = avgLeaderOut[avgLeaderOut.auserID.isin(valid_users)]
outDegree = pd.merge(users,outDegree,on='auserID',how='left')
outDegree['outDegree'].fillna(0,inplace=True)

## IN-DEGREE
inDegree = follows[follows.created_at<period].groupby('tuserID').auserID.count().reset_index()
inDegree.rename(columns={'auserID':'inDegree'},inplace=True)
inDegree.rename(columns={'tuserID':'auserID'},inplace=True)
inDegree['inDegree'] = np.log(inDegree['inDegree']+1)

# Average in-degree per follower
avgLeaderIn = inDegree.rename(columns={'auserID':'tuserID'})
avgLeaderIn = pd.merge(follows[['auserID','tuserID']],avgLeaderIn,on='tuserID',how='left')
avgLeaderIn['inDegree'].fillna(0,inplace=True)
avgLeaderIn = avgLeaderIn.groupby('auserID').inDegree.mean().reset_index()
avgLeaderIn.rename(columns={'inDegree':'avgLeaderIn'},inplace=True)

# Keep the ones for valid users only
avgLeaderIn = avgLeaderIn[avgLeaderIn.auserID.isin(valid_users)]
inDegree = pd.merge(users,inDegree,on='auserID',how='left')
inDegree['inDegree'].fillna(0,inplace=True)

## NUMBER OF REPOS
numRepos = events[events.created_at<period].groupby('userID').repoID.nunique().reset_index()
numRepos.rename(columns={'repoID':'numRepos','userID':'auserID'},inplace=True)
numRepos['numRepos'] = np.log(numRepos['numRepos']+1)

# Average number of repos per follower
avgLeaderRepos = numRepos.rename(columns={'auserID':'tuserID'})
avgLeaderRepos = pd.merge(follows[['auserID','tuserID']],avgLeaderRepos,on='tuserID',how='left')
avgLeaderRepos['numRepos'].fillna(0,inplace=True)
avgLeaderRepos = avgLeaderRepos.groupby('auserID').numRepos.mean().reset_index()
avgLeaderRepos.rename(columns={'numRepos':'avgLeaderRepos'},inplace=True)

# Keep the ones for valid users only
avgLeaderRepos = avgLeaderRepos[avgLeaderRepos.auserID.isin(valid_users)]
numRepos = pd.merge(users,numRepos,on='auserID',how='left')
numRepos['numRepos'].fillna(0,inplace=True)
events = np.nan

## EXPERIENCE

# Experience per user
experience.rename(columns={'userID':'auserID'},inplace=True)

# Average experience per followee
avgLeaderExp = experience.rename(columns={'auserID':'tuserID'})
avgLeaderExp = pd.merge(follows[['auserID','tuserID']],avgLeaderExp,on='tuserID',how='left')
avgLeaderExp = avgLeaderExp.groupby('auserID').experience.mean().reset_index()
avgLeaderExp.rename(columns={'experience':'avgLeaderExp'},inplace=True)

# Keep the repos for valid users only
avgLeaderExp = avgLeaderExp[avgLeaderExp.auserID.isin(valid_users)]
experience = experience[experience.auserID.isin(valid_users)]

otherAttributes = {'numRepos':numRepos,'experience':experience,'inDegree':inDegree,'outDegree':outDegree,
                   'avgLeaderRepos':avgLeaderRepos,'avgLeaderExp':avgLeaderExp,
                   'avgLeaderIn':avgLeaderIn,'avgLeaderOut':avgLeaderOut}
####################

#################### VARIANCE FOR PREFERENCE COVARIATES
# Compute the variance for each latent factor.
factorVarsDiag = nearestNeighborMatchingWRMF_Modules.userFactorVariances(userFactors)
factorVarsDiag = 1/factorVarsDiag
factorVars = np.diag(factorVarsDiag)
factorVarsDiag = np.nan
####################

#################### VARIANCE FOR OTHER COVARIATES

# Compute the variance for total number of repos.
print numRepos['numRepos'].describe()
numRepos_var = numRepos['numRepos'].var()
numRepos_var = 1/numRepos_var

# Compute the variance for in-degree
print inDegree['inDegree'].describe()
inDegree_var = inDegree['inDegree'].var()
inDegree_var = 1/inDegree_var

# Compute the variance for out-degree
print outDegree['outDegree'].describe()
outDegree_var = outDegree['outDegree'].var()
outDegree_var = 1/outDegree_var

# Compute the variance for experience
print experience['experience'].describe()
experience_var = experience['experience'].var()
experience_var = 1/experience_var

# Compute the variance for avg leader number of events
print avgLeaderRepos['avgLeaderRepos'].describe()
avgLeaderRepos_var = avgLeaderRepos['avgLeaderRepos'].var()
avgLeaderRepos_var = 1/avgLeaderRepos_var

# Compute the variance for avg leader experience
print avgLeaderExp['avgLeaderExp'].describe()
avgLeaderExp_var = avgLeaderExp['avgLeaderExp'].var()
avgLeaderExp_var = 1/avgLeaderExp_var

# Compute the variance for avg leader in-Degree
print avgLeaderIn['avgLeaderIn'].describe()
avgLeaderIn_var = avgLeaderIn['avgLeaderIn'].var()
avgLeaderIn_var = 1/avgLeaderIn_var

# Compute the variance for avg leader out-Degree
print avgLeaderOut['avgLeaderOut'].describe()
avgLeaderOut_var = avgLeaderOut['avgLeaderOut'].var()
avgLeaderOut_var = 1/avgLeaderOut_var

otherAttributeVars = {'numRepos':numRepos_var,'experience':experience_var,'inDegree':inDegree_var,'outDegree':outDegree_var,
                      'avgLeaderRepos':avgLeaderRepos_var,'avgLeaderExp':avgLeaderExp_var,
                      'avgLeaderIn':avgLeaderIn_var,'avgLeaderOut':avgLeaderOut_var}
####################

total = len(users)
print total

finished=False
iterator = 0

# Keep valid users who follow each other. We will remove them from nearest neighbors.
follows = follows[follows.tuserID.isin(valid_users)]
follows = follows[follows.created_at<=period]
follows['bad_match'] = 1
follows = follows[['auserID','tuserID','bad_match']]

while finished==False:

    subUsers = np.nan
    print "aUsers Iterator: " + str(iterator)
    # Grab the current batch of dyads
    if (iterator+1)*5000 < total:
        subUsers = users.iloc[iterator*5000:(iterator+1)*5000]
    else:
        subUsers = users.iloc[iterator*5000:]
        finished=True
    
    # Compute distances, and keep the closest numNearestNeighbors for each agent. 
    matches = nearestNeighborMatchingWRMF_Modules.GenerateMatches(subUsers,otherAttributes,valid_users,userFactors,factorVars,otherAttributeVars,follows,numNearestNeighbors)    
    matches['link_period'] = period
    
    # save/append the fully processed panel chunk
    if iterator==0:
        with open('nearest_neighbors_WRMF_'+str(period)+'.csv', 'w') as f:
            matches.to_csv(f,index=False,header=True)
    else:
        with open('nearest_neighbors_WRMF_'+str(period)+'.csv', 'a') as f:
            matches.to_csv(f,index=False,header=False)
            
    # Iterate and save progress
    iterator+=1