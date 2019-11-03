#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Combine multiple datasets into a single table for estimating nearest neighbors.
Each row is an agent, and each column is an attribute.
"""

import pandas as pd
import numpy as np
import nearestNeighborMatchingWRMF_Modules

################### PARAMETERS THAT NEED TO BE SET
period = 1 # Time period (months in paper)
numMonthsExit = 6 # The number of months after last agent activity when we consider them to "exit"

wrmf_dir = "" # directory where output from WRMF (Step 3) is stored
data_dir = "" # directory where formatted stars and follows data (Step 2) is stored
####################

# If you want to match based on languages, then load the languages csv instead of warm
# If you want to match on 'Other Attributes' without using past preference data,
# then just create the matching data without wrmf or langs

#################### LOAD THE DATA
# Load the user factors
userFactors = nearestNeighborMatchingWRMF_Modules.loadUserFactors(period,wrmf_dir)

# Normalize the factor vectors
userFactors = nearestNeighborMatchingWRMF_Modules.normalizeUserFactors(userFactors)
wrmf_users = list(userFactors['userID'].unique())

# Load the behaviors which were used to learn preferences,
# and count the total number per user.
events = nearestNeighborMatchingWRMF_Modules.loadEvents(data_dir)
events = events[['userID','repoID','created_at']]
events = events[events.created_at<=period]

# Load the agents who have not exited. 
exits = nearestNeighborMatchingWRMF_Modules.loadExitsUsers(data_dir,numMonthsExit)
exits = exits[exits.exited_at >= period]
non_exit_users = list(exits['userID'].unique())

# Load the time experience of each agent
experience = nearestNeighborMatchingWRMF_Modules.loadExperienceUsers(data_dir,period)

# Load the follows
follows = nearestNeighborMatchingWRMF_Modules.loadFollows(data_dir)
follows = follows[follows.created_at<=period]
follows = follows[['auserID','tuserID','created_at']]
####################

#################### USERS TO MATCH
# The valid users for matching; i.e., ones who may have been treated.
# The valid users for matching need to have defined preference types, and cannot have exited
users = list(follows[(follows.auserID.isin(list(set(wrmf_users)&set(non_exit_users))))&(follows.tuserID.isin(non_exit_users))].auserID.unique())
users = pd.DataFrame(users)
users.columns=['auserID']

valid_users = list(users.auserID.unique())
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

# Merge everything to create the matching data
matchingData = pd.merge(userFactors,outDegree,on=['auserID'], how='left')
matchingData = pd.merge(matchingData,avgLeaderOut,on=['auserID'], how='left')
matchingData = pd.merge(matchingData,inDegree,on=['auserID'],how='left')
matchingData = pd.merge(matchingData,avgLeaderIn,on=['auserID'],how='left')
matchingData = pd.merge(matchingData,numRepos,on=['auserID'],how='left')
matchingData = pd.merge(matchingData,avgLeaderRepos,on=['auserID'],how='left')
matchingData = pd.merge(matchingData,experience,on=['auserID'],how='left')
matchingData = pd.merge(matchingData,avgLeaderExp,on=['auserID'],how='left')

# Save the data
matchingData.to_csv(c,index=False,header=True)