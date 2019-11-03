# -*- coding: utf-8 -*-
"""
Match nearest neighbors based on activity vector distance.
"""

import pandas as pd
import numpy as np
import nearestNeighborMatchingModules
import activitiesMatchingModules
import scipy.sparse as sparse
#from sklearn.preprocessing import normalize

################### PARAMETERS THAT NEED TO BE SET
period = 6
nFactors = 100
svd_behavior = 1
svd_directory = ''
events_directory = ''
exits_directory = ''
output_directory = ''
####################

#################### LOAD THE DATA
# Load the user factors
userFactors = nearestNeighborMatchingModules.loadUserFactors(period,svd_behavior,svd_directory,nFactors)
svd_users = list(userFactors['userID'].unique())
userFactors = np.nan
print len(svd_users)

# Load the behaviors which were used to learn SVD,
# and count the total number per user.
events = nearestNeighborMatchingModules.loadEvents(svd_behavior,events_directory)
events = events[['userID','repoID','created_at']]
events = events[events.created_at<=period]

# Load the user activity vectors
userActivities = events[events.created_at<period]
userActivities = userActivities[['userID','repoID']]
# You could normalize for weight not equal to 1.
userActivities['weight'] = 1

# Load the agents who have not exited. 
exits = nearestNeighborMatchingModules.loadExitsUsers(exits_directory,6)
exits = exits[exits.exited_at >= period]
non_exit_users = list(exits['userID'].unique())
print len(non_exit_users)

# Load the experience of each agent
experience = nearestNeighborMatchingModules.loadExperienceUsers(exits_directory,period)

# Load the follows, and keep the dyads created during the relevant period.
follows = nearestNeighborMatchingModules.loadFollows(events_directory)
# The current follows could be the current period, or everything leading up to the current period.
follows = follows[follows.created_at<=period]
follows = follows[['auserID','tuserID','dyadID','created_at']]
####################

#################### USERS TO MATCH
# The valid users for matching; i.e., ones who may have been treated.
users = list(follows[(follows.auserID.isin(list(set(svd_users)&set(non_exit_users))))&(follows.tuserID.isin(non_exit_users))].auserID.unique())
users = pd.DataFrame(users)
users.columns=['auserID']

valid_users = list(users.auserID.unique())
print len(valid_users)
userActivities = userActivities[userActivities.userID.isin(valid_users)]
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

otherAttributes = {'numRepos':numRepos,'inDegree':inDegree,'outDegree':outDegree,
            'experience':experience,'avgLeaderRepos':avgLeaderRepos,'avgLeaderExp':avgLeaderExp,
            'avgLeaderIn':avgLeaderIn,'avgLeaderOut':avgLeaderOut}
####################

#################### VARIANCE FOR PREFERENCE COVARIATES
userActivities.sort_values(by='repoID',inplace=True)
# Create the df for the variances for each repo covariate
repo_vars = userActivities.groupby('repoID').weight.sum().reset_index()
repo_vars['N'] = len(valid_users)
repo_vars['sample_mean'] = repo_vars['weight']/repo_vars['N']
repo_vars['sample_sec_moment'] = (repo_vars['weight']**2)/repo_vars['N']
repo_vars['variance'] = repo_vars['sample_sec_moment'] - np.square(repo_vars['sample_mean'])
repo_vars['inv_var'] = 1/repo_vars['variance']
print repo_vars['inv_var'].describe()
repo_vars_diag = repo_vars['inv_var'].values
repo_vars = sparse.diags(repo_vars_diag,0,format="csr")
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

otherAttributeVars = {'numRepos':numRepos_var,'inDegree':inDegree_var,'outDegree':outDegree_var,
            'experience':experience_var,'avgLeaderRepos':avgLeaderRepos_var,'avgLeaderExp':avgLeaderExp_var,
            'avgLeaderIn':avgLeaderIn_var,'avgLeaderOut':avgLeaderOut_var}
####################

#################### BUILD INDEX MAPPING
# Create mappings
rid_to_idx = {}
idx_to_rid = {}
for (idx, rid) in enumerate(userActivities.repoID.unique().tolist()):
    rid_to_idx[rid] = idx
    
uid_to_idx = {}
idx_to_uid = {}
for (idx, uid) in enumerate(userActivities.userID.unique().tolist()):
    uid_to_idx[uid] = idx

I = userActivities.userID.apply(nearestNeighborMatchingModules.map_ids, args=[uid_to_idx]).as_matrix()
J = userActivities.repoID.apply(nearestNeighborMatchingModules.map_ids, args=[rid_to_idx]).as_matrix()
V = np.ones(I.shape[0])
likes = sparse.coo_matrix((V, (I, J)), dtype=np.float64)
likes = likes.tocsr()
# The following line can normalize the preference matrix
#likes = normalize(likes,norm='l2',axis=1,copy=False)
print np.shape(likes)

rid_to_idx, I, J, V = np.nan, np.nan, np.nan, np.nan
user_activities = np.nan
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
    
    # Build a panel with the matches. I want up to 10 per dyad.   
    matches = activitiesMatchingModules.GenerateMatches(subUsers,otherAttributes,valid_users,likes,repo_vars,otherAttributeVars,'Activities',follows,uid_to_idx)    
    matches['link_period'] = period
    matches['svd_behavior'] = 1
    
    # save/append the fully processed panel chunk
    if iterator==0:
        with open(output_directory+'matches_activities_'+str(period)+str(svd_behavior)+'.csv', 'w') as f:
            matches.to_csv(f,index=False,header=True)
    else:
        with open(output_directory+'matches_activities_'+str(period)+str(svd_behavior)+'.csv', 'a') as f:
            matches.to_csv(f,index=False,header=False)
            
    # Iterate and save progress
    iterator+=1