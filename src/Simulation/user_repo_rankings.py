# -*- coding: utf-8 -*-
"""
Created on Thu Mar 15 13:02:34 2018

@author: christopherrojas
"""

import os.path
import pandas as pd
import numpy as np
import simulation_modules

############## PARAMETERS TO SET
# Load the information on number of users and factors
period = 1
svd_directory = ''
events_directory = ''
exits_directory = ''
output_directory = ''
##############

# Check if there is any existing progress on the panel with the current options.
my_file = output_directory+"status.csv"
if os.path.isfile(my_file):
    status = pd.read_csv(my_file)
    iterator_users = status['iterator_users'].values[0]
    iterator_repos = status['iterator_repos'].values[0]
else: 
    iterator = 0
    status = pd.DataFrame()
    status['iterator_users'] = [iterator]
    status['iterator_repos'] = [iterator]

# Load user factors
user_factors = simulation_modules.loadUserFactors(period,svd_directory)
svd_users = list(user_factors['userID'].unique())
print len(user_factors)

# Load the information on number of repos and factors
repo_factors = simulation_modules.loadItemFactors(period,svd_directory)
svd_repos = list(repo_factors['repoID'].unique())
print len(repo_factors)

# Load the behaviors which were used to learn SVD
events = simulation_modules.loadEvents(events_directory)
events = events[['userID','repoID','created_at']]
events = events[events.created_at<period]
events = events[['userID','repoID']]
print len(events)

# Load the agents who have not exited. 
exits = simulation_modules.loadExitsUsers(exits_directory,6)
exits = exits[(exits.deleted==0)|(exits.exited_at >= period)]
non_exit_users = list(exits['userID'].unique())
exits = np.nan
print len(non_exit_users)

joins = simulation_modules.loadExperienceUsers(exits_directory,period+1)
joins_users = list(joins[joins.experience>0].userID.unique())

# Need to make non-exit repos.
# Load the repos that have not been deleted.
exits = simulation_modules.loadExitsRepos(exits_directory,6)
exits = exits[exits.exited_at >= period]
non_exit_repos = list(exits['repoID'].unique())
exits = np.nan
print len(non_exit_repos)

# The valid repos for ranking
valid_repos = list(set(svd_repos)&set(non_exit_repos))
print len(valid_repos)
repo_factors = repo_factors[repo_factors.repoID.isin(valid_repos)]
repo_factors.reset_index(inplace=True,drop=True)

# The valid users for matching
valid_users = list(set(svd_users)&set(non_exit_users)&set(joins_users))
print len(valid_users)
events = events[(events.userID.isin(valid_users))&(events.repoID.isin(valid_repos))]
user_factors = user_factors[user_factors.userID.isin(valid_users)]
user_factors.reset_index(inplace=True,drop=True)

total_users = len(user_factors)
total_repos = len(repo_factors)
print total_users, total_repos

finished_users=False
iterator_users = 0

while finished_users==False:
    
    subUsers = np.nan
    print "users Iterator: " + str(iterator_users)
    # Grab the current batch of dyads
    if (iterator_users+1)*5000 < total_users:
        subUsers = user_factors.iloc[iterator_users*5000:(iterator_users+1)*5000]
    else:
        subUsers = user_factors.iloc[iterator_users*5000:]
        finished_users=True
    
    subUsers.reset_index(inplace=True,drop=True)
    subUsers['index'] = subUsers.index.values
    
    finished_repos=False
    iterator_repos=0
    
    data = pd.DataFrame()
    rankings = range(0,100)*len(subUsers)
    subEventsUsers = events[events.userID.isin(list(subUsers.userID.unique()))]
    
    while finished_repos==False:
            
        subRepos = np.nan
        print "repos Iterator: " + str(iterator_repos)
        # Grab the current batch of dyads
        if (iterator_repos+1)*10000 < total_repos:
            subRepos = repo_factors.iloc[iterator_repos*10000:(iterator_repos+1)*10000]
        else:
            subRepos = repo_factors.iloc[iterator_repos*10000:]
            finished_repos=True
        
        subRepos.reset_index(inplace=True,drop=True)
        
        # First, get the indices of repos starred by each agent.
        subEventsUsersRepos = subEventsUsers[subEventsUsers.repoID.isin(list(subRepos.repoID.unique()))]
        numDone = len(subEventsUsersRepos)        
        if numDone>0:       
            liked_userids = subEventsUsersRepos['userID'].values      
            liked_repoids = subEventsUsersRepos['repoID'].values        

            user_dict = dict(subUsers[['userID','index']].values)
            liked_userpos = np.array([user_dict[x] for x in liked_userids])     
        
            subRepos['index'] = subRepos.index.values
            item_dict = dict(subRepos[['repoID','index']].values)      
            liked_repopos = np.array([item_dict[x] for x in liked_repoids])      
        
        # Compute user preferences
        print "Computing preferences."   
        factor_columns = [str(x) for x in range(0,subUsers.shape[1]-2)]
        X = subUsers[factor_columns].values
        Y = subRepos[factor_columns].values
        Y = np.transpose(Y)
        preferences = X.dot(Y)
        print np.shape(preferences)
        
        # Eliminate already starred repos
        if numDone>0:        
            preferences[liked_userpos,liked_repopos] = -1*np.inf
        
        num_cols = np.shape(preferences)[1]
        current_num_per = min(100,num_cols)
        
        # min_distances_arg? You want the highest dot products, not mins.
        # You could multiply by negative 1, and then take mins
        preferences = -1*preferences
        
        min_distances_arg = np.argpartition(preferences,current_num_per)[:,:current_num_per]
        min_distances_arg = min_distances_arg.flatten()
        most_preferred_repos = subRepos['repoID'].values[min_distances_arg]
        most_preferred_repos = np.transpose(most_preferred_repos)
        
        min_distances = np.partition(preferences,current_num_per)[:,:current_num_per]
        min_distances = min_distances.flatten()
        min_distances = np.transpose(min_distances)

        currentUsers = subUsers.loc[subUsers.index.repeat(current_num_per)].reset_index(drop=True)       
        currentUsers['repoID'] = most_preferred_repos 
        currentUsers['pref'] = min_distances
        # Restore the correct preference value by multiplying by -1 again
        currentUsers['pref'] = -1*currentUsers['pref']        
        
        # The observations that remain are valid matches.
        if iterator_repos>0:
            data = pd.concat([data,currentUsers],axis=0,ignore_index=True)
        else:
            data = currentUsers.copy()
        data = data[['userID','repoID','pref']]
        
        # Sort by preference, keep top 100 per user.
        data.sort_values(by=['pref'],inplace=True,ascending=False)
        data = data.groupby('userID').head(100).reset_index()
        data = data[['userID','repoID','pref']]
           
        iterator_repos += 1
        
        status['iterator_users'] = [iterator_users]
        status['iterator_repos'] = [iterator_repos]
        status.to_csv(my_file,index=False,header=True)        
    
    data.sort_values(by=['userID','pref'],ascending=[True,False],inplace=True)
    data['ranking'] = rankings
    
    if iterator_users==0:
        with open(output_directory+'user_repo_prefs_'+str(period)+'.csv', 'w') as f:
            data.to_csv(f,index=False,header=True)
    else:
        with open(output_directory+'user_repo_prefs_'+str(period)+'.csv', 'a') as f:
            data.to_csv(f,index=False,header=False)
    
    iterator_users+=1