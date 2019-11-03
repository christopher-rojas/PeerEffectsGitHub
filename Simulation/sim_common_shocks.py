# -*- coding: utf-8 -*-
"""
Common shocks
"""

import simulation_modules
import pandas as pd
import numpy as np

################### PARAMETERS THAT NEED TO BE SET
period = 1
svd_directory = ''
events_directory = ''
output_directory = ''
####################

#################### LOAD THE DATA
# Load the user factors
userFactors = pd.read_csv(svd_directory+'user_factors_'+str(period)+'_100.csv',usecols=['userID'])
svd_users = list(userFactors['userID'].unique())
print len(svd_users)
userFactors.rename(columns={'userID':'auserID'},inplace=True)

# Load the SVD repos
svd_repos = pd.read_csv(svd_directory+'item_factors_'+str(period)+'_100.csv',usecols=['repoID'])
svd_repos = list(svd_repos['repoID'].unique())
print len(svd_repos)

# Load the stars
stars = simulation_modules.loadEvents(1,events_directory,exact=True)
stars = stars[['userID','repoID','created_at']]

# Need to drop items agents starred prior to the period
follower_stars = stars[stars.created_at<period]
follower_stars.rename(columns={'userID':'auserID','created_at':'acreated_at'},inplace=True)

# Load the agents who have not exited by the start of the period. 
exits = simulation_modules.loadExitsUsers(events_directory,6)
exits = exits[exits.exited_at >= period]
non_exit_users = list(exits['userID'].unique())
print len(non_exit_users)

# Load the experience of each agent
experience = simulation_modules.loadExperienceUsers(events_directory,period)

# Load the follows
follows = simulation_modules.loadFollows(events_directory)
# I will need follows up to and including the current period.
# However, use only follows prior to period for computing matching covaraiates.
follows = follows[follows.created_at<=period]
follows = follows[['auserID','tuserID']]

#################### USERS TO MATCH
# The valid users for matching; i.e., ones who may have been treated.
users = list(follows[(follows.auserID.isin(list(set(svd_users)&set(non_exit_users))))&(follows.tuserID.isin(non_exit_users))].auserID.unique())
users = pd.DataFrame(users)
users.columns=['auserID']

valid_users = list(users.auserID.unique())
print len(valid_users)
userFactors = userFactors[userFactors.auserID.isin(valid_users)]
####################

total = len(users)
print total

#################### COMMON SHOCKS
# Load the stars
stars = stars[(stars.created_at<period)&(stars.created_at>=period-3)]
stars = stars.groupby('repoID').userID.nunique().reset_index()
stars.sort_values(by=['userID'],ascending=False,inplace=True)
stars.rename(columns={'userID':'numPop'},inplace=True)
stars = stars.head(n=10001)
print len(stars)
########################

iterator, min_counter = 0, 0
users['counter'] = min_counter
users['repoID'] = np.nan

while min_counter <= 101:
    
    print min_counter
    
    current_repo = stars.head(n=1).repoID.values[0]
    print current_repo
    users['repoID'] = current_repo

    users = pd.merge(users,follower_stars,on=['auserID','repoID'],how='left')
    
    current_users = users[users.acreated_at != users.acreated_at]
    current_users = current_users[current_users.counter <= 101]       
    print len(current_users)
    current_users = current_users[['auserID','repoID','counter']]

    stars = stars.iloc[1:]
    print len(stars)
    
    # save/append the fully processed panel chunk
    if iterator==0:
        with open(output_directory+'simulated_common_shocks'+str(period)+'.csv', 'w') as f:
            current_users.to_csv(f,index=False,header=True)
    else:
        with open(output_directory+'simulated_common_shocks'+str(period)+'.csv', 'a') as f:
            current_users.to_csv(f,index=False,header=False)    
    current_users = np.nan
    
    users.ix[users.acreated_at != users.acreated_at,'counter'] += 1
    users = users[['auserID','repoID','counter']]
    users['repoID'] = np.nan
    min_counter = users['counter'].min()
    
    iterator+=1