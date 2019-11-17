# -*- coding: utf-8 -*-
"""
Influence-based adoption
"""

import pandas as pd
import numpy as np
import simulation_modules

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

follower_stars = stars[stars.created_at<period+2]
follower_stars.rename(columns={'userID':'auserID','created_at':'acreated_at'},inplace=True)

stars = stars[(stars.created_at<period+2)&(stars.created_at>=period)]
#stars = stars[stars.repoID.isin(svd_repos)]
stars.rename(columns={'userID':'tuserID','created_at':'tcreated_at'},inplace=True)
print len(stars)

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

finished=False
iterator = 0

while finished==False:

    subUsers = np.nan
    print "aUsers Iterator: " + str(iterator)
    # Grab the current batch of dyads
    if (iterator+1)*5000 < total:
        subUsers = users.iloc[iterator*5000:(iterator+1)*5000]
    else:
        subUsers = users.iloc[iterator*5000:]
        finished=True
    
    data = follows[follows.auserID.isin(list(subUsers.auserID.unique()))]   
    data = pd.merge(data,stars,on=['tuserID'],how='left')
    treatment_stars = follower_stars[follower_stars.auserID.isin(list(subUsers.auserID.unique()))]
    
    data = pd.merge(data,treatment_stars,on=['auserID','repoID'],how='left')
    data = data[~(data.acreated_at <= data.tcreated_at)|(data.acreated_at<period)]
    data = data[['auserID','repoID','tcreated_at']]
    data.sort_values(by='tcreated_at',inplace=True) 

    data = data[data.repoID == data.repoID]    
    
    # save/append the fully processed panel chunk
    if iterator==0:
        with open(output_directory+'simulation_leader_adoptions'+str(period)+'.csv', 'w') as f:
            data.to_csv(f,index=False,header=True)
    else:
        with open(output_directory+'simulation_leader_adoptions'+str(period)+'.csv', 'a') as f:
            data.to_csv(f,index=False,header=False)
            
    # Iterate and save progress
    iterator+=1