# -*- coding: utf-8 -*-
"""
Randomly match users.
"""

import pandas as pd
import matching_modules
import numpy as np

####### PARAMETERS TO SET
period = 1
matching_data = ''
follows_data = ''
exits_data = ''
#######

# Load the follows, and keep the dyads created during the relevant period.
follows = matching_modules.loadFollows(follows_data)
# The current follows could be the current period, or everything leading up to the current period.
follows = follows[follows.created_at<=period]
follows = follows[['auserID','tuserID','dyadID','created_at']]

matching_users = list(pd.read_csv('').auserID.unique())
exits = matching_modules.loadExitsUsers(exits_data,6)
exits = exits[exits.exited_at >= period]
non_exit_users = list(exits['userID'].unique())

# The users for matching followed a user who didn't exit.
users = list(follows[(follows.auserID.isin(list(set(matching_users)&set(non_exit_users))))&(follows.tuserID.isin(non_exit_users))].auserID.unique())
users = pd.DataFrame(users)
users.columns=['auserID']
n_users = len(users)
print n_users

users = pd.concat([users]*51)
numToMatch = len(users)
print numToMatch

# You could select randomly from non-exit users, or from non-exit and svd users.
random_users = np.random.choice(non_exit_users,len(users),replace=True)
print len(random_users)

rankings = range(0,51)
rankings = n_users*rankings
print len(rankings)

# Take the list of users to match, and multiply it by 10, insert into random neighbors
users['a2userID'] = random_users
users['ranking'] = rankings
users['distance'] = -1
users['link_period']=period

# The follows of users that can't be matched.
follows = follows[follows.tuserID.isin(matching_users)]
follows = follows[follows.created_at<=period]
follows['bad_match'] = 1
follows = follows[['auserID','tuserID','bad_match']]

# Drop users who are directly connected to each other in the social network,
# prior to the end of current period.
follows.columns = ['auserID','a2userID','bad_match']
users = pd.merge(users,follows,on=['auserID','a2userID'],how='left')
users = users[users.bad_match != 1]
users.drop('bad_match',axis=1,inplace=True)
follows.columns = ['a2userID','auserID','bad_match']  
users = pd.merge(users,follows,on=['auserID','a2userID'],how='left')
users = users[users.bad_match != 1]
users.drop('bad_match',axis=1,inplace=True)        
follows.columns = ['auserID','a2userID','bad_match']  

users.drop_duplicates(subset=['auserID','a2userID'],inplace=True)

users.to_csv('',index=False)

