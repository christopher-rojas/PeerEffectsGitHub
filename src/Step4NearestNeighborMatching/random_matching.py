# -*- coding: utf-8 -*-
"""
Randomly match users.
"""

import pandas as pd
import matching_modules
import numpy as np
import nearestNeighborMatching_Modules

####### PARAMETERS TO ENTER
period = 1 # time period
numMonthsExit = 6 # The number of months after last agent activity when we consider them to "exit"
numNearestNeighbors = 51 # Number of nearest neighbors to keep for each agent (+ 1), equals 50 in paper
baseline_dir = "" # The output from baseline.py
baseline_filename = "baseline.csv"
follows_dir = ""
follows_data = "follows.csv"
exits_dir = ""
exits_filename = "exitsUsers.csv"
####### MODIFY BELOW THIS LINE AT YOUR OWN RISK

# Load the follows, and keep the dyads created during the relevant period.
follows = matching_modules.loadFollows(follows_data)
# The current follows could be the current period, or everything leading up to the current period.
follows = follows[follows.created_at<=period]
follows['bad_match'] = 1
follows = follows[['auserID','tuserID','bad_match']]

# Load the agents who have not exited. 
exits = nearestNeighborMatching_Modules.loadExitsUsers(exits_dir + exits_filename,numMonthsExit)
exits = exits[exits.exited_at >= period]
non_exit_users = list(exits['userID'].unique())

# Load the users who need to be matched
users = pd.read_csv(baseline_dir + baseline_filename)
users = users[['auserID']]
n_users = len(users)

users = pd.concat([users]*numNearestNeighbors)
numToMatch = len(users)

# Select matches randomly from users who haven't exited
random_users = np.random.choice(non_exit_users,len(users),replace=True)

rankings = range(0,numNearestNeighbors)
rankings = n_users*rankings

# Take the list of users to match, and multiply it by 10, insert into random neighbors
users['a2userID'] = random_users
users['ranking'] = rankings
users['distance'] = -1 # Set "distance" at -1 for the random matches
users['link_period']=period

users = nearestNeighborMatching_Modules.dropFollows(users,follows)

users.drop_duplicates(subset=['auserID','a2userID'],inplace=True)
users.to_csv('',index=False)

