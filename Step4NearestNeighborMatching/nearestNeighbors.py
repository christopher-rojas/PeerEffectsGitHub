# -*- coding: utf-8 -*-
"""
Match nearest neighbors.
"""

import pandas as pd
import numpy as np
import nearestNeighborMatchingWRMF_Modules
from scipy.spatial import distance
import logging

logging.basicConfig(filename='nearest_neighbors_WRMF.log',level=logging.INFO,format='%(asctime)s %(message)s')

################### PARAMETERS THAT NEED TO BE SET
period = 1 # Time period (months in paper)
numNearestNeighbors = 51 # Number of nearest neighbors to keep for each agent + 1, equals 50 in paper
matching_data= "/Users/christopherrojas/Workspace/my_thesis/PeerInfluenceAdoption/Contagion_Common_Prefs_Matching/output/matching_data_1.csv" # directory where the matching sample (step 4 part 1) is stored
follows_data = "/Users/christopherrojas/Workspace/my_thesis/GitHubData/FormatEventsData/GH_Archive/data/follows_first.csv" # directory where the formatted follows data (step 2) is stored
output_name = "nn_svd_"+str(period)+".csv" # save ouput path and name
batch_size_A = 1000 # (batch_size_A * batch_size_B) is size of distance matrix each iteration
batch_size_B = 10000 
####################

# Load the matching sample
matching_sample = pd.read_csv(matching_data)

# Load the follows
# Keep users who followed each other. We will remove them from nearest neighbors at the end.
follows = nearestNeighborMatchingWRMF_Modules.loadFollows(follows_data)
follows = follows[follows.created_at<=period]
follows = follows[['auserID','tuserID','created_at']]
follows = follows[follows.created_at<=period]
follows['bad_match'] = 1
follows = follows[['auserID','tuserID','bad_match']]

# you need to drop user id as well before you 
sampleB = matching_sample.copy()
sampleA = matching_sample.copy()

# Compute variance
thetas = np.var(matching_sample.loc[:,matching_sample.columns != 'auserID'], axis=0, ddof=1)
counter = 0

while len(sampleA) > 0:
       
    logging.info("Remaining Agents: %d" % (len(sampleA)))
    
    # rows of distance matrix at each iteration
    subSampleA = sampleA.loc[0:batch_size_A,:]
    sampleA = sampleA.loc[batch_size_A+1:,:].reset_index(drop=True)
    
    totalMatches = []        
    
    while len(sampleB) > 0:
        
        # columns of distance matrix at each iteration
        subSampleB = sampleB.loc[0:batch_size_B,:]     
        sampleB = sampleB.loc[batch_size_B+1:,:].reset_index(drop=True)
        
        A = subSampleA.loc[:,subSampleA.columns != 'auserID'].as_matrix()
        B = subSampleB.loc[:,subSampleB.columns != 'auserID'].as_matrix() 
        # compute inverse-variance weighted distances
        pairwise_distances = distance.cdist(A, B, 'seuclidean', V=thetas)        

        # Get the column indices of top num_per closest users for each user so far.
        # The row index gives you auser, column index a2user.
        num_cols = np.shape(pairwise_distances)[1]
        current_num_per = min(len(B),numNearestNeighbors)

        min_distances_arg = np.argpartition(pairwise_distances,current_num_per)[:,:current_num_per]
        min_distances_arg = min_distances_arg.flatten()       
        
        closest_a2_users = subSampleB['auserID'].values[min_distances_arg]
        closest_a2_users = np.transpose(closest_a2_users)
        
        min_distances = np.partition(pairwise_distances,current_num_per)[:,:current_num_per]
        min_distances = min_distances.flatten()
        min_distances = np.transpose(min_distances)
        
        currentUsers = subSampleA[['auserID']].loc[subSampleA[['auserID']].index.repeat(current_num_per)].reset_index(drop=True)       
        currentUsers['a2userID'] = closest_a2_users
        currentUsers['distance'] = min_distances
        
        # Drop users who are directly connected to each other in the social network
        currentUsers = nearestNeighborMatchingWRMF_Modules.dropFollows(currentUsers,follows)
        
        if len(totalMatches)==0:
            totalMatches = currentUsers.copy()           
        else:
            totalMatches = pd.concat([totalMatches,currentUsers],axis=0,ignore_index=True)
            
        totalMatches = totalMatches[['auserID','a2userID','distance']]
        
        # Sort by auserID, Distance
        totalMatches.sort_values(by=['auserID','distance'],inplace=True)
        totalMatches = totalMatches.groupby('auserID').head(numNearestNeighbors).reset_index()
        totalMatches = totalMatches[['auserID','a2userID','distance']]
        
    sampleB = matching_sample.copy()
    # save data
    nearestNeighborMatchingWRMF_Modules.saveData(totalMatches,counter,output_name)
    counter += 1