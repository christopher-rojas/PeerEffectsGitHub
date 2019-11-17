# -*- coding: utf-8 -*-
"""
Match nearest neighbors based on preferences and other attributes (possibly only
other attributes) using inverse-variance weighted euclidean distance.
"""

import pandas as pd
import numpy as np
import nearestNeighborMatching_Modules
import logging

################### PARAMETERS TO ENTER
period = 1 # Time period (months in paper)
numNearestNeighbors = 51 # Number of nearest neighbors to keep for each agent (+ 1), equals 50 in paper

follows_dir = ""
follows_filename = "follows.csv"

stars_dir = ""
stars_filename = "stars.csv"

baseline_dir = "" # The output from baseline.py
baseline_filename = "baseline.csv"

output_dir = ""
output_filename = "nearest_neighbors.csv"

pref_type = "wrmf" # Preference type: Must be either wrmf, langs, acts, none.

wrmf_dir = "" # The output from learnFactoVectors.py
wrmf_filename = "wrmf_user_factors.csv"

langs_dir = "" # The output from languageVectors.py
langs_filename = "langs.csv"

acts_dir = "" # The output from adoptionMatrix.py
acts_filename = "adoptions_matrix.npz"
acts_user_mapping_filename = "uid_to_idx.csv"
acts_repo_mapping_filename = "rid_to_idx.csv"
#################### MODIFY BELOW THIS LINE AT YOUR OWN RISK

logging.basicConfig(filename='nearest_neighbors.log',level=logging.INFO,format='%(asctime)s %(message)s')

batch_size_A = 1000 # (batch_size_A * batch_size_B) is size of distance matrix each iteration
batch_size_B = 10000 

# Load the non-preference-based attributes for matching
baseline = pd.read_csv(baseline_dir + baseline_filename)
baseline.sort_values(by=['auserID'],inplace=True)

# Load the preference attributes for matching
if pref_type == 'wrmf':
    preferences = nearestNeighborMatching_Modules.loadWRMF_Prefs_User(wrmf_dir, wrmf_filename)
    
elif pref_type =='langs':
    preferences = nearestNeighborMatching_Modules.loadLangPrefs(langs_dir, langs_filename)    

elif pref_type == 'acts':
    preferences, acts_user_mapping, acts_repo_mapping = nearestNeighborMatching_Modules.loadActsPrefs(acts_dir, acts_filename,
                                                                                                      acts_user_mapping_filename,
                                                                                                      acts_repo_mapping_filename)
                                                                      
# Load the follows
# Keep users who followed each other. We will remove them from nearest neighbors at the end.
follows = nearestNeighborMatching_Modules.loadFollows(follows_dir+follows_filename)
follows = follows[follows.created_at<=period]
follows = follows[['auserID','tuserID','created_at']]
follows = follows[follows.created_at<=period]
follows['bad_match'] = 1
follows = follows[['auserID','tuserID','bad_match']]

# you need to drop user id as well before you 
if (pref_type == 'wrmf') | (pref_type == 'langs'):
    
    matching_sample = pd.merge(preferences, baseline, on =['auserID'], how='inner')   

    sampleB = matching_sample[['auserID']]
    sampleA = matching_sample[['auserID']]
    
    sampleB.sort_values(by='auserID',inplace=True)
    sampleA.sort_values(by='auserID',inplace=True)
    
elif pref_type == 'acts':
    
    sampleA = pd.merge(acts_user_mapping[['auserID']], baseline[['auserID']], on='auserID', how='inner')
    sampleB = pd.merge(acts_user_mapping[['auserID']], baseline[['auserID']], on='auserID', how='inner')
    
    sampleA.sort_values(by='auserID',inplace=True)
    sampleA.reset_index(inplace=True,drop=True)
    
    sampleB.sort_values(by='auserID',inplace=True)
    sampleB.reset_index(inplace=True,drop=True)
    
    baseline = baseline[baseline.auserID.isin(list(sampleA.auserID.unique()))]
    preferences, thetas_prefs  = nearestNeighborMatching_Modules.PrepActs(preferences, acts_repo_mapping, stars_dir, stars_filename, period, sampleA)
    
else:
    sampleB = baseline[['auserID']]
    sampleA = baseline[['auserID']]
    
    sampleA.sort_values(by='auserID',inplace=True)
    sampleA.reset_index(inplace=True,drop=True)
    
    sampleB.sort_values(by='auserID',inplace=True)
    sampleB.reset_index(inplace=True,drop=True)
    
    matching_sample = baseline.sort_values(by='auserID')

counter = 0
remainingSampleA = sampleA.copy()

debug_iter = 0 

while len(remainingSampleA) > 0:
       
    logging.info("Remaining Agents: %d" % (len(sampleA)))
    
    # rows of distance matrix at each iteration
    currentSampleA = remainingSampleA.loc[0:batch_size_A,:]
    remainingSampleA = remainingSampleA.loc[batch_size_A+1:,:].reset_index(drop=True)
    
    totalMatches = []        
    remainingSampleB = sampleB.copy()   
    
    while len(remainingSampleB) > 0:
        
        logging.info(str(len(remainingSampleB)))
        
        # columns of distance matrix at each iteration
        currentSampleB = remainingSampleB.loc[0:batch_size_B,:]  
        remainingSampleB = remainingSampleB.loc[batch_size_B+1:,:].reset_index(drop=True)
        
        # Need a function call here that returns the pairwise distances
        if (pref_type == 'wrmf') | (pref_type=='langs') | (pref_type=='none'):
            
            pairwise_distances = nearestNeighborMatching_Modules.PairwiseDistDense(matching_sample, currentSampleA,
                                                                                  currentSampleB)       

        elif (pref_type == 'acts'):
            
            pairwise_distances = nearestNeighborMatching_Modules.PairwiseDistSparse(preferences, acts_user_mapping, 
                                                                                    currentSampleA, currentSampleB,
                                                                                    baseline, thetas_prefs)                                                          
                                                                                    
                                                                          
        # Get the column indices of top num_per closest users for each user so far.
        # The row index gives you auser, column index a2user.
        num_cols = np.shape(pairwise_distances)[1]
        current_num_per = min(num_cols,numNearestNeighbors)
        
        min_distances_arg = np.argpartition(pairwise_distances,current_num_per)[:,:current_num_per]        
        min_distances_arg = min_distances_arg.flatten()               
        
        closest_a2_users = currentSampleB['auserID'].values[min_distances_arg]
        closest_a2_users = np.transpose(closest_a2_users)    
        
        min_distances = np.partition(pairwise_distances,current_num_per)[:,:current_num_per]
        min_distances = min_distances.flatten()
        min_distances = np.transpose(min_distances)       
        
        currentUsers = currentSampleA[['auserID']].loc[currentSampleA[['auserID']].index.repeat(current_num_per)].reset_index(drop=True)       
        currentUsers['a2userID'] = closest_a2_users
        currentUsers['distance'] = min_distances
        
        # Drop users who are directly connected to each other in the social network
            # They cannot be matched
        currentUsers = nearestNeighborMatching_Modules.dropFollows(currentUsers,follows)
        
        if len(totalMatches)==0:
            totalMatches = currentUsers.copy()           
        else:
            totalMatches = pd.concat([totalMatches,currentUsers],axis=0,ignore_index=True)
            
        totalMatches = totalMatches[['auserID','a2userID','distance']]
        
        # Sort by auserID, Distance
        totalMatches.sort_values(by=['auserID','distance'],inplace=True)
        totalMatches = totalMatches.groupby('auserID').head(numNearestNeighbors).reset_index()
        totalMatches = totalMatches[['auserID','a2userID','distance']]
    
    # save data
    nearestNeighborMatching_Modules.saveData(totalMatches,counter,output_dir+output_filename)  
    counter += 1