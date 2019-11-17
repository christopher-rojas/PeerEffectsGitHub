# -*- coding: utf-8 -*-
"""
For each treated observation (agent-repo), record the treated agent's adoption time
during the period (empty if not adopted). Then match the treated agent with the closest valid agent,
meaning an agent who has not adopted by treatment time, and is not treated by end of period.
Finally, record the matched, non-treated agent's adoption time during the period.
"""

import nearestNeighborMatching_Modules
import pandas as pd
import numpy as np
import logging

####################### PARAMETERS THAT NEED TO BE SET.
period = 1 # time period
stars_dir = '' # directory where the formatted stars data (step 2) is stored)
stars_filename = 'stars.csv'
follows_dir = '' # directory where the formatted follows data (step 2) is stored
follows_filename = 'follows.csv'
neighbors_dir = '' # directory where the nearest neighbors data (step 4) is stored
neighbors_filename = ''
duration = 3 # the number of past periods to include in treatment
output_dir = ''
output_filename = 'matched_sample_outcomes.csv'

# May create an additional output table with more info
# about adopting leaders.
# Needed for some heterogeneity analysis in our paper.
verbose = False  # If True then you need to add directory and filename
verbose_output_dir = ''
verbose_output_filename = 'matches_leader_adoptions.csv'
####################### MODIFY BELOW THIS LINE AT YOUR OWN RISK

logging.basicConfig(filename='matchTreatments.log',level=logging.INFO,format='%(asctime)s %(message)s')

####################### LOAD THE MATCHED PAIRS
matched_pairs = pd.read_csv(neighbors_dir+neighbors_filename)
matched_pairs = matched_pairs[matched_pairs['auserID'] != matched_pairs['a2userID']] # the closest neighbor is oneself
ausers = list(matched_pairs.auserID.unique())
a2users = list(matched_pairs.a2userID.unique())
#######################

####################### LOAD THE FOLLOWS
# Start with all the follows (need the exact datetimes, so set exact=True)
follows = nearestNeighborMatching_Modules.loadFollows(follows_dir+follows_filename,exact=True)
follows = follows[(follows.auserID.isin(ausers+a2users))]
follows = follows[(follows.created_at<period+1)]
#######################

####################### LOAD THE STARS
follower_events = nearestNeighborMatching_Modules.loadEvents(stars_dir+stars_filename,True)
follower_events = follower_events[['userID','repoID','created_at']]
follower_events = follower_events[follower_events.created_at<period+1]

leader_events = follower_events.copy()

leader_events = leader_events[(leader_events.created_at>=period-duration)]
follower_events = follower_events[follower_events.userID.isin(ausers+a2users)] 

# auser is the follower, tuser is the leader
leader_events.rename(columns={'userID':'tuserID','created_at':'tcreated_at'},inplace=True)
follower_events.rename(columns={'userID':'auserID','created_at':'acreated_at'},inplace=True)

# Need only the dyads with leaders who starred something
follows = follows[follows.tuserID.isin(list(leader_events.tuserID.unique()))]
#######################

####################### COMPUTE THE TREATMENTS AND ADOPTION OUTCOMES IN BATCHES OF AUSERS
aUsers = pd.DataFrame(ausers) # ausers are the agents who may be treated
aUsers.columns=['auserID']

finishedMatchingAll = False
total = len(aUsers)
ausersIterator = 0

logging.info("Number of Agents: %d" % (len(aUsers)))

while finishedMatchingAll==False:

    logging.info("Iteration: %d" % (ausersIterator))
    
    # Grab the current batch of users
    if (ausersIterator+1)*10000 < total:
        aUsersChunk = aUsers.iloc[ausersIterator*10000:(ausersIterator+1)*10000]
    else:
        aUsersChunk = aUsers.iloc[ausersIterator*10000:]
        finishedMatchingAll=True
    
    # First, get the all of the potential treatments and outcomes for the actual treated agents
    a_treat_out = nearestNeighborMatching_Modules.CountA_TreatmentsOutcomes(aUsersChunk, 
                                                                               follows, 
                                                                               leader_events, 
                                                                               follower_events,
                                                                               period,
                                                                               verbose,
                                                                               verbose_output_dir,
                                                                               verbose_output_filename,
                                                                               ausersIterator)    
    a_treat_out['observation'] = a_treat_out.index
    
    ###################### FIND CLOSEST NON-TREATED AGENT FOR EACH TREATED AGENT-REPO
    aMatchesChunk = matched_pairs[matched_pairs.auserID.isin(aUsersChunk)]    
    min_ranking = 1
    max_ranking = matched_pairs.ranking.max() # M=50 in the paper
    finishedMatching = False
    shift = 1 # increment the ranking of nearest neighbors
    
    while finishedMatching==False:
        
        logging.info("Ranking: %d" % (min_ranking))
        
        # Grab the current ranking of nearest neighbors
        if (min_ranking+shift)==max_ranking:
            aMatchesChunk = matched_pairs[matched_pairs.ranking==min_ranking]
            finishedMatching=True
        else:
            aMatchesChunk = matched_pairs[(matched_pairs.ranking>=min_ranking)&(matched_pairs.ranking<min_ranking+shift)]
        
        # Take the unmatched ones, so far, and try to match them
        if min_ranking==1:
            final_matches = np.nan
            a_a2_treat_outs = a_treat_out.copy()
        else:
            a_a2_treat_outs  = a_treat_out[~(a_treat_out.observation.isin(list(final_matches.observation.unique())))]

        # Get all of the closest possible matches for user-repos who haven't been matched yet.      
        a_a2_treat_outs = pd.merge(a_a2_treat_outs,aMatchesChunk,on='auserID',how='left')
        # If there are no more matches for a treated agent-repo, then drop it.
        a_a2_treat_outs = a_a2_treat_outs[a_a2_treat_outs.a2userID==a_a2_treat_outs.a2userID]
        
        # Remove the matches who are not valid for this item (e.g. starred it before the current period)
        a_a2_treat_outs = nearestNeighborMatching_Modules.RemoveBadMatches(a_a2_treat_outs, 
                                                                               follows,
                                                                               leader_events,
                                                                               follower_events,
                                                                               period)
        
        
        # Make sure a2 doesn't adopt before t, and add time of a2 adoption
        a_a2_treat_outs = nearestNeighborMatching_Modules.AddOutcomes(a_a2_treat_outs,                                         
                                                                          follower_events,
                                                                          period)
                                                                          
        a_a2_treat_outs = a_a2_treat_outs[['observation','auserID','a2userID','repoID','treatment_num', 'a_adopted_at','a2_adopted_at']]
        a_a2_treat_outs.drop_duplicates(subset=['auserID','repoID','treatment_num'],inplace=True,keep='first')
        
        # Add the valid matched observations to a dataframe, then iterate to the next ranking
        if min_ranking==1:    
            final_matches = a_a2_treat_outs.copy()
        else:
            final_matches = pd.concat([final_matches, a_a2_treat_outs])
        
        if len(final_matches)==len(a_treat_out):
            finishedMatching=True
        
        min_ranking += shift
    
    final_matches.drop('observation',axis=1,inplace=True)   
    final_matches.drop_duplicates(subset=['auserID','repoID','treatment_num'],inplace=True,keep='first')    
    
    nearestNeighborMatching_Modules.saveData(final_matches, ausersIterator, output_dir+output_filename)
   
    ausersIterator += 1