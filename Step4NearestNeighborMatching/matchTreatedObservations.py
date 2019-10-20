# -*- coding: utf-8 -*-
"""
Created on Wed May 30 12:46:31 2018

@author: christopherrojas

For each treated agent-repo, match treated agent with the closest non-treated agent.
"""

import nearestNeighborMatchingWRMF_Modules
import pandas as pd
import numpy as np
        
####################### OPTIONS THAT NEED TO BE SET.
period = 1
wrmf_dir = ''
data_dir = ''
# Enter the number of periods before the current that the treatment is still active.
duration = 3
####################### 

output_filename = 'matched_sample_WRMF_'+str(period)+".csv"

#######################

####################### LOAD THE MATCHED PAIRS
matched_pairs = pd.read_csv('nearest_neighbors_WRMF_'+str(period)+'.csv')

matched_pairs = matched_pairs[matched_pairs['auserID'] != matched_pairs['a2userID']]
ausers = list(matched_pairs.auserID.unique())
a2users = list(matched_pairs.a2userID.unique())
#######################

####################### LOAD THE CURRENT FOLLOWS
# Start with all the follows
allFollows = nearestNeighborMatchingWRMF_Modules.loadFollows(data_dir,True)
# We keep dyads in which the leader hasn't exited by the start of the current period.
# The treated agents and matched agents we already verified did not exit by start of period.
user_exits = nearestNeighborMatchingWRMF_Modules.loadExitsUsers(data_dir,6)
user_exits = user_exits[user_exits.exited_at >= period]
non_exit_users = list(user_exits['userID'].unique())

allFollows = allFollows[(allFollows.auserID.isin(ausers+a2users))]
allFollows = allFollows[allFollows.tuserID.isin(non_exit_users)]
allFollows = allFollows[(allFollows.created_at<period+1)]

user_exits = np.nan
print len(allFollows)
#######################

####################### LOAD THE EVENTS DATA.
follower_events = nearestNeighborMatchingWRMF_Modules.loadEvents(data_dir,True)
follower_events = follower_events[['userID','repoID','created_at']]
follower_events = follower_events[follower_events.created_at<period+1]

leader_events = follower_events.copy()

leader_events = leader_events[(leader_events.created_at>=period-duration)]
follower_events = follower_events[follower_events.userID.isin(ausers+a2users)] 

leader_events = leader_events[leader_events.userID.isin(non_exit_users)]

non_exit_users = np.nan
print len(follower_events), len(leader_events)

leader_events.rename(columns={'userID':'tuserID','created_at':'tcreated_at'},inplace=True)
follower_events.rename(columns={'userID':'auserID','created_at':'acreated_at'},inplace=True)
#######################

# Need only the dyads with leaders who starred something
allFollows = allFollows[allFollows.tuserID.isin(list(leader_events.tuserID.unique()))]
print len(allFollows)

####################### COMPUTE THE TREATMENT NUMBER FOR EACH TREATED USER-REPOS IN CURRENT BATCH
coreUsers = pd.DataFrame(ausers)
coreUsers.columns=['auserID']

finishedMatchingAll = False
total = len(coreUsers)
coreChunk = 0
print "Total Core Users: " + str(total)

while finishedMatchingAll==False:

    subCoreUsers = np.nan
    print "Core Users Iterator: " + str(coreChunk)
    
    # Grab the current batch of dyads
    if (coreChunk+1)*10000 < total:
        subCoreUsers = coreUsers.iloc[coreChunk*10000:(coreChunk+1)*10000]
    else:
        subCoreUsers = coreUsers.iloc[coreChunk*10000:]
        finishedMatchingAll=True
    
    currentCoreIDs = list(subCoreUsers.auserID.unique())
    subCoreFollows = allFollows[allFollows.auserID.isin(currentCoreIDs)]
    
    subCoreLeaders = list(subCoreFollows.tuserID.unique())
    subCoreLeaderEvents = leader_events[leader_events.tuserID.isin(subCoreLeaders)]
    subCoreFollowerEvents = follower_events[follower_events.auserID.isin(currentCoreIDs)]

    # First get the treatments and outcomes for the core users.
    most_recent = nearestNeighborMatchingWRMF_Modules.CountLeaderAdoption(subCoreFollows,subCoreLeaderEvents,subCoreFollowerEvents,True,period)
    most_recent.reset_index(inplace=True,drop=True)    
    most_recent['observation'] = most_recent.index
    
    ###################### COMPUTE CLOSEST NON-TREATED AGENT FOR EACH TREATED AGENT-REPO IN THE BATCH.
    subCoreMatches = matched_pairs[matched_pairs.auserID.isin(currentCoreIDs)]    
    min_ranking = 1
    max_ranking = subCoreMatches.ranking.max()
    finishedMatching = False
    shift = 1
    
    while finishedMatching==False:
    
        print str(min_ranking), str(min_ranking+shift)
        if (min_ranking+shift)>=max_ranking:
            current_subCoreMatches = subCoreMatches[subCoreMatches.ranking>=min_ranking]
            finishedMatching=True
        else:
            current_subCoreMatches = subCoreMatches[(subCoreMatches.ranking>=min_ranking)&(subCoreMatches.ranking<min_ranking+shift)]
        
        # Start by taking the unmatched ones, so far.
        if min_ranking==1:
            final_matches = np.nan
            treatment_controls = most_recent.copy()
            print len(treatment_controls)
        else:
            treatment_controls = most_recent[~(most_recent.observation.isin(list(final_matches.observation.unique())))]
            print len(treatment_controls)
        # Get all of the closest possible matches for user-repos who haven't been matched yet.      
        treatment_controls = pd.merge(treatment_controls,current_subCoreMatches,on='auserID',how='left')
        # In case there are no more matches for an agent-repo, then drop it.
        treatment_controls = treatment_controls[treatment_controls.a2userID==treatment_controls.a2userID]
        # Get the treated repos for all of the matches
        potentialMatches = list(treatment_controls['a2userID'].unique())
        
        # Get the events for them, their follows, events for users they follow.
        potentialMatchFollows = allFollows[allFollows.auserID.isin(potentialMatches)]

        potentialMatchLeaders = list(potentialMatchFollows.tuserID.unique())      
        potentialMatchLeaderEvents = leader_events[leader_events.tuserID.isin(potentialMatchLeaders)]
        potentialMatchFollowerEvents = follower_events[follower_events.auserID.isin(potentialMatches)]
        
        if (len(potentialMatchFollows)>0)&(len(potentialMatchLeaderEvents)>0)&(len(potentialMatchFollowerEvents)>0):        
            # Compute the treated repos for each of the matched agents.
                        
            badMatches = nearestNeighborMatchingWRMF_Modules.CountLeaderAdoption(potentialMatchFollows,potentialMatchLeaderEvents,potentialMatchFollowerEvents,False,period)        
            badMatches.rename(columns={'auserID':'a2userID','treatment_num':'treatment_num2'},inplace=True)
            
            treatment_controls = pd.merge(treatment_controls,badMatches,on=['a2userID','repoID'],how='left')
            treatment_controls = treatment_controls[~(treatment_controls.treatment_num2>=treatment_controls.treatment_num)]
            treatment_controls.drop('treatment_num2',axis=1,inplace=True)
        
        # Add the time that the matched user adopts
        potentialMatchFollowerEvents.rename(columns={'auserID':'a2userID','acreated_at':'a2_adopted_at'},inplace=True)
        treatment_controls = pd.merge(treatment_controls,potentialMatchFollowerEvents,on=['a2userID','repoID'],how='left')
        # Drop if the matched user adopts before start of current period, or most recent treatment time, even if they are untreated        
        treatment_controls = treatment_controls[~(treatment_controls.a2_adopted_at<period)]        
        treatment_controls = treatment_controls[~(treatment_controls.a2_adopted_at<treatment_controls.tcreated_at)]
        
        treatment_controls = treatment_controls[['observation','auserID','a2userID','repoID','treatment_num', 'a_adopted_at','a2_adopted_at']]
        treatment_controls.drop_duplicates(subset=['auserID','repoID','treatment_num'],inplace=True,keep='first')
        
        # Now you need to add the finished user-repo observations to a dataframe, then delete current and redo.
        if min_ranking==1:    
            final_matches = treatment_controls.copy()
        else:
            final_matches = pd.concat([final_matches,treatment_controls])
        
        print len(final_matches), len(most_recent)
        
        if len(final_matches)==len(most_recent):
            finishedMatching=True
        
        min_ranking+=shift
    
    final_matches.drop('observation',axis=1,inplace=True)   
    final_matches.drop_duplicates(subset=['auserID','repoID','treatment_num'],inplace=True,keep='first')    
    
    if coreChunk==0:    
        with open(output_filename, 'w') as f:
            final_matches.to_csv(f,index=False,header=True) 
    else:
        with open(output_filename, 'a') as f:
            final_matches.to_csv(f,index=False,header=False)
    coreChunk+=1