# -*- coding: utf-8 -*-
"""
Created on Thu Mar 22 16:51:31 2018

@author: christopherrojas
"""
import pandas as pd
import numpy as np
from scipy import sparse
from scipy.spatial import distance

def convertDates(data,frequency,exact,zeroHour='2013-01-01 00:00:00'):
    # Turn datetimes into time periods
    convert = pd.DataFrame()
    convert['created_at'] = list(data)
    convert['created_at'].fillna('1970-01-01T00:00:00Z',inplace=True)
    convert['created_at'] = convert['created_at'].astype('datetime64[s]')   
    convert['zeroHour'] = zeroHour
    convert['zeroHour'] = convert['zeroHour'].astype('datetime64[s]')
    convert['cperiod'] = convert['created_at'] - convert['zeroHour']
    convert['cperiod'] = convert['cperiod'] / np.timedelta64(frequency[0], frequency[1])
    if exact==True:    
        convert['cperiod'] = 1+convert['cperiod'].round(6)
    else:
        convert['cperiod'] = 1+np.floor(convert['cperiod'].round(12))
    return list(convert['cperiod'])

def loadFollows(data_path,exact=False):
    follows = pd.read_csv(data_path)
    follows['created_at'] = convertDates(follows['created_at'],[1,'M'],exact)
    return follows
    
def loadEvents(data_path,exact=False):
    # Load stars
    events = pd.read_csv(data_path) 
    events['created_at'] = convertDates(events['created_at'],[1,'M'],exact)
    return events
    
def loadExitsUsers(data_path,time_after,exact=False):
    exits = pd.read_csv(data_path)
    exits['exited_at'] = convertDates(exits['exited_at'],[1,'M'],exact)
    exits['exited_at'] = exits['exited_at'] + time_after   
    return exits

def loadExperienceUsers(data_path,current_period,exact=False):
    # Use joins data to estimate experience (number of periods)
    # since user first appeared in data, to beginning of period
    joins = pd.read_csv(data_path)
    joins.rename(columns={'joined_at_arch':'joined_at'},inplace=True)
    joins = joins[['userID','joined_at']]
    joins['joined_at'] = convertDates(joins['joined_at'],[1,'M'],exact)
    joins['experience'] = current_period - joins['joined_at']
    
    return joins[['userID','experience']]
       
def loadWRMF_Prefs_User(wrmf_dir, wrmf_filename):
    prefs = pd.read_csv(wrmf_dir+wrmf_filename)
    prefs = normalizeUserFactors(prefs)
    prefs.rename(columns={'userID':'auserID'},inplace=True)
    return prefs

def loadLangPrefs(langs_dir, langs_filename):
    prefs = pd.read_csv(langs_dir+langs_filename)
    prefs.rename(columns={'userID':'auserID'},inplace=True)
    return prefs
    
def loadActsPrefs(acts_dir, acts_filename, acts_user_id_mapping, acts_repo_id_mapping):
    prefs = sparse.load_npz(acts_dir+acts_filename)
    user_mapping = pd.read_csv(acts_dir + acts_user_id_mapping)
    repo_mapping = pd.read_csv(acts_dir + acts_repo_id_mapping)
    
    user_mapping.rename(columns={'userID':'auserID'},inplace=True)
    return prefs, user_mapping, repo_mapping



def userFactorVariances(user_factors):
    
    factor_columns = [str(x) for x in range(0,user_factors.shape[1]-1)]
    variances = user_factors[factor_columns].var()
    return variances

#### WRMF User factors have magnitude proportional to number of events
#### We record this separately, so normalize by dividing by variance
    
def normalizeUserFactors(user_factors):
    
    factor_columns = [str(x) for x in range(0,user_factors.shape[1]-1)]
    user_factors['norm'] = np.sqrt(np.square(user_factors[factor_columns]).sum(axis=1))
    user_factors['norm'] = 1/user_factors['norm']
    user_factors[factor_columns] = user_factors[factor_columns].multiply(user_factors['norm'],axis="index")
    user_factors.drop('norm',axis=1,inplace=True)
    
    return user_factors 
    
def PrepActs(preferences, acts_repo_mapping, stars_dir, stars_filename, period, valid_users):
    
    # Estimate variances for adoption matrix nearest neighbor matching
    
    stars = loadEvents(stars_dir + stars_filename) 
    stars = stars[stars.created_at < period]
    stars = stars[stars.userID.isin(list(valid_users.auserID.unique()))]
    stars = stars[['userID','repoID']]  
    
    # You could change for weight not equal to 1.
    stars['weight'] = 1
    
    #### Keep only the repos that at least one agent stars
    valid_repos = list(stars.repoID.unique())
    acts_repo_mapping = acts_repo_mapping[acts_repo_mapping.repoID.isin(valid_repos)]
    repo_idx = list(acts_repo_mapping.idx.unique())
    preferences = preferences[repo_idx,:]
    preferences = preferences.transpose()
    
    #################### VARIANCE FOR PREFERENCE COVARIATES
    stars.sort_values(by='repoID',inplace=True)
    # Create the df for the variances for each repo covariate
    repo_vars = stars.groupby('repoID').weight.sum().reset_index()
    repo_vars['N'] = len(valid_users)
    repo_vars['sample_mean'] = repo_vars['weight']/repo_vars['N']
    repo_vars['bernoulli_variance'] = (repo_vars['sample_mean'])*(1-repo_vars['sample_mean'])
    repo_vars['variance'] = repo_vars['N']*repo_vars['bernoulli_variance']
    repo_vars['inv_var'] = 1/repo_vars['variance'] 
    
    repo_vars = pd.merge(acts_repo_mapping, repo_vars, on = 'repoID', how='left')    
    repo_vars.sort_values(by='idx',inplace=True)
    
    repo_vars_diag = repo_vars['inv_var'].values
    repo_vars = sparse.diags(repo_vars_diag,0,format="csr")
    ####################
    
    return preferences, repo_vars

def PairwiseDistDense(sample, currentSampleA, currentSampleB): 
    
    # Estimate distance for wrmf or language vectors
    
    subsampleA = sample[sample.auserID.isin(list(currentSampleA.auserID.unique()))]
    subsampleB = sample[sample.auserID.isin(list(currentSampleB.auserID.unique()))]

    subsampleA = subsampleA.loc[:, (subsampleA != 0).any(axis=0)] 
    subsampleB = subsampleB.loc[:, (subsampleB != 0).any(axis=0)]
    
    # cdist can only include non-zero columns, so drop any zero columns
    colsA = set(subsampleA.columns)   
    colsB = set(subsampleB.columns)
    cols = list(colsA & colsB)
    subsampleA = subsampleA[cols]
    subsampleB = subsampleB[cols]         
    
    A = subsampleA.loc[:,subsampleA.columns != 'auserID'].as_matrix()
    B = subsampleB.loc[:,subsampleB.columns != 'auserID'].as_matrix() 
    
    # estimate variance from whole sample
    current_vars = sample[cols]
    current_vars = np.var(current_vars.loc[:,current_vars.columns != 'auserID'], axis=0, ddof=1)    
    
    # compute inverse-variance weighted distances
    pairwise_distances = distance.cdist(A, B, 'seuclidean', V=current_vars) 

    return pairwise_distances
    
def PairwiseDistSparse(preferences,acts_user_mapping, currentSampleA, 
                       currentSampleB, other_attributes, thetas_prefs):
    
    # combine adoption matrix distance and other attribute distances
    
    acts_distance_squared = activitiesDistance(currentSampleA, currentSampleB, acts_user_mapping,
                                  preferences, thetas_prefs) #squared distance
    
    otherAttributes_distance = PairwiseDistDense(other_attributes, currentSampleA,
                                                 currentSampleB)
                                        
    otherAttributes_distance_squared = np.square(otherAttributes_distance)

    pairwise_distances = acts_distance_squared + otherAttributes_distance_squared

    return pairwise_distances
    
def activitiesDistance(agent1_ids, agent2_ids, user_mapping, likes, repo_vars):
    
    # estimate distance with sparse binary adoption matrix
    
     # Get the matrix of likes for each subset of agents
    agent1_idx = pd.merge(agent1_ids, user_mapping, on='auserID', how='left')
    agent1_idx = list(agent1_idx.idx)
    agent2_idx = pd.merge(agent2_ids, user_mapping, on='auserID', how='left')
    agent2_idx = list(agent2_idx.idx)
        
    agent1_likes = likes[agent1_idx,]
    agent1_likes_T = agent1_likes.transpose()
    
    agent2_likes = likes[agent2_idx,]
    agent2_likes_T = agent2_likes.transpose()

    # Compute the weighted number of likes by each
    num_both1 = repo_vars.dot(agent1_likes_T)
    num_both1 = agent1_likes.dot(num_both1)
    num_both1 = num_both1.diagonal()

    num_both2 = repo_vars.dot(agent2_likes_T)
    num_both2 = agent2_likes.dot(num_both2)
    num_both2 = num_both2.diagonal()

    num_both1 = np.array([num_both1,]*len(agent2_idx))
    num_both1 = num_both1.transpose()
    num_both2 = np.array([num_both2,]*len(agent1_idx))
    
    # Compute the weighted number of likes by both  
    num_both = repo_vars.dot(agent2_likes_T)

    num_both = agent1_likes.dot(num_both)
    num_both = num_both.todense()
    num_both = -2*num_both

    acts_distance = num_both1 + num_both2 + num_both
    acts_distance = np.asarray(acts_distance)
    
    return acts_distance    
    
def CountA_TreatmentsOutcomes(aUsersChunk, follows, leader_events, follower_events, period, verbose, verbose_output_dir, verbose_output_filename, iterator, maxTreatment=3):
    
    # Estimate treatment intensity and adoption outcomes for given agents
    
    currentIDs = list(aUsersChunk.auserID.unique())
    currentFollows = follows[follows.auserID.isin(currentIDs)]
        
    # Merge on the treatments, stars by leaders
    treatments = pd.merge(currentFollows,leader_events,on=['tuserID'],how='left')
    
    # Merge on the outcomes, stars by followers
    treatments_adoptions = pd.merge(treatments,follower_events,on=['auserID','repoID'],how='left')

    # Only keep the observation if several conditions are met:
    # First, the follower has not starred the repo by the beginning of the period
    treatments_adoptions = treatments_adoptions[~(treatments_adoptions.acreated_at < period)] 
    # Second, the follower has not starred it before link creation
    treatments_adoptions = treatments_adoptions[~(treatments_adoptions.acreated_at < treatments_adoptions.created_at)]   
    # Third, the follower has not starred it before the leader
    treatments_adoptions = treatments_adoptions[~(treatments_adoptions.acreated_at < treatments_adoptions.tcreated_at)]
    treatments_adoptions.rename(columns={'acreated_at':'a_adopted_at'},inplace=True)
    
    if verbose==True:
        adopting_leaders = treatments_adoptions[['auserID','tuserID','repoID']]    
        saveData(adopting_leaders, iterator, verbose_output_dir + verbose_output_filename)
        
    # Add the cumulative number of leader adoptions, which is number of treatments
    treatments_adoptions.sort_values(by=['auserID','tcreated_at'],inplace=True)
    treatments_adoptions['treatment_num'] = treatments_adoptions.groupby(['auserID','repoID']).tcreated_at.cumcount()
    treatments_adoptions['treatment_num'] = treatments_adoptions['treatment_num']+1
    treatments_adoptions = treatments_adoptions[['auserID','repoID','a_adopted_at','tcreated_at','treatment_num']]
        
    # The maximum number of treatments
    treatments_adoptions = treatments_adoptions[treatments_adoptions.treatment_num<=maxTreatment]

    # Set the treatment time equal to the last time the agent is treated on the item
    treatment_times = treatments_adoptions.groupby(['auserID','repoID']).tcreated_at.max().reset_index()
    treatments_adoptions.drop('tcreated_at',axis=1,inplace=True)
    treatments_adoptions = pd.merge(treatments_adoptions,treatment_times,on=['auserID','repoID'],how='left')        
    
    return treatments_adoptions
    
def RemoveBadMatches(a_a2_treat_outs, follows, leader_events, follower_events, period):
    
    # Remove invalid treatments (e.g. follower starred before leader)
    
    currentIDs = list(a_a2_treat_outs['a2userID'].unique())
    currentFollows = follows[follows.auserID.isin(currentIDs)]   
    
    bad_treatments = pd.merge(currentFollows, leader_events,on=['tuserID'],how='left')
    # Merge on the outcomes, stars by followers
    bad_treatments_adoptions = pd.merge(bad_treatments,follower_events,on=['auserID','repoID'],how='left')

    # Only keep the observation if several conditions are met:
    # First, the follower has not starred the repo by the beginning of the period
    bad_treatments_adoptions = bad_treatments_adoptions[~(bad_treatments_adoptions.acreated_at < period)] 
    # Second, the follower has not starred it before link creation
    bad_treatments_adoptions = bad_treatments_adoptions[~(bad_treatments_adoptions.acreated_at < bad_treatments_adoptions.created_at)]   
    # Third, the follower has not starred it before the leader
    bad_treatments_adoptions = bad_treatments_adoptions[~(bad_treatments_adoptions.acreated_at < bad_treatments_adoptions.tcreated_at)]
    bad_treatments_adoptions.rename(columns={'acreated_at':'a_adopted_at'},inplace=True)
    
    bad_treatments_adoptions = bad_treatments_adoptions.groupby(['auserID','repoID']).tuserID.nunique().reset_index()
    bad_treatments_adoptions.rename(columns={'tuserID':'treatment_num2', 'auserID':'a2userID'},inplace=True)
    
    if len(bad_treatments) > 0:
        a_a2_treat_outs = pd.merge(a_a2_treat_outs,bad_treatments_adoptions,on=['a2userID','repoID'],how='left')
        a_a2_treat_outs = a_a2_treat_outs[~(a_a2_treat_outs.treatment_num2 >= a_a2_treat_outs.treatment_num)]
        a_a2_treat_outs.drop('treatment_num2',axis=1,inplace=True)
    
    return a_a2_treat_outs
    
def AddOutcomes(a_a2_treat_outs, follower_events, period):
    
    # Add counterfactual outcome
    
    potentialMatchFollowerEvents = follower_events.rename(columns={'auserID':'a2userID','acreated_at':'a2_adopted_at'})
    a_a2_treat_outs = pd.merge(a_a2_treat_outs,potentialMatchFollowerEvents,on=['a2userID','repoID'],how='left')
        
    # Drop if the matched user adopts before start of current period, or most recent treatment time, even if they are untreated        
    a_a2_treat_outs = a_a2_treat_outs[~(a_a2_treat_outs.a2_adopted_at < period)]        
    a_a2_treat_outs = a_a2_treat_outs[~(a_a2_treat_outs.a2_adopted_at < a_a2_treat_outs.tcreated_at)]
    
    return a_a2_treat_outs
    
def dropFollows(currentUsers,follows):
    
    # Remove observation if treated and counterfactual agent are connected in social network
    
    follows.columns = ['auserID','a2userID','bad_match']
    currentUsers = pd.merge(currentUsers,follows,on=['auserID','a2userID'],how='left')
    currentUsers = currentUsers[currentUsers.bad_match != 1]
    currentUsers.drop('bad_match',axis=1,inplace=True)
    follows.columns = ['a2userID','auserID','bad_match']  
    currentUsers = pd.merge(currentUsers,follows,on=['auserID','a2userID'],how='left')
    currentUsers = currentUsers[currentUsers.bad_match != 1]
    currentUsers.drop('bad_match',axis=1,inplace=True)        
    follows.columns = ['auserID','a2userID','bad_match']
       
    return currentUsers
     
def saveData(data,counter,output_name):
    # save/append the fully processed panel chunk
    if counter==0:
        with open(output_name, 'w') as f:
            data.to_csv(f,index=False,header=True)
    else:
        with open(output_name, 'a') as f:
            data.to_csv(f,index=False,header=False)