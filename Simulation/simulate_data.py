# -*- coding: utf-8 -*-
"""
Simulate adoptions based on influence, homophily and common shocks
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
userFactors.rename(columns={'userID':'auserID'},inplace=True)

# Load the SVD repos
svd_repos = pd.read_csv(svd_directory+'item_factors_'+str(period)+'_100.csv',usecols=['repoID'])
svd_repos = list(svd_repos['repoID'].unique())

# Load the stars and forks
follower_stars = simulation_modules.loadEvents(1,events_directory,exact=True)
follwer_stars = follower_stars[['userID','repoID','created_at']]
follower_stars = follower_stars[(follower_stars.created_at>=period)&(follower_stars.created_at<period+2)]
follower_stars = follower_stars[follower_stars.userID.isin(svd_users)]
follower_stars = follower_stars[follower_stars.repoID.isin(svd_repos)]
print len(follower_stars)

# Load the treatment stars and forks
leader_adoptions = pd.read_csv('/Users/christopherrojas/Workspace/my_thesis/PeerInfluenceAdoption/MethodologyGraphs/Output/simulation_leader_adoptions1.csv')
leader_adoptions.rename(columns={'auserID':'userID'},inplace=True)
leader_adoptions.sort_values(by=['userID','tcreated_at'],inplace=True)
# Load the most-preferred repos
most_preferred = pd.read_csv('/Users/christopherrojas/Workspace/my_thesis/PeerInfluenceAdoption/MethodologyGraphs/Output/user_repo_prefs_11.csv',usecols=['userID','repoID','ranking'])

# Keep at most the first 101 adoptions per agent
follower_stars.sort_values(by=['userID','created_at'],inplace=True)
follower_stars['obs'] = follower_stars.groupby(['userID']).cumcount()
print follower_stars['obs'].min()
follower_stars['obs'] = follower_stars['obs']+1
follower_stars = follower_stars[follower_stars.obs<=101]
print len(follower_stars)

# Load the common shocks
commonshocks = pd.read_csv('/Users/christopherrojas/Workspace/my_thesis/PeerInfluenceAdoption/MethodologyGraphs/Output/simulated_common_shocks1.csv')
commonshocks.rename(columns={'auserID':'userID'},inplace=True)
commonshocks.sort_values(by=['userID','counter'],inplace=True)
print len(commonshocks)

# binomial(n,p,length)
s = np.random.choice([0,1,2],len(follower_stars),p=[0.9,0.05,0.05])
print np.shape(s)
print np.max(s), np.min(s)

follower_stars['influence'] = s
print len(follower_stars[follower_stars.influence==0]),len(follower_stars[follower_stars.influence==1]), len(follower_stars[follower_stars.influence==2])

for iterator in range(1,follower_stars['obs'].max()+1):
    
    print iterator
    
    data = follower_stars[follower_stars.obs==iterator]
    
    data_treat = data[data.influence==2]
    data_treat = data_treat[['userID','created_at','influence']]    
    
    data_commonshocks = data[data.influence==1]   
    data_commonshocks = data_commonshocks[['userID','created_at','influence']]
    
    data_homophily = data[data.influence==0]
    data_homophily = data_homophily[['userID','created_at','influence']]
    
    if len(data_homophily)>0:
        current_ranking = most_preferred.groupby(['userID']).repoID.first().reset_index()       
        data_homophily = pd.merge(data_homophily,current_ranking,on=['userID'],how='left')
        current_ranking = np.nan        
        
    if len(data_commonshocks)>0:
        current_commonshocks = commonshocks.groupby(['userID']).repoID.first().reset_index()
        data_commonshocks = pd.merge(data_commonshocks,current_commonshocks,on=['userID'],how='left')        
        current_commonshocks = np.nan
        
    if len(data_treat)>0:
        data_treat = pd.merge(data_treat,leader_adoptions,on=['userID'],how='left')
        print len(data_treat)        
        data_treat = data_treat[data_treat.tcreated_at<data_treat.created_at]
        print len(data_treat)
        
        if len(data_treat)>0:
            data_treat = data_treat.groupby(['userID','created_at']).tail(1)
    
    print len(data_homophily), len(data_commonshocks), len(data_treat)
    
    data = pd.concat([data_treat,data_homophily,data_commonshocks])
    
    # Pop off the userID-repoID pairs you just added
    pop_off = data[['userID','repoID']]
    pop_off['bad']=1
    
    print len(most_preferred)
    most_preferred = pd.merge(most_preferred,pop_off,on=['userID','repoID'],how='left')
    most_preferred = most_preferred[most_preferred.bad != 1]
    most_preferred.drop(labels='bad',axis=1,inplace=True)
    print len(most_preferred)
    
    print len(commonshocks)
    commonshocks = pd.merge(commonshocks,pop_off,on=['userID','repoID'],how='left')
    commonshocks = commonshocks[commonshocks.bad != 1]
    commonshocks.drop(labels='bad',axis=1,inplace=True)
    print len(commonshocks)
    
    print len(leader_adoptions)
    leader_adoptions = pd.merge(leader_adoptions,pop_off,on=['userID','repoID'],how='left')
    leader_adoptions = leader_adoptions[leader_adoptions.bad != 1]
    leader_adoptions.drop(labels='bad',axis=1,inplace=True)
    print len(leader_adoptions)
    
    # save/append the fully processed panel chunk
    if iterator==1:
        with open(output_directory+'simulated_data'+str(period)+'.csv', 'w') as f:
            data.to_csv(f,index=False,header=True)
    else:
        with open(output_directory+'simulated_data'+str(period)+'.csv', 'a') as f:
            data.to_csv(f,index=False,header=False)
            
    # Iterate and save progress
    iterator+=1
    