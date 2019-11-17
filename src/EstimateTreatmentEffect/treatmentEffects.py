# -*- coding: utf-8 -*-
"""
Estimate the treatment effect, for different numbers of starring followees.
"""

import pandas as pd
import numpy as np

def computeBootStrapSE(df,pigeon,nSims=100):
    thetas = np.array([])
    repos = df.repoID.unique()
    users = df.auserID.unique()

    for i in range(0,nSims):
        
        if pigeon==True:
            sub_repos = np.random.choice(repos,np.ceil(0.5*len(repos)),replace=False)
            sub_users = np.random.choice(users,np.ceil(0.5*len(users)),replace=False)
            current = df[(df.auserID.isin(sub_users))&(df.repoID.isin(sub_repos))]
        else:
            current = df.sample(frac=1,replace=True)

        current_p = current.a_adopts.mean()
        current_q = current.a2_adopts.mean()
        
        if current_q>0:
            theta = current_p/current_q
        elif (current_q==0)&(current_p>0):
            theta = np.inf
        elif (current_q==0)&(current_p==0):
            theta = np.nan
            
        thetas = np.append(thetas,theta)
        
    return np.percentile(thetas,2.5), np.percentile(thetas,97.5)

############ PARAMETERS TO SET
matches_dir = ""
# Whether or not to use pigeonhole bootstrap
pigeon = False
# Range of time periods
periods = range(1,11)
numFactors=100
# Number of different treatments.
treatments = range(1,4)
############

svd_means1, svd_means2, svd_means3 = [], [], []
svd_lower1, svd_lower2, svd_lower3 = [], [], []
svd_upper1, svd_upper2, svd_upper3  = [], [], []

for period in periods:
    
    print period
    
    svd = pd.read_csv(matches_dir+'matched_sample_WRMF_'+str(period)+".csv")

    svd['a_adopts'] = svd['a_adopted_at']==svd['a_adopted_at']
    svd['a2_adopts'] = svd['a2_adopted_at']==svd['a2_adopted_at']
    svd['a_adopts'] = svd['a_adopts'].astype(int)
    svd['a2_adopts'] = svd['a2_adopts'].astype(int)   
    svd = svd[svd.treatment_num<=treatments[-1]]

    svd_all = pd.DataFrame()
    
    for treatment in treatments:

        N = len(svd[svd.treatment_num==treatment])
        print treatment, N
    
        svd_p = svd[svd.treatment_num==treatment].a_adopts.mean()
        svd_q = svd[svd.treatment_num==treatment].a2_adopts.mean()
        
        if svd_q>0:
            svd_theta = svd_p/svd_q
        elif (svd_q==0)&(svd_p>0):
            svd_theta = np.inf
        elif (svd_q==0)&(svd_p==0):
            svd_theta = np.nan
        print svd_theta
        
        svd_lower, svd_upper = computeBootStrapSE(svd[svd.treatment_num==treatment],pigeon)
        
        if treatment==1:
            
            svd_means1.append(svd_theta)
            svd_lower1.append(svd_lower)
            svd_upper1.append(svd_upper)
            
            svd_all['1'] = svd_means1
            svd_all['1+'] = svd_upper1
            svd_all['1-'] = svd_lower1
            
            svd_all['1N'] = N
            
        elif treatment==2:
            
            svd_means2.append(svd_theta)
            svd_lower2.append(svd_lower)
            svd_upper2.append(svd_upper)
            
            svd_all['2'] = svd_means2
            svd_all['2+'] = svd_upper2
            svd_all['2-'] = svd_lower2
            
            svd_all['2N'] = N
            
        elif treatment==3:
            
            svd_means3.append(svd_theta)
            svd_lower3.append(svd_lower)
            svd_upper3.append(svd_upper)            
            
            svd_all['3'] = svd_means3
            svd_all['3+'] = svd_upper3
            svd_all['3-'] = svd_lower3
            
            svd_all['3N'] = N
    
    svd_all['period'] = period
    
    if period==1:
        with open('peerEffectsWRMF.csv', 'w') as f:
            svd_all.to_csv(f,index=False,header=True)      
    else:
        with open('peerEffectsWRMF.csv', 'a') as f:
            svd_all.to_csv(f,index=False,header=False)
            
    svd_means1, svd_means2, svd_means3 = [], [], []
    svd_lower1, svd_lower2, svd_lower3 = [], [], []
    svd_upper1, svd_upper2, svd_upper3  = [], [], []