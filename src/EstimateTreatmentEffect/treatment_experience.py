# -*- coding: utf-8 -*-
"""
Heterogeneity based on follower experience
"""

import pandas as pd
import numpy as np
import treatment_modules

periods = range(1,11)
numFactors=100
ageGroups = ['new','old']
random_means1, svd_means1 = [], []
random_means2, svd_means2 = [], []
random_me1, svd_me1 = [], []
random_me2, svd_me2 = [], []

exits_directory = ''

for period in periods:
    print period
    
    experience = treatment_modules.loadExperienceUsers(exits_directory,period+1)
    experience = experience[experience.experience>0]
    experience.rename(columns={'userID':'auserID'},inplace=True)    
    
    svd = pd.read_csv(''+str(period)+'1'+str(numFactors)+'31.csv')
    random = pd.read_csv(''+str(period)+'131.csv')
    # Just compare the treatment effect of at least 1, versus none
    svd = svd[svd.treatment_num==1]
    random = random[random.treatment_num==1]    
    
    svd['a_adopts'] = svd['a_adopted_at']==svd['a_adopted_at']
    svd['a2_adopts'] = svd['a2_adopted_at']==svd['a2_adopted_at']
    svd['a_adopts'] = svd['a_adopts'].astype(int)
    svd['a2_adopts'] = svd['a2_adopts'].astype(int)

    random['a_adopts'] = random['a_adopted_at']==random['a_adopted_at']
    random['a2_adopts'] = random['a2_adopted_at']==random['a2_adopted_at']
    random['a_adopts'] = random['a_adopts'].astype(int)
    random['a2_adopts'] = random['a2_adopts'].astype(int)
    
    print len(svd), len(random)
    
    valid1 = svd[['auserID','repoID','treatment_num']]
    valid2 = random[['auserID','repoID','treatment_num']]
    valid = pd.merge(valid1,valid2,on=['auserID','repoID','treatment_num'],how='inner')
    
    svd = pd.merge(valid,svd,on=['auserID','repoID','treatment_num'],how='inner')
    random = pd.merge(valid,random,on=['auserID','repoID','treatment_num'],how='inner')
    
    print len(svd), len(random)
    
    svd = pd.merge(svd,experience,on=['auserID'],how='left')
    random = pd.merge(random,experience,on=['auserID'],how='left')
    
    #lower = svd[['auserID','experience']].drop_duplicates().experience.quantile(.10)  
    #upper = svd[['auserID','experience']].drop_duplicates().experience.quantile(.10) 
    #print lower, upper    
    
    svd['new'] = svd.experience<=6    
    svd['old'] = svd.experience>=24
    svd['new'] = svd['new'].astype(int)
    svd['old'] = svd['old'].astype(int)
    
    random['new'] = random.experience<=5    
    random['old'] = random.experience>=24
    random['new'] = random['new'].astype(int)
    random['old'] = random['old'].astype(int)
    
    random_all = pd.DataFrame()
    svd_all = pd.DataFrame()
    
    for status in ageGroups:
        
        N = len(random[random[status]==1])
        print status, N
    
        random_p = random[random[status]==1].a_adopts.mean()
        random_q = random[random[status]==1].a2_adopts.mean()
        print random_p/random_q
    
        random_std_dev = np.sqrt((1-random_p)/(N*random_p)+(1-random_q)/(N*random_q))*(N*random_p)/(N*random_q)
        random_se = 1.96*random_std_dev/np.sqrt(N)
        print random_se
        
        svd_p = svd[svd[status]==1].a_adopts.mean()
        svd_q = svd[svd[status]==1].a2_adopts.mean()
        print svd_p/svd_q
        
        svd_std_dev = np.sqrt((1-svd_p)/(N*svd_p)+(1-svd_q)/(N*svd_q))*(N*svd_p)/(N*svd_q)
        svd_se = 1.96*svd_std_dev/np.sqrt(N)
        print svd_se
        
        if status=='new':
            random_means1.append(random_p/random_q)
            random_me1.append(random_se)
            svd_means1.append(svd_p/svd_q)
            svd_me1.append(svd_se)
            
            random_all['1'] = random_means1
            random_all['1+'] = random_all['1'] + random_me1
            random_all['1-'] = random_all['1'] - random_me1
            
            svd_all['1'] = svd_means1
            svd_all['1+'] = svd_all['1'] + svd_me1
            svd_all['1-'] = svd_all['1'] - svd_me1
            
        elif status=='old':
            random_means2.append(random_p/random_q)
            random_me2.append(random_se)
            svd_means2.append(svd_p/svd_q)
            svd_me2.append(svd_se)
            
            random_all['2'] = random_means2
            random_all['2+'] = random_all['2'] + random_me2
            random_all['2-'] = random_all['2'] - random_me2
            
            svd_all['2'] = svd_means2
            svd_all['2+'] = svd_all['2'] + svd_me2
            svd_all['2-'] = svd_all['2'] - svd_me2
    
    random_all['period'] = period
    svd_all['period'] = period
    
    if period==1:
        with open('treatment_effect_random_age.csv', 'w') as f:
            random_all.to_csv(f,index=False,header=True)
        with open('treatment_effect_nearest_neighbor_age.csv', 'w') as f:
            svd_all.to_csv(f,index=False,header=True)      
    else:
        with open('treatment_effect_random_age.csv', 'a') as f:
            random_all.to_csv(f,index=False,header=False)
        with open('treatment_effect_nearest_neighbor_age.csv', 'a') as f:
            svd_all.to_csv(f,index=False,header=False)
            
    random_means1, svd_means1 = [], []
    random_means2, svd_means2 = [], []
    random_me1, svd_me1 = [], []
    random_me2, svd_me2 = [], []