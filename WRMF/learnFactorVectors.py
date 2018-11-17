#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 12:56:37 2018

@author: christopherrojas

Learn the preference and characteristic vectors with WRMF.
"""

import pandas as pd
import numpy as np
import scipy.sparse as sparse
import implicit

## Events Functions
def ActiveUsers(activities,minRepo):
    X = activities.groupby('userID').repoID.count().reset_index()
    
    activeUsers = X[X.repoID>=minRepo]
    X = np.nan
    
    activities = activities[activities.userID.isin(list(activeUsers.userID))]
    return activities

def map_ids(row, mapper):
    return mapper[row]

# Enter the directory for the data.
data_dir = ""

# Set parameters
period = 1
minReposPerUser = 10
confidence = 1000
regularize = 50
nfactors = 100
numIterations = 15

# Set the cutoff date for the given period
d = pd.date_range(start='1/1/2013', end='10/1/2013', freq='MS')
cutoff_date = d[period-1]
print cutoff_date

# Load data
df = pd.read_csv(data_dir+'stars_first.csv')
df['created_at'] = df['created_at'].astype('datetime64[s]')
print len(df)
df = df[df.created_at<cutoff_date]
print len(df)

df = df[['userID','repoID']]
df.drop_duplicates(inplace=True)

df = ActiveUsers(df,minReposPerUser)
print "Number of Users: " + str(len(df.userID.unique()))
print "Number of Repos: " + str(len(df.repoID.unique()))
print "Number of Observations: " + str(len(df))
df.columns = ['uid','rid']

# Create mappings
rid_to_idx = {}
idx_to_rid = {}
for (idx, rid) in enumerate(df.rid.unique().tolist()):
    rid_to_idx[rid] = idx
    idx_to_rid[idx] = rid
    
uid_to_idx = {}
idx_to_uid = {}
for (idx, uid) in enumerate(df.uid.unique().tolist()):
    uid_to_idx[uid] = idx
    idx_to_uid[idx] = uid

I = df.rid.apply(map_ids, args=[rid_to_idx]).as_matrix()    
J = df.uid.apply(map_ids, args=[uid_to_idx]).as_matrix()
V = confidence*np.ones(I.shape[0])
likes = sparse.coo_matrix((V, (I, J)), dtype=np.float64)
likes = likes.tocsr()

# initialize a model
model = implicit.als.AlternatingLeastSquares(factors=nfactors,regularization=regularize,iterations=numIterations)

# train the model on a sparse matrix of item/user/confidence weights
print np.shape(likes)
model.fit(likes)

# retrieve the latent user factors
user_factors = model.user_factors
user_factors = pd.DataFrame(user_factors)
user_factors['userID'] = list(df.uid.unique())
user_factors.to_csv('stars_user_factors_'+str(period)+'.csv',index=False,header=True)

# retrieve the latent item factors
#item_factors = model.item_factors
#item_factors = pd.DataFrame(item_factors)
#item_factors['repoID'] = list(df.rid.unique())
#item_factors.to_csv('stars_item_factors_'+str(period)+'.csv',index=False,header=True)

