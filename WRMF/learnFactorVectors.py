#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Learn the preference and characteristic vectors with Weighted, Regularized
    Matrix Factorization, implemented by the Implicit library.
"""

import pandas as pd
import numpy as np
import scipy.sparse as sparse
import implicit
import logging

#Set up logging
logging.basicConfig(filename='learn_factor_vectors.log',level=logging.INFO,format='%(asctime)s %(message)s')

def ActiveUsers(activities,minRepo):
    # We keep only the agents who have starred a minimum number of repos.
    # Input, the events data (activities), and the minimum number of stars
    #   needed to keep an agent (minRepo).

    X = activities.groupby('userID').repoID.count().reset_index()
    
    activeUsers = X[X.repoID>=minRepo]
    X = np.nan
    
    activities = activities[activities.userID.isin(list(activeUsers.userID))]
    return activities

def map_ids(row, mapper):
    return mapper[row]

# Enter the directory for the data output from combineCleanData.py.
data_dir = ""

###### Parameters that need to be set
period = 1 # time period
minReposPerUser = 10 # minimum number of repos to star to infer preferences
confidence = 1000 # confidence hyper-parameter for WRMF
regularize = 50 # regularization hyper-parameter for WRMF
nfactors = 100 # number of latent factors hyper-parameter for WRMF
numIterations = 15 # number of alternations in alterating least squares, hyper-parameter for WRMF
start_date = '1/1/2013'
end_date = '10/1/2013'
frequency = 'MS' # ensure that the start of a month is the cutoff in our paper
######

# Set the cutoff date for data used to infer preference types
d = pd.date_range(start=start_date, end=end_date, freq=frequency)
cutoff_date = d[period-1]
logging.info("Use data prior to: "+cutoff_date+" to learn preferences.")

# Load data
df = pd.read_csv(data_dir+'stars.csv')
df['created_at'] = df['created_at'].astype('datetime64[s]')
df = df[df.created_at<cutoff_date]
df = df[['userID','repoID']]
df.drop_duplicates(inplace=True)

# Drop agents who haven't starred enough items by cutoff_date
df = ActiveUsers(df,minReposPerUser)
df.columns = ['uid','rid']

# Create mappings
# taken from www.ethanrosenthal.com/2016/10/19/implicit-mf-part-1/
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

# build a sparse user-items matrix
I = df.rid.apply(map_ids, args=[rid_to_idx]).as_matrix()    
J = df.uid.apply(map_ids, args=[uid_to_idx]).as_matrix()
# The line below is how this WRMF library incorporates confidence weights
V = confidence*np.ones(I.shape[0])
likes = sparse.coo_matrix((V, (I, J)), dtype=np.float64)
likes = likes.tocsr()

# log some info about the user-items matrix
nrows = np.shape(likes)[0]
ncols = np.shape(likes)[1]
logging.info("Size of user-items matrix is: %d by %d" % (nrows,ncols))
sparsity = float(len(df))/(nrows*ncols)
logging.info("Sparsity: %f" % sparsity)

# initialize the model
model = implicit.als.AlternatingLeastSquares(factors=nfactors,regularization=regularize,iterations=numIterations)
logging.info("Training model")
# train the model
model.fit(likes)
logging.info("Finished training model")

# save the output
user_factors = model.user_factors
user_factors = pd.DataFrame(user_factors)
user_factors['userID'] = list(df.uid.unique())
user_factors.to_csv('stars_user_factors_'+str(period)+'.csv',index=False,header=True)

item_factors = model.item_factors
item_factors = pd.DataFrame(item_factors)
item_factors['repoID'] = list(df.rid.unique())
item_factors.to_csv('stars_item_factors_'+str(period)+'.csv',index=False,header=True)