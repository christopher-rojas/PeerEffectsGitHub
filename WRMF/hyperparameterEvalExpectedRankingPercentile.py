#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 19 10:49:16 2018

@author: christopherrojas

Estimate the expected ranking percentile for different hyperparameter combinations.
"""

import pandas as pd
import numpy as np
import scipy.sparse as sparse
import logging
import itertools
from implicit.als import AlternatingLeastSquares

def train_test_split(ratings, split_count, fraction=None):
    """
    Split recommendation data into train and test sets
    
    Params
    ------
    ratings : scipy.sparse matrix
        Interactions between users and items.
    split_count : int
        Number of user-item-interactions per user to move
        from training to test set.
    fractions : float
        Fraction of users to split off some of their
        interactions into test set. If None, then all 
        users are considered.
    """
    # Note: likely not the fastest way to do things below.
    train = ratings.copy().tocoo()
    test = sparse.lil_matrix(train.shape)
    
    if fraction:
        try:
            user_index = np.random.choice(
                np.where(np.bincount(train.row) >= split_count * 2)[0], 
                replace=False,
                size=np.int32(np.floor(fraction * train.shape[0]))
            ).tolist()
        except:
            print(('Not enough users with > {} '
                  'interactions for fraction of {}')\
                  .format(2*split_count, fraction))
            raise
    else:
        user_index = range(train.shape[0])
        
    train = train.tolil()

    for user in user_index:
        test_ratings = np.random.choice(ratings.getrow(user).indices, 
                                        size=split_count, 
                                        replace=False)
        train[user, test_ratings] = 0.
        # These are just 1.0 right now
        test[user, test_ratings] = ratings[user, test_ratings]
   
    
    # Test and training are truly disjoint
    assert(train.multiply(test).nnz == 0)
    return train.tocsr(), test.tocsr(), user_index

def ActiveUsers(activities,minRepo):
    X = activities.groupby('userID').repoID.count().reset_index()
    
    activeUsers = X[X.repoID>=minRepo]
    X = np.nan
    
    activities = activities[activities.userID.isin(list(activeUsers.userID))]
    return activities

def ExpectedRanking(test, train_current, model,test_user_indices):    

    test_rankings = []    
    for user in test_user_indices:
        
        labels = test.getrow(user).indices 
        done_already = train_current.getrow(user).indices
        
        A = np.matrix(model.user_factors[user])
        B = A*np.transpose(model.item_factors)
        temp = np.argsort(-B)
        temp = np.asarray(temp).reshape(-1)

        for item in done_already:
            temp = temp[(temp != item)]
        
        for item in labels:
            ranking = np.where(temp==item)[0][0]
            
            num_eligible = len(model.item_factors)-len(done_already)
            ranking_percentile = float(ranking)/num_eligible            
            
            test_rankings.append(ranking_percentile)
        
    return np.mean(test_rankings)
    
def map_ids(row, mapper):
    return mapper[row]

logging.basicConfig()

# Enter the directory for the data.
data_dir = ""

# Set parameters
period = 1
minReposPerUser = 10
nfactors = 100
numIterations = 15

# Set the cutoff date for the given period
d = pd.date_range(start='1/1/2013', end='1/1/2014', freq='31D') 
cutoff_date = d[period-1]

########
# LOAD THE DATA
########

stars_path = data_dir+'stars_first.csv'
friend_stars_path = 'friends_stars.csv'
pop_stars_path = 'popular_stars.csv'

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
V = np.ones(I.shape[0])
likes = sparse.coo_matrix((V, (I, J)), dtype=np.float64)
likes = likes.tocsr()

train, test, user_index = train_test_split(likes,5,fraction=0.03)
print np.shape(train), np.shape(test)
print len(user_index)

print "Done splitting training/test data..."

########
# ESTIMATE AND TEST MODELS WITH DIFFERENT HYPER-PARAMETERS
########
regularizations = [0.01,0.1,1,10,50,100]
pos_confidences = [1,10,50,100,500,1000]
num_factors = [nfactors]

parameters = itertools.product(regularizations,pos_confidences,num_factors)

for parameter in parameters:
    
    current_reg = parameter[0]
    current_pos_confidence =  parameter[1]
    current_factors = parameter[2]
    
    print "Likes"
    train_current = train*current_pos_confidence   
    model = AlternatingLeastSquares(factors=current_factors,regularization=current_reg,iterations=15)
    model.fit(np.transpose(train_current))
    print "Model fitted. Testing..."
    
    erp_current = ExpectedRanking(test,train_current,model,user_index)
    
    print "(lambda,alpha,n) = " + str(parameter) + ", erp: " + str(erp_current)