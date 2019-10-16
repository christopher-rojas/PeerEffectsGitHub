#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Split data into training and test sets, train WRMF model on training data
    and compute Discounted Cumulative Gain using test data.
"""

import pandas as pd
import numpy as np
import scipy.sparse as sparse
import logging
from implicit.als import AlternatingLeastSquares

#Set up logging
logging.basicConfig(filename='test_hyperparams.log',level=logging.INFO,format='%(asctime)s %(message)s')

def train_test_split(ratings, split_count, fraction):
    """
    Split recommendation data into train and test sets
    Taken from www.ethanrosenthal.com/2016/10/19/implicit-mf-part-1/
    
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
            logging.info(('Not enough users with > {} '
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

def ActiveUsers(activities,minRepos,doneUsers):
    # We keep only the agents who have starred a minimum number of repos.
    # Input, the events data (activities), and the minimum number of stars
    #   needed to keep an agent (minRepo).

    X = activities.groupby('userID').repoID.count().reset_index()    
    activeUsers = X[X.repoID>=minRepos]
    doneUsers = len(X) == len(activeUsers)
    X = np.nan
    
    activities = activities[activities.userID.isin(list(activeUsers.userID))]
    
    return activities, doneUsers

def DCG(test, train_current, model, user_index, max_ranking):    
    # Estimate the discounted cumulative gain    
    # Inputs: the test data, the training data, the list of agent indices,
    #   and the lowest ranking included to compute dcg
    
    mean_dcg = []   
    
    for user in user_index:
        
        labels = test.getrow(user).indices # the agents in the test set
        done_already = train_current.getrow(user).indices # the items that the test agents already starred
        user_dcg = 0
        
        A = np.matrix(model.user_factors[user])
        B = A*np.transpose(model.item_factors)
        temp = np.argsort(-B)
        temp = np.asarray(temp).reshape(-1)
        
        # do not recommend any items that an agent already starred in the training data
        for item in done_already:
            temp = temp[(temp != item)]
        
        # compute the dcg for remaining recommended items
        for item in labels:
            # Min ranking should be 1, not 0
            ranking = np.where(temp==item)[0][0]+1
            if ranking<= max_ranking:            
                gain = 1/np.log2(ranking+1)
                user_dcg += gain
            
        mean_dcg.append(user_dcg)
         
    return np.mean(mean_dcg)
    
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
max_dcg_ranking = 100 # lowest ranking included to compute dcg
num_random_search = 100 # number of random hyper-parameter values to draw
min_conf = 1 # minimum possible confidence hyper-parameter
max_conf = 251 # maximum possible confidence hyper-parameter
min_reg = 1 # minimum possible confidence hyper-parameter
max_reg = 1001 # maximum possible regularization hyper-parameter
min_factors = 1 # minimum possible number of factors hyper-parameter
max_factors = 101 # maximum possible number of factors hyper-parameter
min_alts = 15 # minimum possible alternations hyper-parameter
max_alts = 16 # maximum possible alternations hyper-parameter
test_items = 5 # for each agent in test data, number of their stars to put in test data
test_frac = 0.03 # fraction of agents to include in test data
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
# Next line is how the benfred/implicit WRMF incorporates confidence weights
V = confidence*np.ones(I.shape[0])
likes = sparse.coo_matrix((V, (I, J)), dtype=np.float64)
likes = likes.tocsr()

# log some info about the user-items matrix
nrows = np.shape(likes)[0]
ncols = np.shape(likes)[1]
logging.info("Size of user-items matrix is: %d by %d" % (nrows,ncols))
sparsity = float(len(df))/(nrows*ncols)
logging.info("Sparsity: %f" % sparsity)

# split the user-items matrix into training and test data
train, test, user_index = train_test_split(likes, test_items, fraction=test_frac)

# Randomly draw hyper-parameter values
pos_confidences = range(min_conf, max_conf, 1)
regularizations = range(min_reg, max_reg, 1)
num_factors = range(min_factors, max_factors, 1)
alternations = range(min_alts, max_alts, 1)

pos_confidences = np.random.choice(pos_confidences,num_random_search,replace=True)
regularizations = np.random.choice(regularizations,num_random_search,replace=True)
num_factors = np.random.choice(num_factors,num_random_search,replace=True)
alternations = np.random.choice(alternations,num_random_search,replace=True)

parameters = np.column_stack((pos_confidences,regularizations,num_factors,alternations))

# Estimate models and compute dcg
for parameter in parameters:
    
    current_conf =  parameter[0]
    current_reg = parameter[1]
    current_factors = parameter[2]
    current_alts = parameter[3]
    
    logging.info("alpha: %d, lambda: %d, n: %d, iters: %d" % (current_conf, current_reg, current_factors, current_alts))
    logging.info("Training model")
    
    train_current = train*current_conf  
    model = AlternatingLeastSquares(factors=current_factors,regularization=current_reg,iterations=current_alts)
    model.fit(np.transpose(train_current))
    logging.info("Testing model")
    
    dcg_current = DCG(test,train_current,model,user_index,max_dcg_ranking)
    
    logging.info("DCG: %f" % dcg_current)