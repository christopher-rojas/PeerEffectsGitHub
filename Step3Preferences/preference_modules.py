#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modules for use in estimation of preferences
"""
import numpy as np
import scipy.sparse as sparse
import logging

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
    # return the matrix row (column) corresponding to a repo id (user id)
    return mapper[row]

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