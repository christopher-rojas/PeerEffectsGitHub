#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Implement random search for hyper-parameter tuning with discounted cumulative gain
as the test statistic.
"""

import numpy as np
import scipy.sparse as sparse
import logging
from implicit.als import AlternatingLeastSquares
import preference_modules

###### PARAMETERS TO ENTER
max_dcg_ranking = 100 # lowest ranking included to compute dcg
num_random_search = 10 # number of random hyper-parameter values to draw
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

input_data_dir = "" # directory where adoptions matrix is stored 
likes_filename = "stars.csv" # adoptions matrix filename
###### MODIFY BELOW THIS LINE AT YOUR OWN RISK

#Set up logging
logging.basicConfig(filename='dcg_hyperparams.log',level=logging.INFO,format='%(asctime)s %(message)s')

# Load the sparse adoptions matrix
likes = sparse.load_npz(input_data_dir + likes_filename)

# log some info about the user-items matrix
nrows = np.shape(likes)[0]
ncols = np.shape(likes)[1]
logging.info("Size of user-items matrix is: %d by %d" % (nrows,ncols))
sparsity = float(likes.count_nonzero())/(nrows*ncols)
logging.info("Sparsity: %f" % sparsity)

# split the user-items matrix into training and test data
train, test, user_index = preference_modules.train_test_split(likes, test_items, fraction=test_frac)

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
    
    dcg_current = preference_modules.DCG(test,train_current,model,user_index,max_dcg_ranking)
    
    logging.info("DCG: %f" % dcg_current)