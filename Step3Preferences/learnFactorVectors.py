#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Learn the preference and characteristic vectors with Weighted, Regularized
    Matrix Factorization, implemented by the Implicit library.
"""

import numpy as np
import scipy.sparse as sparse
import implicit
import pandas as pd

###### PARAMETERS TO ENTER
confidence = 1000 # confidence hyper-parameter for WRMF
regularize = 50 # regularization hyper-parameter for WRMF
nfactors = 10 # number of latent factors hyper-parameter for WRMF
numIterations = 15 # number of alternations in alterating least squares, hyper-parameter for WRMF

likes_dir = "" # directory where adoption matrix AND mappings are saved
likes_filename = "adoptions_matrix.npz" # name of adoption matrix filename
uid_mapping_filename = "uid_to_idx.cxv" # name of mapping of user id to matrix position filename
rid_mapping_filename = "rid_to_idx.csv" # name of mapping of repo id to matrix position filename

output_data_dir = ""
output_user_factors_filename = "wrmf_user_factors.csv"
output_repo_factors_filename = "wrmf_item_factors.csv"
###### MODIFY BELOW THIS LINE AT YOUR OWN RISK

# Load the sparse adoptions matrix
likes = sparse.load_npz(likes_dir + likes_filename)
likes = likes.multiply(confidence)

# Load the id mappings
uid_mapping = pd.read_csv(likes_dir + uid_mapping_filename)
uid_mapping.sort_values(by='idx',ascending=True,inplace=True)

rid_mapping = pd.read_csv(likes_dir + rid_mapping_filename)
rid_mapping.sort_values(by='idx',ascending=True,inplace=True)

# log some info about the user-items matrix
nrows = np.shape(likes)[0]
ncols = np.shape(likes)[1]
print("Size of user-items matrix is: %d by %d" % (nrows,ncols))
sparsity = float(likes.count_nonzero())/(nrows*ncols)
print("Sparsity: %f" % sparsity)

# initialize the model
model = implicit.als.AlternatingLeastSquares(factors=nfactors,regularization=regularize,iterations=numIterations)
print("Training model")
# train the model
model.fit(likes)
print("Finished training model")

# save the output
user_factors = model.user_factors
user_factors = pd.DataFrame(user_factors)
user_factors['userID'] = list(uid_mapping.userID)
user_factors.to_csv(output_data_dir+output_user_factors_filename,index=False)

item_factors = model.item_factors
item_factors = pd.DataFrame(item_factors)
item_factors['repoID'] = list(rid_mapping.repoID)
item_factors.to_csv(output_data_dir+output_repo_factors_filename,index=False)