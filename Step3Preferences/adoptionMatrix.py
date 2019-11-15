# -*- coding: utf-8 -*-
"""
Create the adoption matrix, which is a (# users by # items) 
sparse binary matrix, with a 1 in the (u, i) position if 
item i was starred by agent u
"""

import pandas as pd
import numpy as np
import scipy.sparse as sparse
import csv
import preference_modules

###### PARAMETERS TO ENTER
period = 1 # time period
minReposPerUser = 10 # minimum number of repos to star by beginning
                        # of period to infer preferences
input_data_dir = "" # directory where stars data is located
input_filename = "stars.csv" # stars filename

output_data_dir = "" # directory where output is stored
adoptions_filename = "adoptions_matrix.npz" # adoption matrix filename
uid_mapping_filename = "uid_to_idx.csv" # user id to matrix row mapping
rid_mapping_filename = "rid_to_idx.csv" # repo id to matrix column mapping
###### MODIFY BELOW THIS LINE AT YOUR OWN RISK

# periods are months, and period begins at start of month
period_cutoffs = pd.date_range(start='1/1/2013', end='10/1/2013', freq='MS')
cutoff_date = period_cutoffs[period-1]

# Load data
stars = pd.read_csv(input_data_dir + input_filename)
stars['created_at'] = stars['created_at'].astype('datetime64[s]')
stars = stars[stars.created_at<cutoff_date]
stars = stars[['userID','repoID']]
stars.drop_duplicates(inplace=True)

# Drop agents who haven't starred enough items by cutoff_date
stars = preference_modules.ActiveUsers(stars,minReposPerUser)
stars.columns = ['uid','rid']

# Create mappings from my id #'s to position in adoption matrix
# taken from www.ethanrosenthal.com/2016/10/19/implicit-mf-part-1/
rid_to_idx = {}
idx_to_rid = {}
for (idx, rid) in enumerate(stars.rid.unique().tolist()):
    rid_to_idx[rid] = idx
    idx_to_rid[idx] = rid
    
uid_to_idx = {}
idx_to_uid = {}
for (idx, uid) in enumerate(stars.uid.unique().tolist()):
    uid_to_idx[uid] = idx
    idx_to_uid[idx] = uid

# build a sparse user-items matrix
I = stars.rid.apply(preference_modules.map_ids, args=[rid_to_idx]).as_matrix()    
J = stars.uid.apply(preference_modules.map_ids, args=[uid_to_idx]).as_matrix()
V = np.ones(I.shape[0])
likes = sparse.coo_matrix((V, (I, J)), dtype=np.float64)
likes = likes.tocsr()

# save the adoption data
sparse.save_npz(output_data_dir + adoptions_filename, likes, compressed=True)

# save the mappings
w1 = csv.writer(open(output_data_dir + uid_mapping_filename, "w"))
w1.writerow(['userID','idx'])
for key, val in uid_to_idx.items():
    w1.writerow([key, val])    

w2 = csv.writer(open(output_data_dir + rid_mapping_filename, "w"))
w2.writerow(['repoID','idx'])
for key, val in rid_to_idx.items():
    w2.writerow([key, val])
