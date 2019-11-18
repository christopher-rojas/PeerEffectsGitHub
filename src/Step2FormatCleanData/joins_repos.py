# -*- coding: utf-8 -*-
"""
Program to build a dataframe of repo join dates and exit dates.
"""
import pandas as pd

data_dir = ''
data_filename = ''

# Import data.

stars = pd.read_csv(data_dir + data_filename, usecols=['repoID','created_at'])
stars['created_at'] = stars['created_at'].astype('datetime64[s]')
stars['repoID'] = stars['repoID'].astype(int)


# Create the joins .csv
joins = stars.groupby('repoID').created_at.min().reset_index()
joins.columns = ['repoID','joined_at']
joins.to_csv(data_dir+'joinDatesRepos.csv',header=True,index=False)
