# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 14:58:48 2017

@author: christopherrojas

Program to build a dataframe of user join dates and exit dates.
"""
import pandas as pd

# Enter the path to the data.
data_dir = ""

# Import data.
follows = pd.read_csv(data_dir+'follows.csv',usecols=['auserID','tuserID','created_at'])
follows['created_at'] = follows['created_at'].astype('datetime64[s]')
follows1 = follows[['auserID','created_at']].rename(columns={'auserID':'userID'})
follows1['userID'] = follows1['userID'].astype(int)
follows2 = follows[['tuserID','created_at']].rename(columns={'tuserID':'userID'})
follows2['userID'] = follows2['userID'].astype(int)
follows = 0
print "Loaded follows"

stars = pd.read_csv(data_dir+'stars.csv',usecols=['userID','created_at'])
stars['created_at'] = stars['created_at'].astype('datetime64[s]')
stars['userID'] = stars['userID'].astype(int)
print "Loaded stars"

# Create the joins .csv
total = pd.concat([follows1,follows2,stars],ignore_index=True)
total.drop_duplicates(inplace=True)
follows1, follows2, pushes, pulls, stars, issues, forks, creates = 0,0,0,0,0,0,0,0
joins = total.groupby('userID').created_at.min().reset_index()
joins.columns = ['userID','joined_at']
joins.to_csv(data_dir+'join_dates_users.csv',header=True,index=False)
joins = 0

# Create the exits .csv
exits = total.groupby('userID').created_at.max().reset_index()
exits.columns = ['userID','exited_at']
exits.to_csv(data_dir+'exit_dates_users.csv',header=True,index=False)
