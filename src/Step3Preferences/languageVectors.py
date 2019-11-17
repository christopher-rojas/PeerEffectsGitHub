# -*- coding: utf-8 -*-
"""
Estimate language vectors
"""
import pandas as pd
import numpy as np
import preference_modules

#################### PARAMETERS TO ENTER
# Load the language-based user factor vectors
input_data_dir = '' #directory where stars data is located
input_filename = 'stars.csv' # stars filename
period = 1 # time period
minReposPerUser = 10 # minimum number of repos to star by beginning
                        # of period to infer preferences
factors = 100 # cap on number of languages to include
output_dir = '' # directory where output is stored
output_filename = 'langs.csv' # language vectors filename
##################### MODIFY BELOW THIS LINE AT YOUR OWN RISK

# Load data
stars = pd.read_csv(input_data_dir + input_filename)

# Set the cutoff date for data used to infer preference types
period_cutoffs = pd.date_range(start='1/1/2013', end='10/1/2013', freq='MS')
cutoff_date = period_cutoffs[period-1]

# Load data
stars = pd.read_csv(input_data_dir + input_filename)
stars['created_at'] = stars['created_at'].astype('datetime64[s]')
stars = stars[stars.created_at<cutoff_date]

# Get agents who starred enough items by cutoff_date
starsActive = preference_modules.ActiveUsers(stars,minReposPerUser)
starsActive = list(starsActive.userID.unique())

stars.sort_values(by=['created_at'],inplace=True)
stars = stars[['userID','repoID','repo_language']]
stars.drop_duplicates(subset=['userID','repoID'],inplace=True)
stars['repo_language'].fillna('none',inplace=True)

# Count language stars by agent, and total stars by agent
    # then divide to get fractions
stars = stars.groupby(['userID','repo_language']).repoID.count().reset_index()
stars.rename(columns={'repoID':'num'},inplace=True)

totals = stars.groupby('userID').num.sum().reset_index()
totals.rename(columns={'num':'total'},inplace=True)

stars = pd.merge(stars,totals,on='userID',how='left')
stars['fraction'] = stars['num']/stars['total']
stars = stars[stars.repo_language != 'none']

# Keep the 'factors'-most popular languages
    # in the sense that the most agents starred those languages
most_pop = stars.groupby('repo_language').userID.nunique().reset_index()
most_pop = most_pop.nlargest(factors,'userID')
most_pop['popular'] = 1
most_pop = most_pop[['repo_language','popular']]

stars = pd.merge(stars, most_pop, on = 'repo_language', how='left')

stars = stars[stars['popular'] == 1] 
stars = stars[['userID','repo_language','fraction']]

# Reformat the languages output to be the same as WRMF output
langIDs = list(stars.repo_language.unique())

stars = stars.set_index(['userID','repo_language']).unstack(level=1)
stars.fillna(0,inplace=True)
stars.reset_index(inplace=True)
stars.columns = ['userID']+langIDs
stars = stars[langIDs+['userID']]

newLangIDs = range(0,len(langIDs))
newLangIDs = [str(item) for item in newLangIDs]
stars.columns = newLangIDs + ['userID']
totals, langIDs, newLangIDs = np.nan, np.nan, np.nan
stars.rename(columns={'userID':'auserID'},inplace=True)
stars = stars[stars.auserID.isin(starsActive)]

# Save output
stars.to_csv(output_dir + output_filename,index=False,header=True)