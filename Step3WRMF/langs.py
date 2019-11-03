# -*- coding: utf-8 -*-
"""
Estimate language fractions
"""
import pandas as pd
import numpy as np

#################### PARAMETERS TO ENTER
# Load the language-based user factor vectors
stars = pd.read_csv('')
period = 1
#####################

stars = stars[stars.created_at<period]
stars = stars.sort_values(by=['created_at'])
stars = stars[['userID','repoID','repo_language']]
stars.drop_duplicates(subset=['userID','repoID'],inplace=True)

stars['repo_language'].fillna('none',inplace=True)
stars = stars[['userID','repo_language','repoID']]
stars = stars.groupby(['userID','repo_language']).repoID.count().reset_index()
stars.rename(columns={'repoID':'num'},inplace=True)

totals = stars.groupby('userID').num.sum().reset_index()
totals.rename(columns={'num':'total'},inplace=True)

stars = pd.merge(stars,totals,on='userID',how='left')
stars['fraction'] = stars['num']/stars['total']
stars = stars[['userID','repo_language','fraction']]
stars = stars[stars.repo_language!='none']
langIDs = list(stars.repo_language.unique())
print len(langIDs)

# Reshape
stars = stars.set_index(['userID','repo_language']).unstack(level=1)
stars.fillna(0,inplace=True)
stars.reset_index(inplace=True)
stars.columns = ['userID']+langIDs
stars = stars[langIDs+['userID']]

# Make the ids the same as when I did SVD
newLangIDs = range(0,len(langIDs))
newLangIDs = [str(item) for item in newLangIDs]
stars.columns = newLangIDs + ['userID']
totals, langIDs, newLangIDs = np.nan, np.nan, np.nan

stars.rename(columns={'userID':'auserID'},inplace=True)

print np.shape(stars)
####################

stars.to_csv('langs'+str(period)+'.csv',index=False,header=True)