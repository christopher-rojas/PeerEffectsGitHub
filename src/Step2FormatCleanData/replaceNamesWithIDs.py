# -*- coding: utf-8 -*-
"""
@author: christopherrojas

Replace usernames with numeric id numbers
"""
import pandas as pd
import numpy as np

def IDs(actors,repos):
    # Input: List of usernames and list of repo names
    # Output: Mappings of usernames to ids, and repo names to ids
    reposID = np.arange(0,len(repos),1)
    actorsID = np.arange(0,len(actors),1)
    
    actorsID_Mapping = pd.DataFrame({'userID': actorsID, 'alogin': actors})
    reposID_Mapping = pd.DataFrame({'repoID': reposID, 'repo': repos})
    
    return actorsID_Mapping, reposID_Mapping
    
def GetFollowsActors(follows,actors=[]):
    # Input: Follows data and possibly a pre-existing list of actors
    actors += list(follows.tlogin.unique())
    actors += list(follows.alogin.unique())
    actors = list(set(actors))
    
    return actors

def GetStarsActorsRepos(events, actors=[], repos=[]):
    # Stars data and possibly pre-existing lists of actors and repos.

    actors += list(events.alogin.unique())
    actors = list(set(actors))
    
    repos += list(events.repo.unique())
    repos = list(set(repos))
    
    return actors, repos


######## STUFF TO ENTER

data_dir = ""
user_ids_filename = ""
repo_ids_filename = ""
stars_filename = ""
follows_filename = ""

########

######## DO NOT CHANGE BELOW THIS LINE

# Make the ids for each unique user, repo
stars = pd.read_csv(data_dir+stars_filename)
follows = pd.read_csv(data_dir+follows_filename)

actors, repos = GetStarsActorsRepos(stars)
actors = GetFollowsActors(follows,actors)

actors, repos = IDs(actors, repos)

# Merge the ids
stars = pd.merge(stars,actors,on='alogin',how='left')
stars = pd.merge(stars,repos,on='repo',how='left')
actors.rename(columns={'userID':'auserID'},inplace=True)
follows = pd.merge(follows,actors,on='alogin',how='left')
actors.rename(columns={'auserID':'tuserID','alogin':'tlogin'},inplace=True)
follows = pd.merge(follows,actors,on='tlogin',how='left')

# Build the ids for dyad
dyads = np.arange(0,len(follows),1)
follows['dyadID'] = dyads
follows['dyadID'] = follows['dyadID'].astype('int')

stars.to_csv(data_dir+stars_filename,index=False)
follows.to_csv(data_dir+follows_filename,index=False)