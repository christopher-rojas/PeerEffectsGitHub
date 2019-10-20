#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu May  4 09:51:16 2017

@author: christopherrojas

Program to build a dataframe of user ids.
"""

import pandas as pd
import numpy as np

def IDs(actors,repos):
    reposID = np.arange(0,len(repos),1)
    actorsID = np.arange(0,len(actors),1)
    
    actorsID_Mapping = pd.DataFrame({'userID': actorsID, 'alogin': actors})
    reposID_Mapping = pd.DataFrame({'repoID': reposID, 'repo': repos})
    actorsID_Mapping.to_csv('userID_Mapping.csv',index=False)
    reposID_Mapping.to_csv('repoID_Mapping.csv',index=False)
    
    return actorsID_Mapping, reposID_Mapping
    
def GetFollowsActors(followEvents,actors):
    print "actors"
    
    actors += list(followEvents.tlogin.unique())
    actors += list(followEvents.alogin.unique())
    actors = list(set(actors))
    
    return actors

def GetActorsRepos(events,actors,repos):
    print "actors"
    actors += list(events.alogin.unique())
    actors = list(set(actors))
    
    print "repos"
    repos += list(events.repo.unique())
    repos = list(set(repos))
    
    return actors, repos


actors = []
repos = []

# Enter the path to the data files
data_dir = ""

print "stars"
stars = pd.read_csv(path_to_data+'stars_first.csv',usecols=['alogin','repo'])
actors, repos = GetActorsRepos(stars,actors,repos)
print np.shape(actors), np.shape(repos)
stars = np.nan

print "follows"
follows = pd.read_csv(path_to_data+'follows_first.csv',usecols=['alogin','tlogin'])
actors = GetFollowsActors(follows,actors)
print np.shape(actors), np.shape(repos)
follows = np.nan

print "IDs"
userMapping, itemMapping = IDs(actors,repos)

