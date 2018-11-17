#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 14:53:31 2017

@author: christopherrojas

Combine the monthly files into one, and delete monthly files
"""
import pandas as pd
import cleanData

events = ['stars','follows']
periods = pd.date_range(start='2011-2-01',end='2013-10-31',freq='M')

# First, iterate through the events, creating the combined events for each.
for event in events:
    cleanData.Combine(event,periods)

# Next, drop the erroneous duplicates, and make the first_ and last_ files.
for event in events:
    cleanData.Clean(event)

