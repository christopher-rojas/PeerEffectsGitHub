#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
For each type of data (stars, follows), fix the date-time string format,
    fix errors and (possibly) drop duplicate stars or follows.
In our paper, we keep the first observation when there are duplicates.

We assume these files are in the same directory as the output from 
    starsMongoExtract.py and followsMongoExtract.py.
"""
import pandas as pd
import logging
import cleanData

#Set up logging
logging.basicConfig(filename='data_combine.log',level=logging.INFO,format='%(asctime)s %(message)s')

##### Parameters to enter
start_date = '2011-2-01'
end_date = '2013-10-31'
frequency = 'M'
event_types = ['stars', 'follows']
keep_type = 'first' # If an agent stars or follows multiple times, 
# keep the 'first', 'last', or 'all' observation(s). 
#####

events = event_types
periods = pd.date_range(start=start_date, end=end_date, freq=frequency)

# First, iterate through the .csv files, created one combined .csv for each event type.
for event in events:
    cleanData.Combine(event,periods)

# Next, drop erroneous observations, and (possibly) drop duplicates
for event in events:
    cleanData.Clean(event, keep_type)

