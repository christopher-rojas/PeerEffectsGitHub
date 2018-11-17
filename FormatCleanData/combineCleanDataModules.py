#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 14 15:32:36 2017

@author: christopherrojas

For each type of data, fix the dates, and drop exact duplicates.
"""

import pandas as pd

def Combine(event,period_range):
    print event
    for period in period_range:
        print period
        data = pd.read_csv(event+'-'+str(period.month)+'-'+str(period.year)+'.csv')
        
        if period==period_range[0]:
            # Append subsequent months, without headers
            with open(event+'_first.csv', 'w') as f:
                data.to_csv(f, header=True, index=False)
        else:
            # Append subsequent months, without headers
            with open(event+'_first.csv', 'a') as f:
                data.to_csv(f, header=False, index=False)

def fixDate(string):
    # Fix some idiosyncracies in how GitHub records datetimes.
    # Designed to handle repo creation times and link creation times.
    # Record everything at UTC time.
    if type(string)==str:
        date = string[0:4] + '-' + string[5:7] + '-' + string[8:10]
        time = 'T' + string[11:19]+'Z'
        datetime = date + time
    else:
        datetime = string
    return datetime
         
def Clean(event):
    print event
    events = pd.read_csv(event+'_first.csv')
    events['created_at'] = events.created_at.apply(lambda x: fixDate(x))
    
    if event=='follows':
        # Drop erroneous duplicates
        events.drop_duplicates(keep='first',inplace=True)
        events = events[events.alogin==events.alogin]
        events = events[events.tlogin==events.tlogin]
        events = events[events.created_at==events.created_at]
        events.sort_values(by=['alogin','created_at'],inplace=True)

        # Drop repeat follows.
        events.drop_duplicates(subset=['alogin','tlogin'],keep='first',inplace=True)
        events.to_csv(event+'_first.csv',index=False,header=True)
                
    else:
        events['repo_created_at'] = events.repo_created_at.apply(lambda x: fixDate(x))
        # Drop erroneous duplicates        
        events.drop_duplicates(keep='first',inplace=True)
        events = events[events.alogin==events.alogin]
        events = events[events.repo==events.repo]
        events = events[events.created_at==events.created_at]
        events.sort_values(by=['alogin','created_at'],inplace=True)

        # Drop repeat stars
        events.drop_duplicates(subset=['alogin','repo'],keep='first',inplace=True)
        events.to_csv(event+'_first.csv',index=False,header=True)