# -*- coding: utf-8 -*-
"""
Modules to clean and combine data.
"""

import pandas as pd
import logging

def Combine(event,period_range):
    # Combine the individual period .csv files into one combined .csv.
    # Inputs: the event type and range of periods to combine.    
    logging.info(event+'_combine')
    
    for period in period_range:
        
        logging.info(period)
        data = pd.read_csv(event+'-'+str(period.month)+'-'+str(period.year)+'.csv')
        
        if period==period_range[0]:
            # Write the first period to a new file, with headers
            with open(event+'.csv', 'w') as f:
                data.to_csv(f, header=True, index=False)
        else:
            # Append subsequent months, without headers
            with open(event+'.csv', 'a') as f:
                data.to_csv(f, header=False, index=False)

def fixDate(string):
    # Fix some idiosyncracies in how GitHub API records datetimes.
    # Designed to handle repo creation times and link creation times.
    # Convert everything to UTC time.
    # Input: a date-time string from the GH archive.
    if type(string)==str:
        date = string[0:4] + '-' + string[5:7] + '-' + string[8:10]
        time = 'T' + string[11:19]+'Z'
        datetime = date + time
    else:
        datetime = string
    return datetime
         
def Clean(event, keep_type):
        
    logging.info(event+'_clean')
    logging.info('kept: '+keep_type)
    
    events = pd.read_csv(event+'.csv')
    # Clean up the date-times of events
    events['created_at'] = events.created_at.apply(lambda x: fixDate(x))
    
    if event=='follows':
        
        # Drop mistakes
        # One agent cannot follow another twice AT THE SAME TIME
        events.drop_duplicates(keep='first',inplace=True)
        # Drop if missing the follower
        events = events[events.alogin==events.alogin]
        # Drop if missing the followee
        events = events[events.tlogin==events.tlogin]
        # Drop if missing the time of link creation
        events = events[events.created_at==events.created_at]
        events.sort_values(by=['alogin','created_at'],inplace=True)

        # Possibly drop duplicates where one agent follows another at different times
        events.drop_duplicates(subset=['alogin','tlogin'], keep=keep_type, inplace=True)
        
        events.to_csv(event+'.csv', index=False, header=True)
                
    else: # event is stars
        
        # For stars, the repo creation time also needs to be cleaned up
        events['repo_created_at'] = events.repo_created_at.apply(lambda x: fixDate(x))
        
        # Drop mistakes again       
        # An agent cannot star a repo twice AT THE SAME TIME
        events.drop_duplicates(keep='first',inplace=True)
        events = events[events.alogin==events.alogin]
        events = events[events.repo==events.repo]
        events = events[events.created_at==events.created_at]
        events.sort_values(by=['alogin','created_at'],inplace=True)

        # Possibly drop duplicates where one agent stars a repo at different times
        events.drop_duplicates(subset=['alogin','repo'],keep='first',inplace=True)
        
        events.to_csv(event+'.csv',index=False,header=True)