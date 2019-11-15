#!/usr/bin/env python2.7
"""
Collect GitHub Archive data and insert into mongodb.
Make sure to run in a Python console, not iPython, for logging.
"""
import CollectArchiveData_Modules
import os, sys, pymongo, time, gzip, json
import logging
import pandas as pd

#Set up logging
logging.basicConfig(filename='GitHubArchiveData.log',level=logging.INFO,format='%(asctime)s %(message)s')

####### PARAMETERS TO ENTER
starting_datetime = "2011-2-12-00"
last_datetime = "2013-10-31 23:00:00"
# Connect to MongoDB
# Enter you own host-name, db_name, user-name and password.
MONGO_HOST = ""
MONGO_DB = ""
MONGO_USER = ""
MONGO_PASSWORD = ""
#######

connection=pymongo.MongoClient(MONGO_HOST)
# Exit if unable to connect
if connection is None:
    logging.info('No DB Connection')
    sys.exit(0)
logging.info('Connected succesfully')   
# Set up DB
db = connection[MONGO_DB] 

# Load the current datetime, and hours remaining
time_periods = list(pd.date_range(start=starting_datetime,end=last_datetime,freq='60min'))
remaining = len(time_periods)
current_datetime = time_periods.pop(0)

# While the number of hours remaining is greater than 0,
# download the hour's data, insert into mongodb, delete 
# the hour's data, and then increment the hour forward by 1.
while remaining >= 1:
    # Create a collection for each month of GitHub Archive data.
    month, year = str(current_datetime.month), str(current_datetime.year)
    coll = db[MONGO_DB+'-'+month+'-'+year]
    
    # Datetime needs to be a string for logging and for accessing
    # GitHub archive.
    logging.info('Datetime: ' + CollectArchiveData_Modules.pretty_string(current_datetime))
    # Don't need to hit up GitHub archive too frequently
    time.sleep(.1)
    # Use exception handling in case of unforeseen problems,
    # but log everything.
    try:
        # First download the data for current date
        logging.info('Attempting to download')
        CollectArchiveData_Modules.download_file(current_datetime)
        try:
            # Next open the file
            logging.info('Download Succesful. Attempting to open')
            filename = CollectArchiveData_Modules.pretty_string(current_datetime) + '.json.gz'            
            github_data = []
            for line in gzip.open(filename, 'r'):
                github_data.append(json.loads(line))
            
            try:
                # Next add the data to mongodb
                logging.info('Opened. Attempting to add to mongodb')
                # Need at least one document to insert
                if len(github_data)>0:
                    result = coll.insert_many(github_data)
                logging.info("Added data to mongodb")
                # Delete file
                logging.info("Deleting file")
                os.remove(filename)
                logging.info("File deleted")               
                
                # Increment datetime up one hour.
                remaining = len(time_periods)                
                current_datetime = time_periods.pop(0)

            except Exception, e1:
                # Adding data to mongodb failed
            
                logging.info("Error adding data to mongodb")
                logging.info(str(e1))
                # Delete file
                logging.info("Deleting file")
                os.remove(filename)
                logging.info("File deleted")
                      
        except Exception, e2:
            # Opening the data failed
            logging.info('Error opening data')
            logging.info(str(e2))
            # Delete file
            os.remove(filename) 
            logging.info('File deleted')
            
    except Exception,e3:
        # Downloading the data failed.
        logging.info('Error downloading data: ' + str(e3))
        logging.exception("message")
