# PeerEffectsGitHub
Estimate how much starring behaviors of individuals are affected by starring behaviors of their followees on GitHub.

# Required Libraries
The following Python libraries must be installed in order to use my programs.
1.) Pandas
2.) Implicit (Benfred/Implicit on GitHub)
3.) Pymongo
4.) Numpy, Os, Sys, Time, Gzip, Json, Logging, Urllib2

# Useage Instructions
There are separate directories to 1) Download data from the GitHub 
Archive, 2) Format/Clean the data 3) Perform WRMF-based collaborative filtering using the stars data 4) Match nearest-neighbors 5) Estimate the peer influence effect on the matched sample.

# 1) Download data from the GitHub Archive

Use the program "collectArchiveData.py" in the directory "DownloadGH_ArchiveData." The file will download data from the GitHub Archive and dump it into a mongo database. The files are in json and there is one file for each hour. You must specify the host ip address, username and password for the database, using the parameters "MONGO_HOST", "MONGO_USER" and "MONGO_PASSWORD," respectively. You can also specify the earliest and latest datetimes (to the nearest hour) for which you want to get the data. By default they are set to February 12, 2011 at 00:00:00 and October 31, 2013 at 23:00:00. 

This program will create a mongo database with a separate collection for each month of data. Note that the storage size can get pretty big.

# 2) Format/Clean the data

After getting the GH Archive data, we need extract and clean the data that we will be using, which is Stars and Follows. Use the program "starsMongoExtract.py" to take only the stars data out of the database, and use the program "followsMongoExtract.py" to take out the follows data. These programs will create one .csv file for each months' worth of data. Then we can clean and combine the stars and follows files by using the program "combineCleanData.py." This program will clean the data (e.g. drop duplicates) and combine the monthly .csv files into one file, then delete the monthly .csv files. You must specify the range of months for which you have data.

After creating the data, use the programs "ids.py" and "idsMerge.py" to create id numbers for each user and each repo, and to replace usernames and repo names with ids. The program "joinsExitsUsers.py" can be used to build a .csv file with the earliest and latest date that each agent shows up in the data, which we can use as a proxy for join and exit dates. In both the id and join/exit programs, you must specify the directory in which the star and following files are located.

I have included the finished files for stars and follows, which are called "stars_first.csv" and "follows_first.csv." I have also included the join dates and exit dates for agents, which are called "join_dates_users.csv" and "exit_dates_users.csv."

# 3) Perform WRMF 
