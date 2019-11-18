# Useage Instructions
There are separate directories corresponding to the different steps in our empirical analysis:

1) Download data from the GitHub Archive

2) Format/Clean the data 

3) Estimate Preferences 

4) Match nearest-neighbors 

5) Estimate the peer influence effect on the matched sample.

6) Perform Simulation

7) Estimate Treatment Effects

To build everything from scratch, you must run each of these in order. However, I have included the formatted data from the end of step 2 on my personal website at https://www.carojas.com/research, so you do not need to rebuild the data from scratch and could start from step 3.

Click on each of the directories for a more detailed readme for that step.

# 1) Download data from the GitHub Archive

Use the program "collectArchiveData.py" in the directory "DownloadGH_ArchiveData." The file will download data from the GitHub Archive and dump it into a mongo database. The files are in json and there is one file for each hour. You must specify the host ip address, username and password for the database, using the parameters "MONGO_HOST", "MONGO_USER" and "MONGO_PASSWORD," respectively. You can also specify the earliest and latest datetimes (to the nearest hour) for which you want to get the data. By default they are set to February 12, 2011 at 00:00:00 and October 31, 2013 at 23:00:00. 

This program will create a mongo database with a separate collection for each month of data. Note that the storage size can get pretty big.

# 2) Format/Clean the data

After getting the GH Archive data, we need extract and clean the data that we will be using, which is Stars and Follows. Use the program "starsMongoExtract.py" to take only the stars data out of the database, and use the program "followsMongoExtract.py" to take out the follows data. These programs will create one .csv file for each months' worth of data. Then we can clean and combine the stars and follows files by using the program "combineCleanData.py." This program will clean the data (e.g. drop duplicates) and combine the monthly .csv files into one file, then delete the monthly .csv files. You must specify the range of months for which you have data.

After creating the data, use the programs "ids.py" and "idsMerge.py" to create id numbers for each user and each repo, and to replace usernames and repo names with ids. The program "joinsExitsUsers.py" can be used to build a .csv file with the earliest and latest date that each agent shows up in the data, which we can use as a proxy for join and exit dates. In both the id and join/exit programs, you must specify the directory in which the star and following files are located.

# 3) Perform WRMF 

After building the file "stars_first.csv" you can do WRMF to get the preference vectors for each agent. You must specify the data directory, the minimum number of items each agent must have to include in WRMF, the hyper-parameters for WRMF, and the time period (month). The output of this program is a .csv file in which each row is an agent, and the columns contain their latent user factors, as well as their user id number. 

In our paper I estimate preferences for each period (month) from January, 2013 to October, 2013, using earlier stars data. The preferences in a given period are computed with all the data prior to the beginning of that period.

To set the hyper-parameters, we use the data prior to the first period and split it into training/test sets. We compute the expected ranking percentile for different combinations of the hyper-parameters. This can be done with the program hyperparameterEvalExpectedRankingPercentile.py.

# 4) Nearest-Neighbor Matching

The program "nearestNeighborWRMF.py" will estimate the set of nearest-neighbors for each agent. You must specify the timer period, number of nearest neighbors for each agent, number of months of inactivity which defines exit, and then the directories where the stars/follows data (from 2), and WRMF factor vectors (from 3) are located. The output of this program 
is a file in which each agent (for whom we can learn preferences) is listed along with their nearest neighbors, and the distance to the neighbor. We use variance-weighted Euclidean distance.

The program "matchTreatedObservations.py" will compute, for each agent and each repo which was recently starred by agent(s) they follow, the nearest non-treated (and non-connected in the social network) agent. You must specify the time period, directories where stars/follows data and WRMF factor vectors are located. You must also set the maximum duration of peer influence, which determines how long in the past someone an agent follows could have starred a repo, for that agent to be considered as treated. The output is a .csv file in which each row corresponds to an agent-repo, and the output includes whether or not the treated agent adopts the item that period (listed as a datetime) and whether or not the matched, non-treated agent adopts the item that period (also listed as a datetime).

# 5) Estimate Treatment Effect

Program to estimate the peer influence effect by comparing the ratio of items starred by treated agents, to the ratio of items starred by matched, non-treated agents. Treatment can be defined based on the number of a users' followees who recently adopted, and we analyze 1, 2 and 3 adopting leaders. You must specify the periods to include and the directory where the matched-sample is located. You must also specify the type of non-parametric bootstrap method to use for computing 95% confidence intervals: regular or pigeonhole. The output is a .csv file with the mean treatment effect, upper/lower confidence limits, and number of observations, for each level of treatment and each period.

I include the estimated treatment effect output, for periods 1 through 10, using the regular bootstrap.
