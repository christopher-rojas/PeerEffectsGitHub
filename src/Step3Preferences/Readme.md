We now can estimate preferences for each agent by using their past stars. Before doing WRMF, you must first use the file 'adoptionMatrix.py' to create the sparse, binary adoption matrix, which is a (# users) x (# items) matrix with a 1 in the (u,i) position of agent u has starred repo i. This sparse matrix will be used for one of our matching algorithms, and will also serve as the input to estimate WRMF-based preferences. 

Estimate preferences with WRMF using the file 'learnFactorVectors.py'. You must specify the data directory, the minimum number of items each agent must have to infer preferences (10 in paper), the hyper-parameters for WRMF, and the time period (month). The output of this program is a .csv file in which each row is an agent, and the columns contain their latent user factors, as well as their user id number.

In our paper I estimate preferences for each period (month) from January, 2013 to October, 2013, using earlier stars data. The preferences in a given period are computed with all the data prior to the beginning of that period.

To set the hyper-parameters, we use the data prior to the first period and split it into training/validation sets. We compute the discounted cumulative gain for different combinations of the hyper-parameters. This can be done with the program 'parameter_eval_dcg.py'.

Finally, estimate preferences based on programming languages by using the program 'languageVectors.py'.
