Movie-Recommender
=================

Recommend new released movies to Netflix users (using IMDB movie features, and netflix user ratings)


**Hadoop Jobs Structure**

**TRAINING TOOL**

1. Parsing Data : convert data into sequential files for later jobs
  * Parse Imdb Data
  * Parse Netflix Data
  
2. Initialize Imdb clusters and Netflix cluster: Used Random probability p=0.2 for selecting Vector (imdb or nflix vector) as center and all selected vectors are aggregated to create the initial centers of K clusters.
  * InitKCluster.
  * InitNetflixCluster.
  
3. Iteration of K mean algorithm:
  * Imdb: used Euclidean distance as a measure between the movies.
  * Neftlix K mean: used cosine distance
  
  While iterating, if clusters are not assigned any Data point then those centers are randomly reinitialized.
  
4. Last Iteration for Assignment of Movies (imdb or Netflix movies ) to respective clusters in a
file.
  * FinalStepKImdb
  * FinalStepKNetflix

**TEST TOOL**-responsible for

1. Mapping between users and imdb movies
  * Mapping imdb cluster with nflix cluster
  * Map Between Netflix clusters and users:
  
2. Mapping between Test movies and users
  * Final mapping for imdb cluster with user: output from 1.
  * Map testing movies with imdb cluster
  * Final Recommendation: use output from above 2 jobs and map the test movie with Users

**Program Parameters**

1. CLUSTER_MAP_THRESHOLD: each Netflix cluster is mapped with Imdb cluster if it‘s distance is less than minimum_distance*CLUSTER_MAP_THRESHOLD; (so one Imdb cluster is mapped with more than one Netflix cluster)

2. Map_Threshold: each test movie is mapped with more than one imdb cluster if it’s distance is less than minimum_distance*MAP_THRESHOLD. (the value of above parameter is >1 , chosen values are 1.05 for both)

3. K: number of Imdb clusters(chosen value is 100);

4. Netflix_K: number of Netflix clusters for K mean (chosen value 200). With Netflix dataset, no of clusters are reducing during the iteration (due to no assignment of movies), that’s why more number of clusters are initialized to counter the problem. (*cluster are reinitialized if the cluster is not assigned with any data point)

5. USER_FILTER: used for filtering user based on rating. if the user rating is greater than USER_FILTER then that user likes the movie. 
