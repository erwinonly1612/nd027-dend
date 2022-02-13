# Movie Recommmendation with Spark and AWS
## Introduction

The project uses datasets (ml-latest-small) including 5-star rating and free-text tagging activity from [MovieLens](https://grouplens.org/datasets/movielens/latest/), a movie recommendation service. It contains 100836 ratings and 3683 tag applications across 9742 movies. These data were created by 610 users between March 29, 1996 and September 24, 2018. This dataset was generated on September 26, 2018.

The complete dataset contains 27753444 ratings and 1108997 tag applications across 58098 movies. Data were created by 283228 users between January 09, 1995 and September 26, 2018. This dataset was generated on September 26, 2018.

The Project is to build an ETL pipeline that extracts data from S3, processes them using Spark, stages them in Redshift, and transforms data into a set of dimensional tables.

With the dimensional data model, we can extract various insights from the dataset, such as:
- Which movies are highly rated across various genres / years?
- Which movies received the most awards/nominations across different genres/ years?
- What are the ratings for movies that received awards/ nomination?
- Is there any kind of correlation between ratings and awards / nomination?

## Project Datasets 
* Test data: 's3://udacity-input/ml-latest-small'  
* Complete data: 's3://udacity-input/ml-latest'  


#### Movies Data File Structure (movies.csv)  

Movie information is contained in the file `movies.csv`. Each line of this file after the header row represents one movie, and has the following format:

* movieId, title, genres

Movie titles are entered manually or imported from <https://www.themoviedb.org/>, and include the year of release in parentheses. Errors and inconsistencies may exist in these titles.

Genres are a pipe-separated list, and are selected from the following:

* Action, Adventure, Animation, Children's, Comedy, Crime, Documentary, Drama, Fantasy, Film-Noir, Horror, Musical, Mystery, Romance, Sci-Fi, Thriller, War, Western, (no genres listed)


#### Ratings Data File Structure (ratings.csv)  

All ratings are contained in the file `ratings.csv`. Each line of this file after the header row represents one rating of one movie by one user, and has the following format:

* userId, movieId, rating, timestamp

The lines within this file are ordered first by userId, then, within user, by movieId. Ratings are made on a 5-star scale, with half-star increments (0.5 stars - 5.0 stars). Timestamps represent seconds since midnight Coordinated Universal Time (UTC) of January 1, 1970.


#### Tags Data File Structure (tags.csv)  

All tags are contained in the file `tags.csv`. Each line of this file after the header row represents one tag applied to one movie by one user, and has the following format:

* userId, movieId, tag, timestamp

The lines within this file are ordered first by userId, then, within user, by movieId. Tags are user-generated metadata about movies. Each tag is typically a single word or short phrase. The meaning, value, and purpose of a particular tag is determined by each user. Timestamps represent seconds since midnight Coordinated Universal Time (UTC) of January 1, 1970.


#### Awards Data File (Awards.txt)

All awards are contained in the file `Awards.txt`. The file is copied from Wikipedia <https://en.wikipedia.org/wiki/List_of_Academy_Award-winning_films>

* Film, year, awards, nominations

#### Awards Data File (Award_corrected.txt)

Correction for the Awards.txt  


## Schema for Movie Recommendation Analysis
Using the above datasets, I need to create fact and dimension tables optimized for queries on movie recommendation analysis. This includes the following tables.


### Fact and dimension tables
* **awards** - (film, year, nominations, awards)
* **movies** - (movieId, title, year)   
* **genres** - (genreId, movieId, genre)  
* **ratings** - (userId, movieId, rating, rate_time)  
* **time** - timestamps in ratings broken down into specific units (date_key, day, week, month, year, weekday)


## Project Template
* **create_redshift_cluster.ipynb** create redshift cluster
* **etl.ipynb** read data from s3, staging to redshift, and create dimension tables
* **dwh.cfg** contains AWS credentials       
* **etl_v_2.ipynb** organize the orders and add more documentation

## Project Steps
### Create user on AWS
* Get the access key and secret key, and put it into dwh.cfg


### Create redshift cluster
* Run create_redshift_cluster.ipynb


### Build ETL Pipeline
* Step 1: Implement the logic in etl.ipynb to extract data from s3, and load data back to S3 in parquet format 
* Step 2: Data Wrangling with DataFrames and Spark SQL (clean and explore data)
* Step 3: Extract data from S3, transform data into fact and dimension tables, and load them to redshift

#### reference: 
[More Analysis with plots](https://www.kaggle.com/cesarcf1977/movielens-data-analysis-beginner-s-first?select=tag.csv)  
Explore the data with some basic plots  




