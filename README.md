# NBA Analysis hosted on Databricks

## What was the purpose of this project?

I wanted to be able to do exploratory analysis and apply simple machine learning and AI algorithms to NBA datasets. 
Based on player statistics, I wanted to be able to predict what would make a good player based on their draft position as well as predict if a player would be a hall of famer based on the stats.

## Tech Stack
I used Databricks hosted on AWS and using Github Actions to promote notebooks from dev to prod environments. 
In Databricks, I created a medallion architecture - bronze, silver, and gold.
  - In the bronze layer there is raw data which is a collection of datasets from kaggle gathered using API calls to kaggle.
  - In the silver layer I cleansed the raw data by defining the schema desired, performing conversions / standardizations on the statistics, and casting values as needed.
  - In the gold layer, I performed aggregations of the data that would be useful in the exploratory analysis as well as for dashboarding if a PowerBI or Tableau report were to be attached to the Databricks instance.
