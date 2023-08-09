# Databricks notebook source
# %sql
# select *
# from player_stats
# where player_name like '%LeBron James%'

# COMMAND ----------

# return total points for a given player
from pyspark.sql.functions import col, sum, lower
from pyspark.sql.types import StructType, StringType, DoubleType

def players_total_points(dataframe: StructType, player: StringType) -> DoubleType:
    
    players_df = dataframe.select(col("points")).where( lower(dataframe.player_name) == player.lower() )
    #return players_df
    return players_df.select(sum(players_df.points)).collect()[0][0]

# COMMAND ----------

# test_df = spark.read.table("player_stats")

# total = players_total_points(test_df,'LeBron James')
# print(total)

# COMMAND ----------

# return total points for a given player
from pyspark.sql.functions import col, sum, lower
from pyspark.sql.types import StructType, StringType, DoubleType

def players_total_stat(dataframe: StructType, player: StringType, stat: StringType) -> DoubleType:
    players_df = dataframe.select(col(stat)).where( lower(dataframe.player_name) == player.lower() )
    return players_df.agg({stat: 'sum'}).collect()[0][0]

# COMMAND ----------

# test_df2 = spark.read.table("player_stats")

# total = players_total_stat(test_df,'LeBron James','points')
# print(total)

# COMMAND ----------

# Does the specified column exist in the given DataFrame?
def columnExists(dataFrame, columnName):
  if columnName in dataFrame.columns:
    return True
  else:
    return False

# COMMAND ----------

# How many rows are there for the specified value in the specified column
# in the given DataFrame?
def numRowsInColumnForValue(dataFrame, columnName, columnValue):
  df = dataFrame.filter(col(columnName) == columnValue)
  return df.count()
