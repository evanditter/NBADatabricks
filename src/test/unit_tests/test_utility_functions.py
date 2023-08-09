# Databricks notebook source
# MAGIC %run ../../3_curated/utility_functions

# COMMAND ----------

import pyspark
import unittest
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

tableName1   = "diamonds"
tableName2   = "player_stats"
tableName3   = "draft_history"
dbName1      = "raw"
dbName2      = "default"
dbName3      = "analytics"
columnName1  = "player_name"
columnName2  = "player_age"
columnValue1 = "Bruce Brown"
columnValue2 = "Lebron James"

# Create fake data for the unit tests to run against.
# In general, it is a best practice to not run unit tests
# against functions that work with data in production.
schema = StructType([ \
  StructField("player_name",     StringType(), True), \
  StructField("team",     StringType(),  True), \
  StructField("team_Abbreviation",   StringType(),  True), \
  StructField("points_per_game",   FloatType(),   True), \
  StructField("age",   IntegerType(), True), \
  StructField("rebounds_per_game",       FloatType(),   True), \
  StructField("assists_per_game",       FloatType(),   True), \
  StructField("steals_per_game",       FloatType(),   True), \
  StructField("points",       IntegerType(),   True), \
  StructField("assists",       IntegerType(),   True), \
])

data = [ ("Bruce Brown", "Nuggets", "DEN", 21.5, 41, 8.5, 9.1, 1.98, 691, 301 ), \
         ("Bruce Brown", "Nuggets", "DEN", 21.5, 41, 8.5, 9.1, 1.98, 1022, 203 ), \
         ("Lebron James", "Lakers", "LA", 5.8, 31, 6.5, 3.89, 0.87, 597, 506), \
         ("Lebron James", "Lakers", "LA", 5.8, 31, 6.5, 3.89, 0.87, 2012, 508), \
         ("Lebron James", "Lakers", "LA", 5.8, 31, 6.5, 3.89, 0.87, 1334, 612) ]

df = spark.createDataFrame(data, schema)

class TestUtilityFunctions(unittest.TestCase):

    # Does the column exist?
    def test_columnExists(self):
        assert columnExists(df, columnName1) is True
        assert columnExists(df, columnName2) is False

    # Is there at least one row for the value in the specified column?
    def test_numRowsInColumnForValue(self):
        assert numRowsInColumnForValue(df, columnName1, columnValue1) > 0

    def test_players_total_stat(self):
        assert players_total_stat(df, columnValue1,'points') == 1713
        assert players_total_stat(df, columnValue2,'points') == 597 + 2012 + 1334
        assert players_total_stat(df, columnValue2,'assists') == 506 + 508 + 612

    def test_players_total_points(self):
        assert players_total_points(df, columnValue1) == 1713
        assert players_total_points(df, columnValue2) == 597 + 2012 + 1334

test_results = unittest.main(argv=[''], verbosity=2, exit=False)
assert test_results.result.wasSuccessful(), 'Test Failed; see logs above'
