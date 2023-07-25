# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Running All of the unit tests in the regression test notebook

# COMMAND ----------

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
    # Does the table exist?
    # def test_tableExists(self):
    #     assert tableExists(tableName1, dbName2) is False
    #     assert tableExists(tableName2, dbName2) is True 
    #     assert tableExists(tableName3, dbName2) is True 
    #     assert tableExists(tableName3, dbName3) is False 

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

# test_results = unittest.main(argv=[''], verbosity=2, exit=False)
# assert test_results.result.wasSuccessful(), 'Test Failed; see logs above'

# COMMAND ----------

import unittest
from pyspark.sql.functions import col, sum, lower
from pyspark.sql.types import StructType, StringType, DoubleType
import datetime

today = datetime.date.today()

year_num = today.year

player_stats_df = spark.read.table("player_stats")
draft_history_df = spark.read.table("draft_history")
nba_team_win_df = spark.read.table("nba_team_win")

class TestSilverTables(unittest.TestCase):
    def test_player_stats(self):
        assert player_stats_df.select(col("points")).where( player_stats_df.points < 0 ).isEmpty()
        assert player_stats_df.select(col("total_rebounds")).where( player_stats_df.total_rebounds < 0 ).isEmpty()
        assert player_stats_df.select(col("assists")).where( player_stats_df.assists < 0 ).isEmpty()
        assert player_stats_df.select(col("turnovers")).where( player_stats_df.turnovers < 0 ).isEmpty()
        assert player_stats_df.select(col("blocks")).where( player_stats_df.blocks < 0 ).isEmpty()
        assert player_stats_df.select(col("steals")).where( player_stats_df.steals < 0 ).isEmpty()

    def test_draft_history(self):
        assert draft_history_df.select(col("draft_year")).where( (draft_history_df.draft_year < 1946) | (draft_history_df.draft_year > year_num + 1) ).isEmpty()
        assert draft_history_df.select(col("draft_round")).where( draft_history_df.draft_round < 0 ).isEmpty()
        assert draft_history_df.select(col("round_pick")).where( (draft_history_df.round_pick < 0 ) | (draft_history_df.round_pick > 30) ).isEmpty()
        assert draft_history_df.select(col("pick_number")).where( (draft_history_df.pick_number < 0)  | (draft_history_df.pick_number > 300)).isEmpty()
        assert draft_history_df.select(col("player_id")).where( draft_history_df.player_id < 0 ).isEmpty()


    def test_nba_team_win_df(self):
        assert nba_team_win_df.select(col("season")).where( (nba_team_win_df.season < 1946) | (nba_team_win_df.season > year_num + 1) ).isEmpty()
        assert nba_team_win_df.select(col("winning_pct")).where( (nba_team_win_df.winning_pct < 0) | (nba_team_win_df.winning_pct > 100) ).isEmpty()
        
# test_results = unittest.main(argv=[''], verbosity=2, exit=False)
# assert test_results.result.wasSuccessful(), 'Test Failed; see logs above'

# COMMAND ----------

# MAGIC %run /Repos/evan.ditter@inspire11.com/NBADatabricks/src/1_landing/raw_ConnectToKaggleAPI

# COMMAND ----------

import pyspark
import unittest
import re
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

csv1   = "diamonds.csv"
csv2   = "player_stats/2020-[01]-[01].csv"
csv3   = "draft_history-{test$#}.csv"


df1 = spark.read.format("json").load("dbfs:/FileStore/tables/Kaggle-datasets/kaggle.json")
KAGGLE_USERNAME = df1.select(df1.username).collect()[0][0]
KAGGLE_KEY = df1.select(df1.key).collect()[0][0]
# print( standardize_csv_name(csv1)  )
# print ( re.search(r"[^a-zA-Z0-9.]", standardize_csv_name(csv1) ) )
# print( bool(re.search(r"[^a-zA-Z0-9.]", standardize_csv_name(csv1) )) == False )
# print( standardize_csv_name(csv2)  )
# print( bool(re.search(r"[^a-zA-Z0-9.]", standardize_csv_name(csv2) )) == True )
# print( standardize_csv_name(csv3)  )
# print( bool(re.search(r"[^a-zA-Z0-9.]", standardize_csv_name(csv3) )) == True )

# df = spark.createDataFrame(data, schema)
# assert standardize_csv_name(csv2) is False
# bool(re.search('ba[rzd]', 'foobarrrr'))
class TestImportFunctions(unittest.TestCase):
    # Does the column exist?
    def test_standardize_csv_name(self):
        assert bool(re.search(r"[^a-zA-Z0-9.]", standardize_csv_name(csv1) )) is False
        assert bool(re.search(r"[^a-zA-Z0-9.]", standardize_csv_name(csv2) )) is True
        assert bool(re.search(r"[^a-zA-Z0-9.]", standardize_csv_name(csv3) )) is True

    def test_authenticate_kaggle(self):
        test_api = authenticate_kaggle(KAGGLE_USERNAME, KAGGLE_KEY)
        assert authenticate_kaggle(KAGGLE_USERNAME, KAGGLE_KEY) is not None
        #assert test_api.dataset_list(search="covid") is not None

test_results = unittest.main(argv=[''], verbosity=2, exit=False)
assert test_results.result.wasSuccessful(), 'Test Failed; see logs above'

