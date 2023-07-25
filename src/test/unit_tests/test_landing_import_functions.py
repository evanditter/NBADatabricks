# Databricks notebook source
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

