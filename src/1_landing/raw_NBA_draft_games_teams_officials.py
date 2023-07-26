# Databricks notebook source
# kaggle datasets download -d wyattowalsh/basketball

# COMMAND ----------

# MAGIC %run /Repos/evan.ditter@inspire11.com/NBADatabricks/src/landing/raw_ConnectToKaggleAPI

# COMMAND ----------

kaggle_name = 'basketball'
kaggle_dataset = 'wyattowalsh/basketball'
file_path = '/FileStore/tables/Kaggle-datasets/'

# COMMAND ----------

# %fs
# ls FileStore/tables/Kaggle-datasets/

# COMMAND ----------

kaggle_api.dataset_download_files(kaggle_dataset)

# COMMAND ----------

import zipfile 

kaggle_api.dataset_download_files(kaggle_dataset)

with zipfile.ZipFile('/Workspace/Repos/evan.ditter@inspire11.com/NBADatabricks/Dev/landing/basketball.zip') as zp:
    zp.extractall('/{}/'.format(kaggle_name))
    dbutils.fs.mv('file:/{}/'.format(kaggle_name),'dbfs:/FileStore/tables/Kaggle-datasets' + '/{}'.format(kaggle_name), recurse=True)
    dbutils.fs.rm('file:/Workspace/Repos/evan.ditter@inspire11.com/NBADatabricks/Dev/landing/basketball.zip')

print('Datasets Downloaded and exist in FileStore/tables/Kaggle-datasets' + '/{}'.format(kaggle_name))


# COMMAND ----------

# %fs
# ls FileStore/tables/Kaggle-datasets/basketball/

# COMMAND ----------

change_file_names_in_path('/FileStore/tables/Kaggle-datasets' + '/{}/'.format(kaggle_name))

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/Kaggle-datasets/basketball/')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG nba;
# MAGIC USE SCHEMA raw;
# MAGIC
# MAGIC CREATE TABLE basketball_team_history_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/team_history.csv'
# MAGIC
# MAGIC
# MAGIC -- CREATE TABLE student USING CSV LOCATION '/mnt/csv_files';
# MAGIC

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/team_history.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_team_history_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/team_info_common.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_team_info_common_raw')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from NBA.raw.basketball_team_info_common_raw

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/team_details.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_team_details_raw')


# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/Kaggle-datasets/basketball/')

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/team.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_team_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/player.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_player_raw')


# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE NBA.raw.basketball_play_by_play_raw

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/play_by_play0.csv"
file_location2 = "/FileStore/tables/Kaggle-datasets/basketball/play_by_play1-1.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df2 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", "false") \
  .option("sep", delimiter) \
  .load(file_location2)

df.write.mode("append").format("delta").saveAsTable('NBA.raw.basketball_play_by_play_raw')
df2.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable('NBA.raw.basketball_play_by_play_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/officials.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_officials_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/other_stats.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_other_stats_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/line_score.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_line_score_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/inactive_players.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_inactive_players_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/game_summary.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_game_summary_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/game_info.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_game_info_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/game.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_game_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/draft_history.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_draft_history_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/draft_combine_stats.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_draft_combine_stats_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/common_player_info.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.basketball_common_player_info_raw')


# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Kaggle-datasets/basketball/nba_team_win.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

df.write.mode("overwrite").format("delta").saveAsTable('NBA.raw.nba_team_win_raw')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from NBA.raw.nba_team_win_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in NBA.raw
