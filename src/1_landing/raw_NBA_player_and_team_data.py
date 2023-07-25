# Databricks notebook source
# MAGIC %run /Repos/evan.ditter@inspire11.com/NBADatabricks/src/landing/raw_ConnectToKaggleAPI

# COMMAND ----------

# kaggle datasets download -d loganlauton/nba-players-and-team-data

# COMMAND ----------

kaggle_name = 'nba-players-and-team-data'
kaggle_dataset = 'loganlauton/nba-players-and-team-data'
file_path = '/FileStore/tables/Kaggle-datasets/'

# COMMAND ----------

# DBTITLE 1,List contents of kaggle dataset directory
# MAGIC %fs
# MAGIC ls FileStore/tables/Kaggle-datasets/

# COMMAND ----------

# displays the default download directory - current Workspace directory
kaggle_api.get_default_download_dir()


# COMMAND ----------

# DBTITLE 1,Unzip files from download and move to FileStore/tables/Kaggle-datasets directory
import zipfile 

kaggle_api.dataset_download_files(kaggle_dataset)

with zipfile.ZipFile('/Workspace/Repos/evan.ditter@inspire11.com/NBADatabricks/Dev/landing/nba-players-and-team-data.zip') as zp:
    zp.extractall('/{}/'.format(kaggle_name))
    dbutils.fs.mv('file:/{}/'.format(kaggle_name),'dbfs:/FileStore/tables/Kaggle-datasets' + '/{}'.format(kaggle_name), recurse=True)
    dbutils.fs.rm('file:/Workspace/Repos/evan.ditter@inspire11.com/NBADatabricks/Dev/landing/nba-players-and-team-data.zip')

print('Datasets Downloaded and exist in FileStore/tables/Kaggle-datasets' + '/{}'.format(kaggle_name))


# COMMAND ----------

# DBTITLE 1,Show files in the Kaggle-datasets/nba-players-and-team-data directory
# MAGIC %fs
# MAGIC ls FileStore/tables/Kaggle-datasets/nba-players-and-team-data/

# COMMAND ----------

# DBTITLE 1,Call change_file_names_in_path to standardize file names
change_file_names_in_path('/FileStore/tables/Kaggle-datasets' + '/{}/'.format(kaggle_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas

# COMMAND ----------

# DBTITLE 1,Create the raw NBA payroll table
# MAGIC %sql
# MAGIC CREATE TABLE raw.NBA_Payroll_raw 
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/nba-players-and-team-data/NBA_Payroll_1990_2023_.csv'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Show the NBA payroll raw data
# MAGIC %sql
# MAGIC select *
# MAGIC from raw.NBA_Payroll_raw 
# MAGIC limit 1000

# COMMAND ----------

# DBTITLE 1,Create NBA Player Box Score Stats Raw
# MAGIC %sql
# MAGIC CREATE TABLE raw.NBA_Player_Box_Score_Stats_raw 
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/nba-players-and-team-data/NBA_Player_Box_Score_Stats_1950___2022_.csv'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Display Raw Player Box Score Stats
# MAGIC %sql
# MAGIC select *
# MAGIC from raw.NBA_Player_Box_Score_Stats_raw 
# MAGIC limit 1000

# COMMAND ----------

# DBTITLE 1,Create NBA Player Stats raw table
# MAGIC %sql
# MAGIC CREATE TABLE raw.NBA_Player_Stats_raw 
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/nba-players-and-team-data/NBA_Player_Stats_1950___2022_.csv'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Display NBA Player Stats Raw
# MAGIC %sql
# MAGIC select *
# MAGIC from raw.NBA_Player_Stats_raw 
# MAGIC limit 1000

# COMMAND ----------

# DBTITLE 1,Create NBA Players Salaries Raw
# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE raw.NBA_Player_Salaries_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/nba-players-and-team-data/NBA_Salaries_1990_2023_.csv'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Display NBA Player Salaries Raw
# MAGIC %sql
# MAGIC select *
# MAGIC from raw.NBA_Player_Salaries_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN raw

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED raw.nba_payroll_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED raw.nba_player_box_score_stats_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED raw.nba_player_salaries_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED raw.nba_player_stats_raw
