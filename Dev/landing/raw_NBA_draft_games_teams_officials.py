# Databricks notebook source
# kaggle datasets download -d wyattowalsh/basketball

# COMMAND ----------

# MAGIC %run /Repos/evan.ditter@inspire11.com/NBADatabricks/Dev/landing/raw_ConnectToKaggleAPI

# COMMAND ----------

kaggle_name = 'basketball'
kaggle_dataset = 'wyattowalsh/basketball'
file_path = '/FileStore/tables/Kaggle-datasets/'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls FileStore/tables/Kaggle-datasets/

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

# MAGIC %fs
# MAGIC ls FileStore/tables/Kaggle-datasets/basketball/

# COMMAND ----------

change_file_names_in_path('/FileStore/tables/Kaggle-datasets' + '/{}/'.format(kaggle_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_team_info_common_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/team_info_common.csv'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_team_info_common_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/team_info_common.csv'
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


