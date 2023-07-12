# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Kaggle API connection
# MAGIC
# MAGIC This Notebook is setup to connect to the Kaggle API

# COMMAND ----------

# %fs
# ls FileStore/tables/Kaggle-datasets

# COMMAND ----------

# MAGIC %fs
# MAGIC ls FileStore/tables/

# COMMAND ----------

# Created a Kaggle-datasets table
# %fs
# mkdirs FileStore/shared_uploads/Kaggle-datasets/

# COMMAND ----------

# dbutils.fs.rm("dbfs:/FileStore/tables/Kaggle-datasets/kaggle.json")

# COMMAND ----------

# This cell reads in a manually uploaded kaggle.json file which contains a authentication key for the editter Kaggle account

df1 = spark.read.format("json").load("dbfs:/FileStore/tables/Kaggle-datasets/kaggle.json")
KAGGLE_USERNAME = df1.select(df1.username).collect()[0][0]
KAGGLE_KEY = df1.select(df1.key).collect()[0][0]

# display(df1)

# COMMAND ----------

# This cell is commented out but contains the information from the manually uploaded kaggle file

# print(KAGGLE_KEY)
# print(KAGGLE_USERNAME)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Authenticate Kaggle Account - Username and Key

# COMMAND ----------

def authenticate_kaggle(KAGGLE_USERNAME, KAGGLE_KEY):
    import os 

    os.environ['KAGGLE_USERNAME'] = KAGGLE_USERNAME
    os.environ['KAGGLE_KEY'] = KAGGLE_KEY

    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate

    print('The Authentication is successful')

    return api

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Below is a try and except block to place into each new notebook to create the connection to Kaggle

# COMMAND ----------

try : 
    kaggle_api = authenticate_kaggle(KAGGLE_USERNAME, KAGGLE_KEY)

except Exception as e :
    print(e)
    print('Install the module : pip install kaggle')

    import sys
    import subprocess 

    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'kaggle'])

    kaggle_api = authenticate_kaggle(KAGGLE_USERNAME, KAGGLE_KEY)

# COMMAND ----------

def standardize_csv_name(file_name):
    import re
    return re.sub(r"[^a-zA-Z0-9.]","_",file_name) # r"\s+""

# COMMAND ----------

def change_file_names_in_path(path):
    import os
    # iterating the dbfs and renaming the files
    for filename in os.listdir('/dbfs' + path):
        old_name = path + filename 
        new_name = path + standardize_csv_name(filename)
        dbutils.fs.mv(old_name, new_name)

# COMMAND ----------


