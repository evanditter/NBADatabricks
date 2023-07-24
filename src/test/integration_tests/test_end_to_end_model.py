# Databricks notebook source
# MAGIC %md 
# MAGIC  
# MAGIC  Notebook to test the whole process - calls other notebooks
# MAGIC

# COMMAND ----------

# MAGIC %run /Repos/evan.ditter@inspire11.com/NBADatabricks/src/landing/raw_ConnectToKaggleAPI

# COMMAND ----------

# MAGIC %run /Repos/evan.ditter@inspire11.com/NBADatabricks/src/landing/raw_NBA_player_and_team_data

# COMMAND ----------

# MAGIC %run /Repos/evan.ditter@inspire11.com/NBADatabricks/src/landing/raw_NBA_draft_games_teams_officials

# COMMAND ----------

# MAGIC %run /Repos/evan.ditter@inspire11.com/NBADatabricks/src/cleansed/silver_draft_data

# COMMAND ----------

# MAGIC %run /Repos/evan.ditter@inspire11.com/NBADatabricks/src/cleansed/silver_player_data

# COMMAND ----------

# MAGIC %run /Repos/evan.ditter@inspire11.com/NBADatabricks/src/cleansed/silver_team_data

# COMMAND ----------

# MAGIC %run /Repos/evan.ditter@inspire11.com/NBADatabricks/src/curated/production_model
