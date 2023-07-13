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

# MAGIC %fs
# MAGIC ls /FileStore/tables/Kaggle-datasets/basketball/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_team_history_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/team_history.csv'
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

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_team_details_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/team_details.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_team_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/team.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_player_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/player.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE raw.basketball_play_by_play_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_play_by_play_raw
# MAGIC (game_id string,eventnum string, eventmsgtype string,eventmsgactiontype string,period string,wctimestring string,pctimestring string,homedescription string,neutraldescription string,visitordescription string,score string,scoremargin string,person1type string,player1_id string,player1_name string,player1_team_id string,player1_team_city string,player1_team_nickname string,player1_team_abbreviation string,person2type string,player2_id string,player2_name string,player2_team_id string, player2_team_city string,player2_team_nickname string,player2_team_abbreviation string,person3type string,player3_id string,player3_name string,player3_team_id string,player3_team_city string,player3_team_nickname string,player3_team_abbreviation string,video_available_flag string)
# MAGIC
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/play_by_play0.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE Table raw.basketball_play_by_play2_raw
# MAGIC (game_id string,eventnum string, eventmsgtype string,eventmsgactiontype string,period string,wctimestring string,pctimestring string,homedescription string,neutraldescription string,visitordescription string,score string,scoremargin string,person1type string,player1_id string,player1_name string,player1_team_id string,player1_team_city string,player1_team_nickname string,player1_team_abbreviation string,person2type string,player2_id string,player2_name string,player2_team_id string, player2_team_city string,player2_team_nickname string,player2_team_abbreviation string,person3type string,player3_id string,player3_name string,player3_team_id string,player3_team_city string,player3_team_nickname string,player3_team_abbreviation string,video_available_flag string)
# MAGIC
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/play_by_play1-1.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_play_by_play_combined_raw
# MAGIC (game_id string,eventnum string, eventmsgtype string,eventmsgactiontype string,period string,wctimestring string,pctimestring string,homedescription string,neutraldescription string,visitordescription string,score string,scoremargin string,person1type string,player1_id string,player1_name string,player1_team_id string,player1_team_city string,player1_team_nickname string,player1_team_abbreviation string,person2type string,player2_id string,player2_name string,player2_team_id string, player2_team_city string,player2_team_nickname string,player2_team_abbreviation string,person3type string,player3_id string,player3_name string,player3_team_id string,player3_team_city string,player3_team_nickname string,player3_team_abbreviation string,video_available_flag string)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into raw.basketball_play_by_play_combined_raw
# MAGIC select * from raw.basketball_play_by_play2_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into raw.basketball_play_by_play_combined_raw
# MAGIC select * from raw.basketball_play_by_play_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_officials_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/officials.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_other_stats_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/other_stats.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_line_score_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/line_score.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_inactive_players_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/inactive_players.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_game_summary_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/game_summary.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_game_info_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/game_info.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_game_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/game.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_draft_history_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/draft_history.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_draft_combine_stats_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/draft_combine_stats.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw.basketball_common_player_info_raw
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true"
# MAGIC )
# MAGIC LOCATION '/FileStore/tables/Kaggle-datasets/basketball/common_player_info.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in raw

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from raw.basketball_player_raw
# MAGIC limit 100

# COMMAND ----------


