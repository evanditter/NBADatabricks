-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Player Datasets Cleansed

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC USE CATALOG NBA

-- COMMAND ----------

SHOW TABLES in NBA.raw

-- COMMAND ----------

select *
from raw.basketball_common_player_info_raw
limit 1000

-- COMMAND ----------

DESCRIBE EXTENDED raw.basketball_common_player_info_raw

-- COMMAND ----------

-- DROP TABLE common_player_info

-- COMMAND ----------

CREATE OR REPLACE TABLE NBA.default.common_player_info
(
  player_id int,
  first_name string,
  last_name string,
  full_name string,
  last_comma_first string,
  first_initial_last string,
  player_slug string,
  birthdate date,
  school string,
  country string,
  last_affiliation string,
  height string,
  height_inches double,
  weight_LBs double, 
  season_experience double,
  jersey_number int,
  position string,
  roster_status string,
  games_played_current_season_flag BOOLEAN, 
  team_id int, 
  team_name string,
  team_abbreviation string,
  team_code string,
  team_city string,
  player_code string,
  year_started int,
  year_ended int,
  dleague_flag BOOLEAN,
  nba_flag BOOLEAN,
  draft_year int,
  draft_round int,
  pick_number int,
  top_75_player BOOLEAN
)

-- COMMAND ----------

INSERT OVERWRITE NBA.default.common_player_info
SELECT 
  person_id player_id,
  first_name ,
  last_name ,
  display_first_last full_name,
  display_last_comma_first last_comma_first,
  display_fi_last first_initial_last ,
  player_slug,
  birthdate ,
  school ,
  country ,
  last_affiliation ,
  height,
  SUBSTRING(height, 1,1) * 12 + SUBSTRING(height, 3,2) as height_inches,
  cast(weight as double) weight_LBs, 
  cast(season_exp as double) season_experience,
  cast(jersey as int) jersey_number,
  position ,
  rosterstatus roster_status,
  case when games_played_current_season_flag = 'N' then 0 else 1 end as games_played_current_season_flag, 
  team_id, 
  team_name ,
  team_abbreviation,
  team_code ,
  team_city ,
  playercode player_code ,
  cast(from_year as int) year_started,
  cast(to_year as int) year_ended ,
  case when dleague_flag = 'N' then 0 else 1 end as dleague_flag,
  case when nba_flag = 'N' then 0 else 1 end as nba_flag,
  cast(draft_year as int) draft_year,
  cast(draft_round as int) draft_round,
  cast(draft_number as int) pick_number,
  case when greatest_75_flag = 'N' then 0 else 1 end as top_75_player
FROM NBA.raw.basketball_common_player_info_raw

-- COMMAND ----------

select *
from NBA.default.common_player_info
limit 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Player Stats Dataset Cleansed (Silver)

-- COMMAND ----------

select *
from NBA.raw.nba_player_stats_raw
where season = 2022
and player like '%aron Gordon%'

-- COMMAND ----------

--DROP TABLE player_stats

-- COMMAND ----------

CREATE OR REPLACE TABLE NBA.default.player_stats (
  player_name string,
  season int,
  position string,
  age double,
  team_abbreviation string,
  games_played double,
  games_started double,
  minutes_played double,
  field_goals double,
  field_goals_attempted double,
  field_goal_pct double,
  three_pointers double,
  three_pointers_attempted double,
  three_point_pct double,
  two_pointers double,
  two_pointers_attempted double,
  two_point_pct double,
  effective_field_goal_pct double,
  free_throws double,
  free_throws_attempted double,
  free_throw_pct double,
  points_per_game double,
  offensive_rebounds_per_game double,
  defensive_rebounds_per_game double,
  total_rebounds_per_game double,
  assists_per_game double,
  steals_per_game double,
  blocks_per_game double,
  turnovers_per_game double,
  personal_fouls_per_game double,
  points double,
  offensive_rebounds double,
  defensive_rebounds double,
  total_rebounds double,
  assists double,
  steals double,
  blocks double,
  turnovers double,
  personal_fouls double
)

-- COMMAND ----------

-- INSERT OVERWRITE player_stats
SELECT 
  Player as player_name,
  season,
  pos as position,
  age,
  tm as team_abbreviation ,
  g games_played ,
  gs games_started ,
  mp as minutes_played ,
  fg as field_goals ,
  fga as field_goals_attempted ,
  round(cast(`FG%` as double) * 100, 3) as field_goal_pct ,
  cast(`3P` as double) as three_pointers,
  cast(`3PA` as double) as three_pointers_attempted,
  round(cast(`3P%` as double) * 100, 3) as three_point_pct ,
  cast(`2P` as double) as two_pointers ,
  cast(`2PA` as double) as two_pointers_attempted ,
  round(cast(`2P%` as double) * 100, 3) as two_point_pct ,
  round(cast(`eFG%` as double) * 100, 3) effective_field_goal_pct ,
  cast(FT as double) as free_throws ,
  cast(FTA as double) as free_throws_attempted ,
  round(cast(`FT%` as double) * 100, 3) as free_throw_pct ,
  ROUND(PTS / G, 2) as points_per_game ,
  ROUND(ORB / G, 2) as offensive_rebounds_per_game ,
  ROUND(DRB / G, 2) as defensive_rebounds_per_game ,
  ROUND(TRB / G, 2) as total_rebounds_per_game ,
  ROUND(AST / G, 2) as assists_per_game ,
  ROUND(STL / G, 2) as steals_per_game ,
  ROUND(BLK / G, 2) blocks_per_game ,
  ROUND(CAST(`TOV` as double) / G, 2) as turnovers_per_game ,
  ROUND(PF / G, 2) as personal_fouls_per_game,
  PTS as points,
  ORB as offensive_rebounds ,
  DRB defensive_rebounds ,
  TRB as total_rebounds ,
  AST assists,
  STL as steals ,
  BLK as blocks ,
  CAST(`TOV` as double) as turnovers ,
  PF as personal_fouls 

FROM NBA.raw.nba_player_stats_raw

where player like '%aron Gordon%'

-- COMMAND ----------

select *
from NBA.default.player_stats 
limit 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Player Box Score Stats Dataset (Silver)

-- COMMAND ----------

-- usefull for gettting all of a players games across his career.
select *
from NBA.raw.nba_player_box_score_stats_raw
where plus_minus is not null 
limit 1000

-- COMMAND ----------

-- DROP TABLE player_box_score_stats

-- COMMAND ----------

CREATE OR REPLACE  TABLE NBA.default.player_box_score_stats (
  player_name string,
  season int,
  game_id int, 
  team_abbreviation string,
  game_date_string string, 
  game_date date, 
  matchup string, 
  win_loss string,
  minutes_played double,
  field_goals double,
  field_goals_attempted double,
  field_goal_pct double,
  three_pointers double,
  three_pointers_attempted double,
  three_point_pct double,
  free_throws double,
  free_throws_attempted double,
  free_throw_pct double,
  points double,
  offensive_rebounds double,
  defensive_rebounds double,
  total_rebounds double,
  assists double,
  steals double,
  blocks double,
  turnovers double,
  personal_fouls double,
  plus_minus double

)

-- COMMAND ----------

-- TRUNCATE TABLE player_box_score_stats

-- COMMAND ----------

INSERT OVERWRITE NBA.default.player_box_score_stats

SELECT 
  player_name ,
  season ,
  game_id , 
  team as team_abbreviation ,
  game_date as game_date_string,
  CAST(CASE WHEN SUBSTRING(game_date,1,3) LIKE 'JAN' then CONCAT( SUBSTRING(game_date,9,5), '-01-',  SUBSTRING(game_date,5,2) )
  WHEN SUBSTRING(game_date,1,3) LIKE 'FEB' then CONCAT( SUBSTRING(game_date,9,5), '-02-',  SUBSTRING(game_date,5,2) )
  WHEN SUBSTRING(game_date,1,3) LIKE 'MAR' then CONCAT( SUBSTRING(game_date,9,5), '-03-',  SUBSTRING(game_date,5,2) )
  WHEN SUBSTRING(game_date,1,3) LIKE 'APR' then CONCAT( SUBSTRING(game_date,9,5), '-04-',  SUBSTRING(game_date,5,2) )
  WHEN SUBSTRING(game_date,1,3) LIKE 'MAY' then CONCAT( SUBSTRING(game_date,9,5), '-05-',  SUBSTRING(game_date,5,2) )
  WHEN SUBSTRING(game_date,1,3) LIKE 'JUN' then CONCAT( SUBSTRING(game_date,9,5), '-06-',  SUBSTRING(game_date,5,2) )
  WHEN SUBSTRING(game_date,1,3) LIKE 'JUL' then CONCAT( SUBSTRING(game_date,9,5), '-07-',  SUBSTRING(game_date,5,2) )
  WHEN SUBSTRING(game_date,1,3) LIKE 'AUG' then CONCAT( SUBSTRING(game_date,9,5), '-08-',  SUBSTRING(game_date,5,2) )
  WHEN SUBSTRING(game_date,1,3) LIKE 'SEP' then CONCAT( SUBSTRING(game_date,9,5), '-09-',  SUBSTRING(game_date,5,2) )
  WHEN SUBSTRING(game_date,1,3) LIKE 'OCT' then CONCAT( SUBSTRING(game_date,9,5), '-10-',  SUBSTRING(game_date,5,2) )
  WHEN SUBSTRING(game_date,1,3) LIKE 'NOV' then CONCAT( SUBSTRING(game_date,9,5), '-11-',  SUBSTRING(game_date,5,2) )
  WHEN SUBSTRING(game_date,1,3) LIKE 'DEC' then CONCAT( SUBSTRING(game_date,9,5), '-12-',  SUBSTRING(game_date,5,2) )
  END as  DATE) AS game_date,
  matchup , 
  WL as win_loss ,
  MIN as minutes_played ,
  FGM as field_goals ,
  FGA as field_goals_attempted ,
  round(cast(FG_PCT as double) * 100, 3 ) as three_point_pct ,
  FG3M as three_pointers ,
  FG3A as three_pointers_attempted ,
  round(cast(FG3_PCT as double) * 100, 3 ) as three_point_pct ,
  FTM as free_throws ,
  FTA as free_throws_attempted ,
  round(cast(FT_PCT as double) * 100, 3) as free_throw_pct ,
  PTS as points ,
  OREB as offensive_rebounds ,
  DREB as defensive_rebounds ,
  REB as total_rebounds ,
  AST as assists ,
  STL as steals ,
  BLK as blocks ,
  TOV as turnovers ,
  PF as personal_fouls ,
  plus_minus 

FROM NBA.raw.nba_player_box_score_stats_raw

-- COMMAND ----------

select *
from NBA.default.player_box_score_stats
where game_date >= '2021-01-01'
limit 1000
