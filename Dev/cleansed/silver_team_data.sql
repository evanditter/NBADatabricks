-- Databricks notebook source
show tables in raw

-- COMMAND ----------

describe extended raw.basketball_team_raw

-- COMMAND ----------

select *
from raw.basketball_team_raw

-- COMMAND ----------

CREATE OR REPLACE TABLE basketball_teams (
  team_id int,
  team_name string,
  team_abbreviation string,
  team_nickname string,
  team_city string,
  team_state string,
  year_founded int
)

-- COMMAND ----------

INSERT OVERWRITE basketball_teams

select 
  id as team_id,
  full_name as team_name,
  abbreviation as team_abbreviation,
  nickname as team_nickname,
  city as team_city,
  state as team_state,
  cast(year_founded as int) as year_founded

FROM raw.basketball_team_raw

-- COMMAND ----------

select *
from basketball_teams

-- COMMAND ----------

describe extended raw.basketball_team_history_raw

-- COMMAND ----------

select *
from raw.basketball_team_history_raw

-- COMMAND ----------

CREATE OR REPLACE TABLE basketball_teams_history
(
  team_id int,
  team_city string,
  team_nickname string,
  year_founded int,
  year_active_until int

)

-- COMMAND ----------

INSERT OVERWRITE basketball_teams_history

SELECT 
  team_id ,
  city as team_city,
  nickname as team_nickname,
  year_founded,
  year_active_till as year_active_until

FROM raw.basketball_team_history_raw

-- COMMAND ----------

describe extended raw.basketball_team_details_raw

-- COMMAND ----------

select *
from raw.nba_team_win_raw

-- COMMAND ----------

describe extended raw.nba_team_win_raw

-- COMMAND ----------

CREATE OR REPLACE TABLE nba_team_win (
  season int,
  year string,
  team_nickname string,
  record string,
  winning_pct double
)

-- COMMAND ----------

INSERT OVERWRITE nba_team_win

SELECT 
  cast(SUBSTRING(year,1,4) as int) as season,
  year,
  team as team_nickname,
  record,
  round(`win%` * 100,3) as winning_pct

FROM raw.nba_team_win_raw

-- COMMAND ----------

DESCRIBE EXTENDED common_player_info

-- COMMAND ----------


