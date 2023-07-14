-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Draft Combine Dataset Cleansed (silver)

-- COMMAND ----------

SHOW TABLES in raw

-- COMMAND ----------

select *
from raw.basketball_draft_combine_stats_Raw

-- COMMAND ----------

DESCRIBE EXTENDED raw.basketball_draft_combine_stats_Raw

-- COMMAND ----------

CREATE TABLE draft_combine_stats
(
  season int,
  player_id int,
  first_name string,
  last_name string,
  full_name string,
  position string,
  height string,
  height_inches double,
  height_with_shoes string,
  height_inches_with_shoes double,
  weight_LBs double, 
  wingspan string,
  wingspan_inches double,
  standing_reach string,
  standing_reach_inches double,
  body_fat_pct double,
  hand_length_inches double,
  hand_width_inches double,
  standing_vertical_leap_inches double,
  max_vertical_leap_inches double,
  lane_agility_time_seconds double,
  modified_lane_agility_time_seconds double,
  three_quarter_sprint_seconds double,
  bench_press_reps double,
  spot_fifteen_corner_left_pct double,
  spot_fifteen_break_left_pct double,
  spot_fifteen_top_key_pct double,
  spot_fifteen_break_right_pct double,
  spot_fifteen_corner_right_pct double,

  spot_college_corner_left_pct double,
  spot_college_break_left_pct double,
  spot_college_top_key_pct double,
  spot_college_break_right_pct double,
  spot_college_corner_right_pct double,

  spot_nba_corner_left_pct double,
  spot_nba_break_left_pct double,
  spot_nba_top_key_pct double,
  spot_nba_break_right_pct double,
  spot_nba_corner_right_pct double,

  off_drib_fifteen_break_left_pct double,
  off_drib_fifteen_top_key_pct double,
  off_drib_fifteen_break_right_pct double,

  off_drib_college_break_left_pct double,
  off_drib_college_top_key_pct double,
  off_drib_college_break_right_pct double,

  on_move_fifteen double,
  on_move_college double
)

-- COMMAND ----------

INSERT INTO draft_combine_stats
SELECT 
  season ,
  player_id ,
  first_name ,
  last_name ,
  player_name as full_name ,
  position string,
  height_wo_shoes_ft_in as height ,
  height_wo_shoes as height_inches,
  height_w_shoes_ft_in as  height_with_shoes ,
  height_w_shoes as height_inches_with_shoes ,
  weight as weight_LBs, 
  wingspan_ft_in as wingspan ,
  wingspan as wingspan_inches ,

  standing_reach_ft_in as standing_reach ,
  standing_reach as standing_reach_inches ,
  body_fat_pct ,
  hand_length as hand_length_inches ,
  hand_width hand_width_inches,
  standing_vertical_leap as standing_vertical_leap_inches ,
  max_vertical_leap as max_vertical_leap_inches ,
  lane_agility_time as lane_agility_time_seconds,
  modified_lane_agility_time as modified_lane_agility_time_seconds,
  three_quarter_sprint as three_quarter_sprint_seconds ,
  bench_press as bench_press_reps,

  SUBSTRING_INDEX(spot_fifteen_corner_left, '-', 1) / SUBSTRING_INDEX(spot_fifteen_corner_left, '-', -1)  as spot_fifteen_corner_left_pct,
  SUBSTRING_INDEX(spot_fifteen_break_left,'-',1) / SUBSTRING_INDEX(spot_fifteen_break_left,'-',-1)  as spot_fifteen_break_left,
  SUBSTRING_INDEX(spot_fifteen_top_key,'-',1) / SUBSTRING_INDEX(spot_fifteen_top_key,'-',-1)  as spot_fifteen_top_key_pct,
  SUBSTRING_INDEX(spot_fifteen_break_right,'-',1) / SUBSTRING_INDEX(spot_fifteen_break_right,'-',-1)  as spot_fifteen_break_right_pct,
  SUBSTRING_INDEX(spot_fifteen_corner_right,'-',1) / SUBSTRING_INDEX(spot_fifteen_corner_right,'-',-1)  as spot_fifteen_corner_right_pct,

  SUBSTRING_INDEX(spot_college_corner_left,'-',1) / SUBSTRING_INDEX(spot_college_corner_left,'-',-1)  as spot_college_corner_left_pct,
  SUBSTRING_INDEX(spot_college_break_left,'-',1) / SUBSTRING_INDEX(spot_college_break_left,'-',-1)  as spot_college_break_left,
  SUBSTRING_INDEX(spot_college_top_key,'-',1) / SUBSTRING_INDEX(spot_college_top_key,'-',-1)  as spot_college_top_key_pct,
  SUBSTRING_INDEX(spot_college_break_right,'-',1) / SUBSTRING_INDEX(spot_college_break_right,'-',-1)  as spot_college_break_right_pct,
  SUBSTRING_INDEX(spot_college_corner_right,'-',1) / SUBSTRING_INDEX(spot_college_corner_right,'-',-1)  as spot_college_corner_right_pct,

  SUBSTRING_INDEX(spot_nba_corner_left,'-',1) / SUBSTRING_INDEX(spot_nba_corner_left,'-',-1)  as spot_nba_corner_left_pct,
  SUBSTRING_INDEX(spot_nba_break_left,'-',1) / SUBSTRING_INDEX(spot_nba_break_left,'-',-1)  as spot_nba_break_left,
  SUBSTRING_INDEX(spot_nba_top_key,'-',1) / SUBSTRING_INDEX(spot_nba_top_key,'-',-1)  as spot_nba_top_key_pct,
  SUBSTRING_INDEX(spot_nba_break_right,'-',1) / SUBSTRING_INDEX(spot_nba_break_right,'-',-1)  as spot_nba_break_right_pct,
  SUBSTRING_INDEX(spot_nba_corner_right,'-',1) / SUBSTRING_INDEX(spot_nba_corner_right,'-',-1)  as spot_nba_corner_right_pct,

  SUBSTRING_INDEX(off_drib_fifteen_break_left,'-',1) / SUBSTRING_INDEX(off_drib_fifteen_break_left,'-',-1)  as off_drib_fifteen_break_left_pct,
  SUBSTRING_INDEX(off_drib_fifteen_top_key,'-',1) / SUBSTRING_INDEX(off_drib_fifteen_top_key,'-',-1)  as off_drib_fifteen_break_left_pct,
  SUBSTRING_INDEX(off_drib_fifteen_break_right,'-',1) / SUBSTRING_INDEX(off_drib_fifteen_break_right,'-',-1)  as off_drib_fifteen_break_left_pct,

  SUBSTRING_INDEX(off_drib_college_break_left,'-',1) / SUBSTRING_INDEX(off_drib_college_break_left,'-',-1)  as off_drib_college_break_left_pct,
  SUBSTRING_INDEX(off_drib_college_top_key,'-',1) / SUBSTRING_INDEX(off_drib_college_top_key,'-',-1)  as off_drib_college_break_left_pct,
  SUBSTRING_INDEX(off_drib_college_break_right,'-',1) / SUBSTRING_INDEX(off_drib_college_break_right,'-',-1)  as off_drib_college_break_left_pct,


  SUBSTRING_INDEX(on_move_fifteen, '-', 1) / SUBSTRING_INDEX(on_move_fifteen, '-', -1)  as on_move_fifteen_pct,
  SUBSTRING_INDEX(on_move_college, '-', 1) / SUBSTRING_INDEX(on_move_college, '-', -1)  as on_move_college_pct

FROM raw.basketball_draft_combine_stats_Raw

-- COMMAND ----------

select *
from draft_combine_stats

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Draft History Dataset Cleansed (Silver)

-- COMMAND ----------

select *
from raw.basketball_draft_history_raw
limit 100

-- COMMAND ----------

CREATE TABLE draft_history (
  player_id int,
  full_name string,
  draft_year int,
  draft_round int,
  round_pick int,
  pick_number int,
  draft_type string,
  team_id int,
  team_city string,
  team_name string,
  team_abbreviation string,
  last_affiliation string,
  last_affiliation_type string,
  player_profile_flag BOOLEAN
)

-- COMMAND ----------

INSERT INTO draft_history
SELECT 
  person_id as player_id,
  player_name as full_name,
  season as draft_year,
  round_number as draft_round,
  round_pick,
  overall_pick as pick_number,
  draft_type,
  team_id,
  team_city,
  team_name,
  team_abbreviation,
  organization as last_affiliation,
  organization_type as last_affiliation_type,
  player_profile_flag

FROM raw.basketball_draft_history_raw

-- COMMAND ----------

select *
from draft_history

-- COMMAND ----------

select * 
from draft_combine_stats

-- COMMAND ----------


