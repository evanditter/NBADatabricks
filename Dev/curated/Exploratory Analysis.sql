-- Databricks notebook source
CREATE SCHEMA analytics

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESCRIBE EXTENDED common_player_info

-- COMMAND ----------

DESCRIBE EXTENDED draft_history

-- COMMAND ----------

DESCRIBE EXTENDED player_stats

-- COMMAND ----------

DESCRIBE EXTENDED player_box_score_stats

-- COMMAND ----------

-- combined player one big table

select ps.player_name, c.birthdate, c.country, c.draft_round,d.draft_round as d_draft_round, d.round_pick, d.pick_number, c.draft_year, d.draft_year as d_draft_year, d.last_affiliation, d.last_affiliation_type, c.height_inches, c.weight_LBs, c.jersey_number, c.position, c.top_75_player, c.year_started, c.year_ended,
   d.player_id as d_player_id, c.player_id, d.team_name, d.team_abbreviation, d.team_id

from player_stats ps
full outer join draft_history d on lower(d.full_name) = lower(ps.player_name)
full outer join common_player_info c on lower(c.player_id) = lower(d.player_id)

where ps.player_name like 'Paul George'
limit 100

-- COMMAND ----------

select *
from draft_history
where draft_year = 2010

-- COMMAND ----------

select * from player_stats
where player_name like 'Paul George'

-- COMMAND ----------

select *
from common_player_info
where full_name like '%George%'

-- COMMAND ----------

select count(*) from common_player_info 

-- COMMAND ----------

select ps.player_name, c.full_name
from common_player_info c
LEFT JOIN player_stats ps on ps.player_name = c.full_name
WHERE ps.player_name is null

-- COMMAND ----------

-- DBTITLE 1,found example of improper join on name do to accent
select * from player_stats
where player_name like '%Bojan%'

-- COMMAND ----------

select (lower('Bojan Bogdanović')) collate SQL_Latin1_General_CP1253_CI_AI

-- COMMAND ----------

select ps.player_id, c.player_id, ps.full_name, c.full_name , c.year_ended, c.year_started
from common_player_info c
LEFT JOIN draft_history ps on ps.player_id = c.player_id
WHERE ps.player_id is null
-- 1228 players exist in common_player_info that are not in draft_history

-- COMMAND ----------

select ps.player_id, c.player_id, ps.full_name, c.full_name 
from draft_history c
LEFT JOIN common_player_info ps on ps.player_id = c.player_id
WHERE ps.player_id is null
-- 5005 players exist in draft_history that don't match to common player info

-- COMMAND ----------

select *
from draft_combine_stats
where full_name like '%Naz%'

-- COMMAND ----------

select ps.player_id, c.player_id, ps.full_name, c.full_name 
from draft_history c
LEFT JOIN common_player_info ps on ps.player_id = c.player_id
where c.player_id = 1629675
-- undrafted player

-- COMMAND ----------

select *
from common_player_info
where player_id = 1629675
