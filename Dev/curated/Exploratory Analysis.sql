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
full outer join draft_history d on d.full_name = ps.player_name
full outer join common_player_info c on c.player_id = d.player_id

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


