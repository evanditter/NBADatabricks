# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG NBA

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from player_stats
# MAGIC where player_name like '%aron Gor%'
# MAGIC limit 100

# COMMAND ----------

from pyspark.sql.functions import col

player_stats_silver_df = spark.read.table("player_stats")

important_stats = player_stats_silver_df.select(col('player_name'), (col('field_goal_pct') ), (col('three_point_pct') ), (col('free_throw_pct') ), col('points_per_game'), col('total_rebounds_per_game'), col('assists_per_game'), col('steals_per_game'), col('blocks_per_game'), col('turnovers_per_game'), col('personal_fouls_per_game'), col('minutes_played') )

# display(important_stats.head(5))

# COMMAND ----------

display(important_stats)

# COMMAND ----------

important_stats.write.mode("overwrite").format("delta").saveAsTable('NBA.analytics.important_stats')

# COMMAND ----------

print('test if on main it is pushed correctly')

# COMMAND ----------

#100
# points - 30
# assists - 15
# rebounds - 15
# field goal pct - 10
# 3 point pct - 10
# free throw pct - 10
# turnovers - (-10)
# personal fouls - (-5)
# minutes played - minimum 1200

#important_stats.select()


# COMMAND ----------

print('one more test')

# COMMAND ----------

print('still part of test')
